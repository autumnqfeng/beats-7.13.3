package kafka_cluster

import (
	"context"
	"fmt"
	"github.com/eapache/go-resiliency/breaker"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/elastic/beats/v7/libbeat/common/fmtstr"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/outil"
	"github.com/elastic/beats/v7/libbeat/publisher"
)

type client struct {
	log      *logp.Logger
	observer outputs.Observer
	cluster  outil.Selector
	hosts    outil.Selector
	topic    outil.Selector
	key      *fmtstr.EventFormatString
	index    string
	codec    codec.Codec
	config   *sarama.Config
	mux      sync.Mutex
	done     chan struct{}

	kafkaClientPool *clientPool
}

// This structure is used when the message is sent and kafka calls the callback function to perform subsequent ACK or retry
type msgRef struct {
	client *client
	count  int32
	total  int
	failed []publisher.Event
	batch  publisher.Batch

	err error
}

func newKafkaClusterClient(
	observer outputs.Observer,
	cluster outil.Selector,
	hosts outil.Selector,
	index string,
	key *fmtstr.EventFormatString,
	topic outil.Selector,
	writer codec.Codec,
	cfg *sarama.Config,
) (*client, error) {
	c := &client{
		log:      logp.NewLogger(logSelector),
		observer: observer,
		cluster:  cluster,
		hosts:    hosts,
		topic:    topic,
		key:      key,
		index:    strings.ToLower(index),
		codec:    writer,
		config:   cfg,
		done:     make(chan struct{}),
	}
	return c, nil
}

func (c *client) Connect() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.log.Debugf("connect: %v")

	c.kafkaClientPool = newClientPool()

	return nil
}

func (c *client) Close() error {
	clients := c.kafkaClientPool.getAllClient()
	for _, cli := range clients {
		cli.Close()
	}
	return nil
}

func (c *client) Publish(_ context.Context, batch publisher.Batch) error {
	events := batch.Events()
	c.observer.NewBatch(len(events))

	ref := &msgRef{
		client: c,
		count:  int32(len(events)),
		total:  len(events),
		failed: nil,
		batch:  batch,
	}

	for i := range events {
		d := &events[i]
		kafkaClient, err := c.getEventKafkaClient(d)
		if err != nil {
			c.dealEventError(err, ref)
			continue
		}
		msg, err := kafkaClient.getEventMessage(d)
		if err != nil {
			c.dealEventError(err, ref)
			continue
		}
		msg.ref = ref
		kafkaClient.Publish(msg)
	}
	return nil
}

func (c *client) dealEventError(err error, ref *msgRef) {
	c.log.Errorf("Dropping event: %+v", err)
	ref.done()
	c.observer.Dropped(1)
}

func (c *client) String() string {
	var cs = make([]string, 0)

	if c.kafkaClientPool == nil {
		return "kafka cluster is not init"
	}

	clients := c.kafkaClientPool.getAllClient()
	for cluster, cli := range clients {
		cs = append(cs, fmt.Sprintf("%v:%v", cluster, cli.hosts))
	}
	return "kafka cluster ( " + strings.Join(cs, ",") + " )"
}

func (c *client) getEventKafkaClient(data *publisher.Event) (*kafkaClient, error) {
	event := &data.Content

	topic, err := c.topic.Select(event)
	if err != nil {
		return nil, fmt.Errorf("get kafka topic failed with %v", err)
	}

	cluster, err := c.cluster.Select(event)
	if err != nil {
		return nil, fmt.Errorf("get kafka cluster failed with %v", err)
	}

	if c.kafkaClientPool.contains(cluster) {
		return c.kafkaClientPool.getClient(cluster), nil
	}

	hosts, err := c.hosts.Select(event)
	if err != nil {
		return nil, fmt.Errorf("get kafka hosts failed with %v", err)
	}

	clusterTopic := fmt.Sprintf("%s-%s", cluster, topic)
	kafkaClient, err := c.kafkaClientPool.addClient(clusterTopic, newKafkaClient(c.log, c.observer, hosts, c.index, c.key, topic, c.codec, c.config))
	if err != nil {
		return nil, fmt.Errorf("%v add kafka client failed with %v", clusterTopic, err)
	}

	return kafkaClient, nil
}

func (r *msgRef) done() {
	r.dec()
}

func (r *msgRef) fail(msg *message, err error) {
	switch err {
	case sarama.ErrInvalidMessage:
		r.client.log.Errorf("Kafka (topic=%v): dropping invalid message", msg.topic)
		r.client.observer.Dropped(1)

	case sarama.ErrMessageSizeTooLarge, sarama.ErrInvalidMessageSize:
		r.client.log.Errorf("Kafka (topic=%v): dropping too large message of size %v.",
			msg.topic,
			len(msg.key)+len(msg.value))

	case breaker.ErrBreakerOpen:
		// Add this message to the failed list, but don't overwrite r.err since
		// all the breaker error means is "there were a lot of other errors".
		r.failed = append(r.failed, msg.data)

	default:
		r.failed = append(r.failed, msg.data)
		if r.err == nil {
			// Don't overwrite an existing error. This way at tne end of the batch
			// we report the first error that we saw, rather than the last one.
			r.err = err
		}
	}
	r.dec()
}

func (r *msgRef) dec() {
	i := atomic.AddInt32(&r.count, -1)
	if i > 0 {
		return
	}

	r.client.log.Debug("finished kafka cluster batch")
	stats := r.client.observer

	err := r.err
	if err != nil {
		failed := len(r.failed)
		success := r.total - failed
		r.batch.RetryEvents(r.failed)

		stats.Failed(failed)
		if success > 0 {
			stats.Acked(success)
		}

		r.client.log.Debugf("Kafka cluster publish failed with: %+v", err)
	} else {
		r.batch.ACK()
		stats.Acked(r.total)
	}
}
