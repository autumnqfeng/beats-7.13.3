package kafka_cluster

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/eapache/go-resiliency/breaker"

	"github.com/elastic/beats/v7/libbeat/common/fmtstr"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
)

var (
	once sync.Once
	pool *clientPool
)

type clientPool struct {
	pool *Lru
}

type kafkaClient struct {
	log      *logp.Logger
	observer outputs.Observer
	hosts    string
	topic    string
	key      *fmtstr.EventFormatString
	index    string
	codec    codec.Codec
	config   sarama.Config
	mux      sync.Mutex
	done     chan struct{}

	producer sarama.AsyncProducer

	wg sync.WaitGroup
}

func newClientPool() *clientPool {
	once.Do(func() {
		pool = &clientPool{
			pool: newLruCache(100),
		}
	})
	return pool
}

func newKafkaClient(
	log *logp.Logger,
	observer outputs.Observer,
	hosts string,
	index string,
	key *fmtstr.EventFormatString,
	topic string,
	writer codec.Codec,
	cfg *sarama.Config,
) *kafkaClient {

	return &kafkaClient{
		log:      log,
		observer: observer,
		hosts:    hosts,
		topic:    topic,
		key:      key,
		index:    strings.ToLower(index),
		codec:    writer,
		config:   *cfg,
	}
}

func (p *clientPool) contains(cluster string) bool {
	return p.pool.contains(cluster)
}

func (p *clientPool) getAllClient() map[string]*kafkaClient {
	var clients = make(map[string]*kafkaClient)
	nodes := p.pool.getAll()
	for _, node := range nodes {
		clients[node.Key] = node.Val
	}
	return clients
}

func (p *clientPool) getClient(cluster string) *kafkaClient {
	client, _ := p.pool.get(cluster)
	return client
}

func (p *clientPool) addClient(cluster string, client *kafkaClient) (*kafkaClient, error) {
	if err := p.pool.add(cluster, client, 60*60*10); err != nil {
		return nil, err
	}

	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("%v kafka connect failed with %v", cluster, err)
	}
	return client, nil
}

func (c *kafkaClient) Connect() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.log.Debugf("connect: %v", c.hosts)

	// try to connect
	producer, err := sarama.NewAsyncProducer(strings.Split(c.hosts, ","), &c.config)
	if err != nil {
		c.log.Errorf("Kafka connect fails with: %+v", err)
		return err
	}

	c.producer = producer

	c.wg.Add(2)
	go c.successWorker(producer.Successes())
	go c.errorWorker(producer.Errors())

	return nil
}

func (c *kafkaClient) Close() {
	c.log.Debug("closed kafka client")

	// producer was not created before the close() was called.
	if c.producer == nil {
		return
	}

	close(c.done)
	c.producer.AsyncClose()
	c.wg.Wait()
	c.producer = nil
}

func (c *kafkaClient) Publish(msg *message) {
	ch := c.producer.Input()

	msg.initProducerMessage()
	ch <- &msg.msg
}

func (c *kafkaClient) getEventMessage(data *publisher.Event) (*message, error) {
	event := &data.Content
	msg := &message{partition: -1, data: *data}

	if msg.topic == "" {
		msg.topic = c.topic
	}

	serializedEvent, err := c.codec.Encode(c.index, event)
	if err != nil {
		if c.log.IsDebug() {
			c.log.Debugf("failed event: %v", event)
		}
		return nil, err
	}

	buf := make([]byte, len(serializedEvent))
	copy(buf, serializedEvent)
	msg.value = buf

	// message timestamps have been added to kafka with version 0.10.0.0
	if c.config.Version.IsAtLeast(sarama.V0_10_0_0) {
		msg.ts = event.Timestamp
	}

	if c.key != nil {
		if key, err := c.key.RunBytes(event); err == nil {
			msg.key = key
		}
	}
	return msg, nil
}

func (c *kafkaClient) successWorker(ch <-chan *sarama.ProducerMessage) {
	defer c.wg.Done()
	defer c.log.Debug(fmt.Sprintf("Stop kafka ack worker, cluster: %v, topic: %v", c.hosts, c.topic))

	for libMsg := range ch {
		msg := libMsg.Metadata.(*message)
		msg.ref.done()
	}
}

func (c *kafkaClient) errorWorker(ch <-chan *sarama.ProducerError) {
	breakerOpen := false
	defer c.wg.Done()
	defer c.log.Debug(fmt.Sprintf("Stop kafka error handler, cluster: %v, topic: %v", c.hosts, c.topic))

	for errMsg := range ch {
		msg := errMsg.Msg.Metadata.(*message)
		msg.ref.fail(msg, errMsg.Err)

		if errMsg.Err == breaker.ErrBreakerOpen {
			if breakerOpen {
				// Immediately log the error that presumably caused this state,
				// since the error reporting on this batch will be delayed.
				if msg.ref.err != nil {
					c.log.Errorf("Kafka (topic=%v): %v", msg.topic, msg.ref.err)
				}
				select {
				case <-time.After(10 * time.Second):
					// Sarama's circuit breaker is hard-coded to reject all inputs
					// for 10sec.
				case <-msg.ref.client.done:
					// Allow early bailout if the output itself is closing.
				}
				breakerOpen = false
			} else {
				breakerOpen = true
			}
		}
	}
}
