package kafka_cluster

import (
	"github.com/Shopify/sarama"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/outil"
)

const logSelector = "kafka-cluster"

func init() {
	sarama.Logger = kafkaClusterLogger{log: logp.NewLogger(logSelector)}

	outputs.RegisterType("kafka-cluster", makeKafkaCluster)
}

func makeKafkaCluster(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	log := logp.NewLogger(logSelector)
	log.Debug("initialize kafka cluster output")

	config, err := readConfig(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	topic, err := buildTopicSelector(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	hosts, err := buildHostsSelector(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	cluster, err := buildClusterSelector(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	libCfg, err := newSaramaConfig(log, config)

	codeC, err := codec.CreateEncoder(beat, config.Codec)
	if err != nil {
		return outputs.Fail(err)
	}

	client, err := newKafkaClusterClient(observer, cluster, hosts, beat.IndexPrefix, config.Key, topic, codeC, libCfg)

	retry := 0
	if config.MaxRetries < 0 {
		retry = -1
	}
	return outputs.Success(config.BulkMaxSize, retry, client)
}

func buildTopicSelector(cfg *common.Config) (outil.Selector, error) {
	return outil.BuildSelectorFromConfig(cfg, outil.Settings{
		Key:              "topic",
		MultiKey:         "topics",
		EnableSingleOnly: true,
		FailEmpty:        true,
		Case:             outil.SelectorKeepCase,
	})
}

func buildHostsSelector(cfg *common.Config) (outil.Selector, error) {
	return outil.BuildSelectorFromConfig(cfg, outil.Settings{
		Key:              "hosts",
		EnableSingleOnly: true,
		FailEmpty:        true,
		Case:             outil.SelectorKeepCase,
	})
}

func buildClusterSelector(cfg *common.Config) (outil.Selector, error) {
	return outil.BuildSelectorFromConfig(cfg, outil.Settings{
		Key:              "cluster",
		EnableSingleOnly: true,
		FailEmpty:        true,
		Case:             outil.SelectorKeepCase,
	})
}
