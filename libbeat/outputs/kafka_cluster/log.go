package kafka_cluster

import (
	"github.com/Shopify/sarama"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type kafkaClusterLogger struct {
	log *logp.Logger
}

func (kcl kafkaClusterLogger) Print(v ...interface{}) {
	kcl.Log("kafka cluster message: %v", v...)
}

func (kcl kafkaClusterLogger) Printf(format string, v ...interface{}) {
	kcl.Log(format, v...)
}

func (kcl kafkaClusterLogger) Println(v ...interface{}) {
	kcl.Log("kafka cluster message: %v", v...)
}

func (kcl kafkaClusterLogger) Log(format string, v ...interface{}) {
	warn := false
	for _, val := range v {
		if err, ok := val.(sarama.KError); ok {
			if err != sarama.ErrNoError {
				warn = true
				break
			}
		}
	}
	if kcl.log == nil {
		kcl.log = logp.NewLogger(logSelector)
	}
	if warn {
		kcl.log.Warnf(format, v...)
	} else {
		kcl.log.Infof(format, v...)
	}
}
