package core

import (
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/filebeat/input_d"
	"go.uber.org/zap"

	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/filebeat"
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/logstash"
	"github.com/spf13/viper"
)

var Upgrade *upgrade

type upgrade struct {
	FileBeat *filebeat.FileBeat
	Logstash *logstash.Logstash
	ltf      *logstashToFilebeat
	ftl      *filebeatToLogstash
	fallback bool
}

func NewUpgrade() *upgrade {
	back := viper.GetBool("fallback")
	return &upgrade{
		fallback: back,
		FileBeat: filebeat.NewFileBeat(back),
		Logstash: logstash.NewLogstash(),
		ltf:      &logstashToFilebeat{},
		ftl:      &filebeatToLogstash{},
	}
}

func (u *upgrade) Execute() error {
	if u.fallback {
		err := u.filebeatToLogstash()
		if err != nil {
			zap.L().Error("upgrade.filebeatToLogstash failed.", zap.Error(err))
		}
		return err
	} else {
		err := u.logstashToFilebeat()
		if err != nil {
			zap.L().Error("upgrade.logstashToFilebeat failed.", zap.Error(err))
		}
		return err
	}
}

func (u *upgrade) logstashToFilebeat() error {
	u.Logstash.ReadConfig()
	return u.FileBeat.WriteStates(u.ltf.getStatesFromConfig(u.Logstash.Config))
}

func (u *upgrade) filebeatToLogstash() error {
	if inputConfigs, err := input_d.LoadInputConfigs(u.FileBeat.InputdPath); err != nil {
		return err
	} else if err := u.Logstash.WriteConfig(u.ftl.getLogstashConfig(inputConfigs, u.Logstash.SincedbHome)); err != nil {
		return err
	}

	if states, err := u.FileBeat.ReadStates(); err != nil {
		return err
	} else {
		return u.Logstash.WriteSinceDB(u.ftl.getSinceDbInfo(states))
	}
}
