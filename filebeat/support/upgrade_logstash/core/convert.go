package core

import (
	"fmt"

	"github.com/elastic/beats/v7/filebeat/input/file"
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/common"
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/filebeat/input_d"
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/filebeat/state"
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/logstash"
	"go.uber.org/zap"
)

type logstashToFilebeat struct{}

type filebeatToLogstash struct{}

func (ltf logstashToFilebeat) getStatesFromConfig(configs map[int]*logstash.Config) []file.State {
	states := make([]file.State, 0)
	for _, cfg := range configs {
		fileInfos := state.GetFileInfos([]string{cfg.Path})

		for path, fileInfo := range fileInfos {
			fileState, err := state.GetFileState(path, fileInfo)
			if err != nil {
				zap.L().Error(fmt.Sprintf("get file %v state field", path))
				continue
			}
			if _, ok := cfg.InodeOffsetMap[fileState.FileStateOS.Inode]; ok {
				fileState.Offset = cfg.InodeOffsetMap[fileState.FileStateOS.Inode]
			}
			states = append(states, *fileState)
		}
	}
	return states
}

func (ftl filebeatToLogstash) getLogstashConfig(configs []*input_d.Config, sinceDbHome string) []logstash.Config {
	result := make([]logstash.Config, 0)
	for _, config := range configs {
		for _, path := range config.Paths {
			sinceDbPath := sinceDbHome + "/.sincedb_" + config.Output.Cluster + "_" + common.Md5(path)

			result = append(result, logstash.Config{
				Cluster:       config.Output.Cluster,
				Path:          path,
				Codec:         "%{message}",
				Prefix:        config.Output.Prefix,
				Topic:         config.Output.Topic,
				OutputAddress: config.Output.Hosts,
				SinceDbPath:   sinceDbPath,
			})
		}
	}
	return result
}

func (ftl filebeatToLogstash) getSinceDbInfo(states []file.State) []logstash.SinceDB {
	result := make([]logstash.SinceDB, 0)
	for _, s := range states {
		result = append(result, logstash.SinceDB{
			Path:   s.Source,
			Inode:  s.FileStateOS.Inode,
			Offset: s.Offset,
		})
	}
	return result
}
