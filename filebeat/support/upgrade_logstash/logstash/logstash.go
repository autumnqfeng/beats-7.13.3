package logstash

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/common"
	"github.com/joeshaw/multierror"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const config = `input {
	qbusupdate {}%s
}

output {%s
}`

const input = `
	file {
		type => "%s"
		cluster => "%s"
		path => "%s"
		msg_prefix => "%s"
		sincedb_path => "%s"
	}`

const output = `
	if [type] == "%s" {
		kafka {
			codec => plain {
				format => "%s"
			}
			bootstrap_servers => "%s"
			topic_id => "%s"
			linger_ms => 100
			retries => 10
			retry_backoff_ms => 200
			reconnect_backoff_ms => 500
			metadata_fetch_timeout_ms => 10000
			buffer_memory => 33554432
			batch_size => 16384
		}
	}`

type Logstash struct {
	ConfigPath  string          `yaml:"config_path"`
	SincedbHome string          `yaml:"sincedb_path"`
	Config      map[int]*Config `yaml:"-"`
}

func NewLogstash() *Logstash {
	return &Logstash{
		ConfigPath:  viper.GetString("logstash.config_path"),
		SincedbHome: viper.GetString("logstash.sincedb_path"),
		Config:      make(map[int]*Config),
	}
}

func (l *Logstash) WriteConfig(configs []Config) error {
	inputs := make([]string, 0)
	outputs := make([]string, 0)
	for i, cfg := range configs {
		_type := cfg.Cluster + strings.Replace(cfg.Path, "/", "_", -1)

		inputs = append(inputs, fmt.Sprintf(input, _type, cfg.Cluster, cfg.Path, cfg.Prefix, cfg.SinceDbPath))
		outputs = append(outputs, fmt.Sprintf(output, _type, cfg.Codec, cfg.OutputAddress, cfg.Topic))
		l.Config[i] = cfg.copy()
	}
	config := fmt.Sprintf(config, strings.Join(inputs, ""), strings.Join(outputs, ""))

	zap.L().Info("logstash", zap.String("config", config))
	return common.WriteFile(fmt.Sprintf("%v/logstash.conf", l.ConfigPath), []byte(config))
}

func (l *Logstash) WriteSinceDB(sinceDbs []SinceDB) error {
	var errs multierror.Errors
	for _, cfg := range l.Config {
		data := make([]string, 0)

		for _, sinceDb := range sinceDbs {
			if match, err := regexp.MatchString(cfg.Path, sinceDb.Path); err != nil {
				errs = append(errs, err)
			} else if match {
				data = append(data, fmt.Sprintf("%v 0 2049 %v\n", sinceDb.Inode, sinceDb.Offset))
			}
		}
		dataStr := strings.Join(data, "")
		if err := common.WriteFile(cfg.SinceDbPath, []byte(dataStr)); err != nil {
			errs = append(errs, err)
		}
		zap.L().Info("logstash", zap.String(cfg.SinceDbPath, dataStr))
	}
	return errs.Err()
}

func (l *Logstash) ReadConfig() {
	l.readConfig()
	l.readInodeOffset()

	bytes, _ := json.Marshal(l.Config)
	zap.L().Debug("read logstash message: ", zap.String("", string(bytes)))
}

func (l *Logstash) readConfig() {
	confLines, err := common.ReadFileToLines(fmt.Sprintf("%v/logstash.conf", l.ConfigPath))
	if err != nil {
		zap.L().Error("read logstash config failed", zap.Error(err))
		return
	}

	splitLine := 0
	for i, line := range confLines {
		if strings.Contains(line, "output {") {
			splitLine = i
		}
	}

	inputNum := 0
	outputNum := 0
	for i, line := range confLines {
		if i < splitLine {
			if strings.HasPrefix(line, "type => ") {
				l.Config[inputNum] = newConfig()
				inputNum++
			}
			if strings.HasPrefix(line, "cluster => ") {
				l.Config[inputNum-1].Cluster = strings.Replace(strings.Replace(line, "cluster => ", "", -1), "\"", "", -1)
			}
			if strings.HasPrefix(line, "path => ") {
				l.Config[inputNum-1].Path = strings.Replace(strings.Replace(line, "path => ", "", -1), "\"", "", -1)
			}
			if strings.HasPrefix(line, "sincedb_path => ") {
				l.Config[inputNum-1].SinceDbPath = strings.Replace(strings.Replace(line, "sincedb_path => ", "", -1), "\"", "", -1)
			}
		} else {
			if strings.HasPrefix(line, "if [type] ==") {
				outputNum++
			}
			if strings.HasPrefix(line, "topic_id => ") {
				l.Config[outputNum-1].Topic = strings.Replace(strings.Replace(line, "topic_id => ", "", -1), "\"", "", -1)
			}
		}
	}
}

func (l *Logstash) readInodeOffset() {
	for _, cfg := range l.Config {
		inodeOffsetMap := make(map[uint64]int64)
		readSinceDb(cfg.SinceDbPath, inodeOffsetMap)
		cfg.InodeOffsetMap = inodeOffsetMap
	}
}

func readSinceDb(sinceDbPath string, inodeOffsetMap map[uint64]int64) {
	iNodeOffsets, err := common.Command(fmt.Sprintf("cat %v", sinceDbPath))
	if err != nil {
		return
	}

	for _, iNodeOffset := range iNodeOffsets {
		split := strings.Split(iNodeOffset, " ")
		if len(split) < 4 {
			continue
		}

		iNode, _ := strconv.Atoi(split[0])
		offset, _ := strconv.Atoi(split[3])
		inodeOffsetMap[uint64(iNode)] = int64(offset)
	}
}
