package filebeat

import (
	"fmt"
	"path/filepath"

	"github.com/elastic/beats/v7/filebeat/input/file"
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/common"
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/filebeat/store"
	"github.com/elastic/beats/v7/libbeat/common/transform/typeconv"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const logFileName = "log.json"
const fileStatePrefix = "filebeat::logs::"

type FileBeat struct {
	RegistryPath string `yaml:"registry_path"`
	InputdPath   string `yaml:"inputd_path"`
	disk         *store.DiskStore
}

func NewFileBeat(fallback bool) *FileBeat {
	zap.L().Debug("filebeat", zap.String("func NewFileBeat(fallback bool) *FileBeat", fmt.Sprintf("fallback: %v", fallback)))
	fb := &FileBeat{
		RegistryPath: viper.GetString("filebeat.registry_path"),
		InputdPath:   viper.GetString("filebeat.inputd_path"),
	}

	diskStore, err := store.NewDiskStore(fb.RegistryPath+"/filebeat", logFileName, fallback)
	if err != nil {
		zap.L().Error("filebeat", zap.String("disk store err msg", fmt.Sprintf("Failed to new diskStore: %s", err)))
		return nil
	}
	fb.disk = diskStore

	return fb
}

// {"version":"1"}
func (fb *FileBeat) WriteMeta() error {
	filename := fmt.Sprintf("%s/filebeat/meta.json", fb.RegistryPath)
	err := common.WriteFile(filename, []byte(`{"version":"1"}`))
	if err != nil {
		zap.L().Error("filebeat", zap.String("disk store err msg", fmt.Sprintf("Failed to write %s: %s", filename, err)))
	}
	return err
}

func (fb *FileBeat) WriteStates(states []file.State) error {
	for i := range states {
		key := fileStatePrefix + states[i].Id
		if err := fb.Set(key, states[i]); err != nil {
			zap.L().Error("filebeat",
				zap.String("disk store err msg", fmt.Sprintf("Failed to set state: %s", err)),
				zap.String("state: ", fmt.Sprintf("id: %s, path: %s", states[i].Id, states[i].Source)))
			return err
		}
		zap.L().Debug("filebeat",
			zap.String("state: ", fmt.Sprintf("id: %s, offset: %v, path: %s", states[i].Id, states[i].Offset, states[i].Source)))
	}

	baseName := filepath.Join(fb.RegistryPath, logFileName)
	zap.L().Debug("filebeat",
		zap.String("write disk store", fmt.Sprintf("Write registry file: %s (length: %v)", baseName, len(states))))
	return nil
}

func (fb *FileBeat) Set(key string, value interface{}) error {
	var tmp store.MapStr
	if err := typeconv.Convert(&tmp, value); err != nil {
		return err
	}
	return fb.disk.LogOperation(&store.OpSet{K: key, V: tmp})
}

func (fb *FileBeat) ReadStates() ([]file.State, error) {
	if err := fb.disk.LoadLogFile(); err != nil {
		zap.L().Error("filebeat",
			zap.String("LoadLogFile()", fmt.Sprintf("Failed to read state: %s", err)))
		return nil, err
	}

	if states, err := fb.disk.LoadStates(); err != nil {
		zap.L().Error("filebeat",
			zap.String("LoadStates()", fmt.Sprintf("Failed to read state: %s", err)))
		return nil, err
	} else {
		return states, nil
	}
}
