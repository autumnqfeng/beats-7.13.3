package input_d

import (
	"fmt"
	"github.com/dustin/go-humanize"
	cfg "github.com/elastic/beats/v7/filebeat/config"
	"time"

	"github.com/elastic/beats/v7/filebeat/harvester"
	"github.com/elastic/beats/v7/filebeat/input/log"
	"github.com/elastic/beats/v7/libbeat/cfgfile"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/match"
	"github.com/elastic/beats/v7/libbeat/reader/multiline"
	"github.com/elastic/beats/v7/libbeat/reader/readfile"
	"github.com/elastic/beats/v7/libbeat/reader/readjson"
	"github.com/joeshaw/multierror"
	"go.uber.org/zap"
)

type Config struct {
	harvester.ForwarderConfig `Config:",inline"`
	log.LogConfig             `Config:",inline"`

	// Common
	InputType     string        `Config:"input_type"`
	CleanInactive time.Duration `Config:"clean_inactive" validate:"min=0"`

	// Input
	Enabled        bool                    `Config:"enabled"`
	ExcludeFiles   []match.Matcher         `Config:"exclude_files"`
	IgnoreOlder    time.Duration           `Config:"ignore_older"`
	Paths          []string                `Config:"paths"`
	ScanFrequency  time.Duration           `Config:"scan_frequency" validate:"min=0,nonzero"`
	CleanRemoved   bool                    `Config:"clean_removed"`
	HarvesterLimit uint32                  `Config:"harvester_limit" validate:"min=0"`
	Symlinks       bool                    `Config:"symlinks"`
	TailFiles      bool                    `Config:"tail_files"`
	RecursiveGlob  bool                    `Config:"recursive_glob.enabled"`
	FileIdentity   *common.ConfigNamespace `Config:"file_identity"`

	// Harvester
	BufferSize int    `Config:"harvester_buffer_size"`
	Encoding   string `Config:"encoding"`
	ScanOrder  string `Config:"scan.order"`
	ScanSort   string `Config:"scan.sort"`

	LineTerminator readfile.LineTerminator `Config:"line_terminator"`
	ExcludeLines   []match.Matcher         `Config:"exclude_lines"`
	IncludeLines   []match.Matcher         `Config:"include_lines"`
	MaxBytes       int                     `Config:"max_bytes" validate:"min=0,nonzero"`
	Multiline      *multiline.Config       `Config:"multiline"`
	JSON           *readjson.Config        `Config:"json"`

	// Hidden on purpose, used by the docker input:
	DockerJSON *struct {
		Stream   string `Config:"stream"`
		Partial  bool   `Config:"partial"`
		Format   string `Config:"format"`
		CRIFlags bool   `Config:"cri_flags"`
	} `Config:"docker-json"`

	Output log.Output `Config:"output"`
}

func LoadInputConfigs(file string) ([]*Config, error) {
	configs, err := cfgfile.LoadList(file)
	if err != nil {
		return nil, err
	}

	var result = make([]*Config, 0)
	var errs multierror.Errors
	for _, cfg := range configs {
		inputConfig := defaultConfig()
		if err := cfg.Unpack(&inputConfig); err != nil {
			errs = append(errs, err)
			zap.L().Error(fmt.Sprintf("Error loading Config from file '%s', error %v", file, err))
			continue
		}
		result = append(result, &inputConfig)
	}

	return result, errs.Err()
}

func defaultConfig() Config {
	return Config{
		// Common
		ForwarderConfig: harvester.ForwarderConfig{
			Type: cfg.DefaultType,
		},
		CleanInactive: 0,

		// Input
		Enabled:        true,
		IgnoreOlder:    0,
		ScanFrequency:  10 * time.Second,
		CleanRemoved:   true,
		HarvesterLimit: 0,
		Symlinks:       false,
		TailFiles:      false,
		ScanSort:       "",
		ScanOrder:      "asc",
		RecursiveGlob:  true,
		FileIdentity:   nil,

		// Harvester
		BufferSize:     16 * humanize.KiByte,
		MaxBytes:       10 * humanize.MiByte,
		LineTerminator: readfile.AutoLineTerminator,
		LogConfig: log.LogConfig{
			Backoff:       1 * time.Second,
			BackoffFactor: 2,
			MaxBackoff:    10 * time.Second,
			CloseInactive: 5 * time.Minute,
			CloseRemoved:  true,
			CloseRenamed:  false,
			CloseEOF:      false,
			CloseTimeout:  0,
		},
	}
}
