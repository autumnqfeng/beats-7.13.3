package adaptor

import (
	"errors"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/fmtstr"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/format"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/json"
)

type Encoder struct {
	fmt  *format.Encoder
	json *json.Encoder
}

type Config struct {
	String *fmtstr.EventFormatString `config:"string" validate:"required"`
}

func init() {
	codec.RegisterType("adaptor", func(info beat.Info, cfg *common.Config) (codec.Codec, error) {
		config := Config{}
		if cfg == nil {
			return nil, errors.New("empty adaptor codec configuration")
		}

		if err := cfg.Unpack(&config); err != nil {
			return nil, err
		}
		jsonConfig := json.Config{
			Pretty:     false,
			EscapeHTML: false,
			LocalTime:  false,
		}
		return New(config.String, info.Version, jsonConfig), nil
	})
}

func New(fmt *fmtstr.EventFormatString, version string, config json.Config) *Encoder {
	fmtEncoder := format.New(fmt)
	jsonEncoder := json.New(version, config)
	return &Encoder{
		fmt:  fmtEncoder,
		json: jsonEncoder,
	}
}

//  select codec according field "codec" in the event
//  default using json
func (e *Encoder) Encode(index string, event *beat.Event) ([]byte, error) {
	v, err := getStringValue("output.codec", event)
	if err != nil {
		return nil, err
	}
	if v == "format" {
		return e.fmt.Encode(index, event)
	}
	return e.json.Encode(index, event)
}

func getStringValue(key string, event *beat.Event) (string, error) {
	tmp, err := event.GetValue(key)
	if err != nil {
		return "", errors.New("get codec value error")
	}
	v, ok := tmp.(string)
	if !ok {
		return "", errors.New("get codec to string error")
	}
	return v, nil
}
