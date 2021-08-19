package core

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var cfg = pflag.StringP("ConfigData", "c", "", "ConfigData file path")
var lg *zap.Logger

func init() {
	pflag.Parse()
	if err := initViper(); err != nil {
		zap.L().Error("init viper filed")
		os.Exit(1)
	}

	Upgrade = NewUpgrade()

	// init log
	if err := initLogger(); err != nil {
		zap.L().Error("init zap log filed")
		os.Exit(1)
	}
}

func initViper() error {
	if *cfg != "" {
		viper.SetConfigFile(*cfg)
	} else {
		viper.AddConfigPath("./filebeat/support/upgrade_logstash")
		viper.SetConfigName("upgrade")
	}
	viper.SetConfigType("yml")

	// Call **Viper** to parse the configuration file
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	return nil
}

func initLogger() (err error) {
	encoder := getEncoder()
	var l = new(zapcore.Level)
	err = l.UnmarshalText([]byte("debug"))
	if err != nil {
		return
	}

	var allCore []zapcore.Core
	allCore = append(allCore, zapcore.NewCore(encoder, zapcore.Lock(os.Stdout), l))

	core := zapcore.NewTee(allCore...)

	lg = zap.New(core, zap.AddCaller())
	// Replace the global logger instance in the ZAP package, and then use only `zap.L()` calls in other packages
	zap.ReplaceGlobals(lg)
	return
}

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeDuration = zapcore.SecondsDurationEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}
