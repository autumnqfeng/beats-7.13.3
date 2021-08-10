package updater

import (
	"time"
)

type config struct {
	Enabled  bool          `config:"enabled"`
	Host     string        `config:"host"`
	User     string        `config:"user"`
	Password string        `config:"password"`
	Path     string        `config:"path"`
	Period   time.Duration `config:"period"`
	Timeout  time.Duration `config:"timeout"`
}

var defaultConfig = config{
	Enabled: false,
	Host:    "127.0.0.1:1234",
	Path:    defaultConfigPath,
	Period:  15,
	Timeout: 30,
}
