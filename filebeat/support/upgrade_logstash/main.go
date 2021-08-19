package main

import (
	"os"

	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/core"
)

func main() {
	if err := core.Upgrade.Execute(); err != nil {
		os.Exit(1)
	}
}
