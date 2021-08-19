package state

import (
	"github.com/elastic/beats/v7/filebeat/input/file"
	file2 "github.com/elastic/beats/v7/libbeat/common/file"
	"os"
	"time"
)

// NewState creates a new file state
func NewState(fileInfo os.FileInfo, path string, meta map[string]string) file.State {
	if len(meta) == 0 {
		meta = nil
	}

	s := file.State{
		Fileinfo:    fileInfo,
		Source:      path,
		FileStateOS: file2.GetOSState(fileInfo),
		Timestamp:   time.Now(),
		TTL:         -1, // By default, state does have an infinite ttl
		Type:        "log",
		Meta:        meta,
	}

	s.Id = "native::" + s.FileStateOS.String()
	s.IdentifierName = "native"

	return s
}
