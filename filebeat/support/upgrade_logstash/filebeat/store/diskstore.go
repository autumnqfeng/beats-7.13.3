package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/elastic/beats/v7/filebeat/input/file"
	"github.com/elastic/beats/v7/filebeat/support/upgrade_logstash/common"
	"github.com/elastic/beats/v7/libbeat/common/transform/typeconv"
	"go.uber.org/zap"
)

const defaultBufferSize = 4 * 1024
const defaultFileMode os.FileMode = 0600

type DiskStore struct {
	registryPath string
	logFilePath  string // current log file
	nextTxID     uint64
	logFile      *os.File
	table        map[string]entry
}

type logAction struct {
	Op string `json:"Op"`
	ID uint64 `json:"id"`
}

type entry struct {
	value map[string]interface{}
}

func NewDiskStore(
	registryPath string,
	logFile string,
	fallback bool,
) (*DiskStore, error) {
	s := &DiskStore{
		registryPath: registryPath,
		logFilePath:  filepath.Join(registryPath, logFile),
		nextTxID:     1,
		logFile:      nil,
		table:        make(map[string]entry),
	}

	// default fallback is false(logstash -> filebeat), need to delete registry log
	if !fallback {
		if err := s.deleteLog(); err != nil {
			zap.L().Error("filebeat", zap.String("diskStore", fmt.Sprintf("Failed to init DiskStore %v", err)))
			return nil, err
		}
	}
	return s, s.tryOpenLog()
}

func (s *DiskStore) deleteLog() error {
	if !common.Exist(s.registryPath) {
		if err := os.MkdirAll(s.registryPath, os.ModePerm); err != nil {
			zap.L().Error("filebeat", zap.String("diskStore", fmt.Sprintf("Failed to creat dir %v: %v", s.registryPath, err)))
			return err
		}
	}

	if err := common.DeleteFile(s.logFilePath); err != nil {
		zap.L().Error("filebeat", zap.String("diskStore", fmt.Sprintf("Failed to delete file %v: %v", s.logFilePath, err)))
		return err
	}
	return nil
}

func (s *DiskStore) tryOpenLog() error {
	flags := os.O_RDWR | os.O_CREATE

	f, err := os.OpenFile(s.logFilePath, flags, defaultFileMode)
	if err != nil {
		zap.L().Error("filebeat", zap.String("diskStore", fmt.Sprintf("Failed to open file %v: %v", s.logFilePath, err)))
		return err
	}

	_, err = f.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	s.logFile = f
	return nil
}

func (s *DiskStore) LogOperation(op Op) error {
	if op == nil {
		return nil
	}

	writer := bufio.NewWriterSize(&ensureWriter{s.logFile}, defaultBufferSize)
	counting := &countWriter{w: writer}

	enc := newJSONEncoder(counting)
	if err := enc.Encode(logAction{Op: op.Name(), ID: s.nextTxID}); err != nil {
		return err
	}
	_ = writer.WriteByte('\n')

	if err := enc.Encode(op); err != nil {
		return err
	}
	_ = writer.WriteByte('\n')

	if err := writer.Flush(); err != nil {
		return err
	}

	s.nextTxID++
	return nil
}

func (s *DiskStore) LoadStates() ([]file.State, error) {
	states := make([]file.State, 0)
	for _, V := range s.table {
		s := file.State{}
		if err := V.Decode(&s); err != nil {
			return nil, err
		}
		states = append(states, s)
	}
	return states, nil
}

func (s *DiskStore) LoadLogFile() (err error) {
	err = s.readLogFile(func(rawOp Op) error {
		switch op := rawOp.(type) {
		case *OpSet:
			s.Set(op.K, op.V)
		case *OpRemove:
			s.Remove(op.K)
		}
		return nil
	})
	return err
}

// readLogFile iterates all operations found in the transaction log.
func (s *DiskStore) readLogFile(fn func(Op) error) error {
	path := s.logFilePath
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	for dec.More() {
		var act logAction
		if err := dec.Decode(&act); err != nil {
			return err
		}

		var op Op
		switch act.Op {
		case opValSet:
			op = &OpSet{}
		case opValRemove:
			op = &OpRemove{}
		}

		if err := dec.Decode(op); err != nil {
			return err
		}

		if err := fn(op); err != nil {
			return err
		}
	}

	return nil
}

func (s *DiskStore) Set(key string, value MapStr) {
	s.table[key] = entry{value: value}
}

func (s *DiskStore) Remove(key string) bool {
	_, exists := s.table[key]
	if !exists {
		return false
	}
	delete(s.table, key)
	return true
}

func (e entry) Decode(to interface{}) error {
	return typeconv.Convert(to, e.value)
}
