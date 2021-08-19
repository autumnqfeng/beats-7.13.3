package common

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/elastic/beats/v7/x-pack/elastic-agent/pkg/agent/errors"
)

const defaultFileMode os.FileMode = 0600

func ReadFileToLines(filename string) ([]string, error) {
	if file, err := os.Open(filename); err != nil {
		return nil, err
	} else {
		lines := make([]string, 0)

		buf := bufio.NewScanner(file)
		for {
			if !buf.Scan() {
				break
			}
			line := buf.Text()
			lines = append(lines, strings.TrimSpace(line))
		}
		return lines, nil
	}
}

func WriteFile(filename string, data []byte) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultFileMode)
	if err != nil {
		return err
	}
	defer f.Close()

	if n, err := f.Write(data); err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	return nil
}

func IsDir(path string) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return stat.IsDir(), nil
}

func IsFile(path string) (bool, error) {
	isDir, err := IsDir(path)
	return !isDir, err
}

func DeleteFile(path string) error {
	isFile, err := IsFile(path)
	if err != nil {
		return err
	}
	if !isFile {
		return errors.New(fmt.Sprintf("%s is not file.", path))
	}
	return os.Remove(path)
}
