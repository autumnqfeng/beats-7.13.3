package state

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/elastic/beats/v7/filebeat/input/file"
	"go.uber.org/zap"
)

// getFiles returns all files which have to be logstash
// All globs are expanded and then directory and excluded files are removed
func GetFileInfos(collectFilePaths []string) map[string]os.FileInfo {
	paths := map[string]os.FileInfo{}

	for _, path := range collectFilePaths {
		matches, err := filepath.Glob(path)
		if err != nil {
			zap.L().Error(fmt.Sprintf("glob(%s) failed: %v", path, err))
			continue
		}

		// Check any matched files to see if we need to start a logstash
		for _, file := range matches {

			// Fetch Lstat File info to detected also symlinks
			fileInfo, err := os.Lstat(file)
			if err != nil {
				zap.L().Debug("", zap.String("input", fmt.Sprintf("lstat(%s) failed: %s", file, err)))
				continue
			}

			if fileInfo.IsDir() {
				zap.L().Debug("", zap.String("input", fmt.Sprintf("Skipping directory: %s", file)))
				continue
			}

			isSymlink := fileInfo.Mode()&os.ModeSymlink > 0
			if isSymlink {
				zap.L().Debug("", zap.String("input", fmt.Sprintf("File %s it is a symlink.", file)))
				//continue
			}

			// Fetch Stat file info which fetches the inode. In case of a symlink, the original inode is fetched
			fileInfo, err = os.Stat(file)
			if err != nil {
				zap.L().Debug("", zap.String("input", fmt.Sprintf("stat(%s) failed: %s", file, err)))
				continue
			}

			paths[file] = fileInfo
		}
	}

	return paths
}

func GetFileState(path string, info os.FileInfo) (*file.State, error) {
	var err error
	var absolutePath string
	absolutePath, err = filepath.Abs(path)
	if err != nil {
		return &file.State{}, fmt.Errorf("could not fetch abs path for file %s: %s", absolutePath, err)
	}
	zap.L().Debug("state", zap.String("input", fmt.Sprintf("Check file for harvesting: %s", absolutePath)))

	meta := make(map[string]string)

	// TODO: Enhanced meta

	// Create new state for comparison
	newState := NewState(info, absolutePath, meta)
	return &newState, nil
}
