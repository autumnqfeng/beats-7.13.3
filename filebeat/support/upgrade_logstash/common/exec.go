package common

import (
	"bufio"
	"fmt"
	"go.uber.org/zap"
	"os/exec"
	"strings"
)

// noinspection GoUnreachableCode
func Command(command string) ([]string, error) {
	cmd := exec.Command("/bin/bash", "-c", command)
	result := make([]string, 0, 0)

	// Create the fetch command output pipe
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		zap.L().Error(fmt.Sprintf("Error:can not obtain stdout pipe for command:%s\n", err))
		return nil, err
	}

	// Execute the command
	if err := cmd.Start(); err != nil {
		zap.L().Error(fmt.Sprintln("Error:The command is err,", err))
		return nil, err
	}

	// Use buffered readers
	outputBuf := bufio.NewReader(stdout)

	for {
		// Fetches one row at a time. _ Gets whether the current row is read
		output, _, err := outputBuf.ReadLine()
		if err != nil {

			// Check if you're at the end of the file or you'll get an error
			if err.Error() != "EOF" {
				zap.L().Error(fmt.Sprintf("Error :%s\n", err))
			}
			return result, nil
		}
		result = append(result, strings.Replace(string(output), "\"", "", -1))
		zap.L().Debug("command-exec", zap.String("read-line", fmt.Sprintf("%s", string(output))))
	}

	// wait Method blocks until the command to which it belongs completes its run
	if err := cmd.Wait(); err != nil {
		zap.L().Error(fmt.Sprintln("Execute failed when Wait:", err.Error()))
		return nil, err
	}

	return result, nil
}
