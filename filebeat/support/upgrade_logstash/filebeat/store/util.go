package store

import (
	"io"
	"syscall"
)

type ensureWriter struct {
	w io.Writer
}

// countWriter keeps track of the amount of bytes written over time.
type countWriter struct {
	N uint64
	w io.Writer
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.N += uint64(n)
	return n, err
}

func (e *ensureWriter) Write(p []byte) (int, error) {
	var N int
	for len(p) > 0 {
		n, err := e.w.Write(p)
		N, p = N+n, p[n:]
		if err != nil && !isRetryErr(err) {
			return N, err
		}
	}
	return N, nil
}

func isRetryErr(err error) bool {
	return err == syscall.EINTR || err == syscall.EAGAIN
}
