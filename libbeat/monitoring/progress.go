package monitoring

import (
	"expvar"
	"os"
	"sync"

	"github.com/elastic/beats/v7/filebeat/input/file"
)

type File struct {
	Source string `json:"source"`
	Size   int64  `json:"size"`
	Offset int64  `json:"offset"`
}

func newFile(source string, size, offset int64) File {
	return File{
		Source: source,
		Size:   size,
		Offset: offset,
	}
}

type Progress struct {
	sync.Mutex
	fileSlice []File
	fun       expvar.Func
}

func NewProgress() *Progress {
	v := &Progress{
		fileSlice: []File{},
	}

	v.fun = func() interface{} {
		return v.fileSlice
	}
	return v
}

func (p *Progress) Report(_ Mode, vs Visitor) {
	switch vs.(type) {
	case *KeyValueVisitor:
		vs.OnInterface(p.fun)
	case *structSnapshotVisitor:
		vs.OnInterface(p.fileSlice)
	case *flatSnapshotVisitor:
		vs.OnInterface(p.fileSlice)
	}
}

func (p *Progress) Update(states []file.State) {
	p.Lock()
	defer p.Unlock()

	fileSlice := make([]File, 0)
	for _, state := range states {
		fileInfo, _ := os.Stat(state.Source)
		fileSlice = append(fileSlice, newFile(state.Source, fileInfo.Size(), state.Offset))
	}
	p.fileSlice = fileSlice
}
