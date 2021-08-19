package store

import (
	"io"

	"github.com/elastic/go-structform/gotype"
	"github.com/elastic/go-structform/json"
)

type JsonEncoder struct {
	out    io.Writer
	folder *gotype.Iterator
}

func newJSONEncoder(out io.Writer) *JsonEncoder {
	e := &JsonEncoder{out: out}
	e.reset()
	return e
}

func (e *JsonEncoder) reset() {
	visitor := json.NewVisitor(e.out)
	visitor.SetEscapeHTML(false)

	var err error

	// create new encoder with custom time.Time encoding
	e.folder, err = gotype.NewIterator(visitor)
	if err != nil {
		panic(err)
	}
}

func (e *JsonEncoder) Encode(v interface{}) error {
	return e.folder.Fold(v)
}
