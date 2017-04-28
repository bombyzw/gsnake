package gsnake

import (
	"bufio"
	"github.com/golang/glog"
	"os"
)

type PTailFileReader struct {
	r *bufio.Reader // The reader of os.File fp
}

func NewPTailFileReader() *PTailFileReader {
	br := &PTailFileReader{
		r: nil,
	}

	return br
}

func (r *PTailFileReader) LoadFile(filepath string, fp *os.File) (err error) {
	if r.r == nil {
		glog.V(3).Infof("LoadFile : it is the first time to come here, we create a new reader: bufio.NewReader(fp)")
		r.r = bufio.NewReader(fp)
	} else {
		glog.V(3).Infof("Reset reader")
		r.r.Reset(fp)
	}

	return nil
}

func (r *PTailFileReader) ReadLine() (line []byte, err error) {
	line, err = r.r.ReadBytes('\n')
	glog.V(3).Infof("len(line)=%v %v", len(line), string(line))
	return line, err
}
