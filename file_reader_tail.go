package gsnake

import (
	"github.com/golang/glog"
	"io"
	"os"
	"path"
	"time"
)

type TextFileReader interface {
	LoadFile(filepath string, fp *os.File) error
	ReadLine() ([]byte, error)
}

type FileTailReader struct {
	path   string
	offset int64
	fp     *os.File
	r      TextFileReader
	gzr    *GzipFileReader
	tr     *PTailFileReader
	dr     *DirReader
}

func NewFileTailReader(dr *DirReader) *FileTailReader {
	r := &FileTailReader{
		path:   "",
		offset: 0,
		fp:     nil,
		dr:     dr,
	}

	r.tr = NewPTailFileReader()
	r.gzr = NewGzipFileReader()

	return r
}

func (r *FileTailReader) SelectReader(file string) {
	glog.V(3).Infof("file ext is  %v ", path.Ext(file))
	r.r = r.tr
	if path.Ext(file) == ".gz" {
		r.r = r.gzr
	}
}
func (r *FileTailReader) ReadFile(file string, offset int) (err error) {
	if r.fp != nil {
		r.fp.Close()
		r.fp = nil
	}

	r.path = file
	r.fp, err = os.OpenFile(file, os.O_RDONLY, 0644)
	r.offset = int64(offset)
	if err != nil {
		glog.Errorf("OpenFile <%s> failed : %v\n", file, err.Error())
		return
	}
	glog.V(3).Infof("OpenFile %v OK", file)
	defer r.fp.Close()

	if offset > 0 {
		r.fp.Seek(int64(offset), os.SEEK_SET)
	}
	r.SelectReader(file)

	if r.r.LoadFile(file, r.fp) != nil {
		return
	}

	r.readTextFile()

	return
}
func (r *FileTailReader) Offset() (offset int) {
	return int(r.offset)
}

func (r *FileTailReader) readTextFile() {
	for r.dr.Running {
		line, err := r.r.ReadLine()
		glog.V(3).Infof("ReadLine: current-read=<%s> <%v>", string(line), err)

		if err == io.EOF {
			if len(line) > 0 {
				if line[len(line)-1] == '\n' {
					r.offset, _ = r.fp.Seek(0, os.SEEK_CUR)
					r.onRecord(line)
				}
			}
			time.Sleep(time.Second)
			break
		} else if err != nil {
			glog.Errorf("Read data from <%s> failed : %v", r.path, err.Error())
			break
		}
		glog.V(3).Infof("============> Read a line [%v]", string(line))
		if line[len(line)-1] == '\n' {
			r.offset, _ = r.fp.Seek(0, os.SEEK_CUR)
			r.onRecord(line)
		}
	}
}

func (r *FileTailReader) onRecord(line []byte) {
	if r.dr.conf.owner.textModule != nil {
		r.dr.conf.owner.textModule.OnRecord(line)
	}
}
