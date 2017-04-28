package gsnake

import (
	"github.com/golang/glog"
	"log"
)

type FilesHandler struct {
	dirs   []string
	Reader *DirReader
	paths  []string
	conf   *Conf

	ProcessedEventCounts int64
}

func NewFilesHandler(conf *Conf, dirs []string) (h *FilesHandler, err error) {
	h = &FilesHandler{}
	h.dirs = dirs
	h.conf = conf

	h.Reader, err = NewDirReader(conf)

	return h, nil
}

func (h *FilesHandler) Run() {
	glog.V(3).Infof("FilesHandler Running ...")
	for _, dir := range h.dirs {
		ff, err := LookupFiles(dir, h.conf.FilePattern)
		if err != nil {
			log.Fatal("LoopupFiles <%s> with pathern <%s> failed : %v\n", dir, h.conf.FilePattern, err.Error())
		}

		glog.V(3).Infof("%v existing files: %v", dir, ff)
		for _, f := range ff {
			h.OnFileCreated(f)
		}
	}
	h.Reader.Read()
}

func (h *FilesHandler) Stop() {
	h.Reader.Stop()
}

func (h *FilesHandler) OnFileModified(file string) {
	h.ProcessedEventCounts++
	h.Reader.OnFileModified(file)
}

func (h *FilesHandler) OnFileDeleted(file string) {
	h.ProcessedEventCounts++
	h.Reader.OnFileDeleted(file)
}

func (h *FilesHandler) OnFileCreated(file string) {
	h.ProcessedEventCounts++
	h.Reader.OnFileCreated(file)
}
