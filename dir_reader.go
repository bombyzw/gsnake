package gsnake

import (
	"github.com/golang/glog"
	"sync"
	"sync/atomic"
	"time"
)

type DirReader struct {
	fr FileReader

	waiting int32
	wakeup  chan int
	Running bool

	status *ProcessStatus
	mutex  sync.Mutex
	files  map[string]bool // The files to be reading
	conf   *Conf

	WakeUpCounts        int64
	ProcessedFileCounts int64
}

func NewDirReader(conf *Conf) (*DirReader, error) {
	r := &DirReader{}
	r.Running = true
	r.waiting = 0
	r.files = map[string]bool{}
	r.wakeup = make(chan int)
	r.fr = r.createReader()
	r.conf = conf

	var err error
	r.status, err = NewProcessStatus(conf.StatusFile)
	if err != nil || r.status == nil {
		glog.Fatal(err)
	}

	return r, nil
}

func (r *DirReader) add(file string) (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.files[file] = true
	glog.V(3).Infof("Add a file and waiting to process: %v", file)
	return err
}

const (
	kModify int = 1
	kCreate int = 2
)

func (r *DirReader) OnFileDeleted(file string) (err error) {
	r.status.OnFileDeleted(file)
	return
}

func (r *DirReader) OnFileModified(file string) (err error) {
	r.add(file)
	if atomic.LoadInt32(&r.waiting) > 0 && r.GetPendingFileCount() >= 1 {
		glog.V(3).Infof("The file <%s> has been modified which we are processing. And the processing goroutine is sleeping, so send kModify signal to it.", file)
		r.wakeup <- kModify
	} else {
		glog.V(3).Infof("do not need to send kModify signal")
	}
	return nil
}

func (r *DirReader) OnFileCreated(file string) (err error) {
	r.add(file)
	if atomic.LoadInt32(&r.waiting) > 0 && r.GetPendingFileCount() >= 1 {
		// r.waiting : we will send a signal only when the goroutine is waiting
		// r.files.Len() == 0 : If we create more than 2 files in the same time, the waiting goroutine may be still waiting when we try to send the second signal
		glog.V(3).Infof("send kCreate signal")
		r.wakeup <- kCreate
	} else {
		glog.V(3).Infof("do not need to send kCreate signal")
	}
	return nil
}

func (r *DirReader) createReader() FileReader {
	return NewFileTailReader(r)
}

func (r *DirReader) Stop() {
	r.Running = false
}

func (r *DirReader) Read() (err error) {
	glog.V(3).Infof("Starting to read files ...")
	for r.Running {
		if r.GetPendingFileCount() == 0 {
			glog.V(3).Infof("No more files. Waiting ...")
			r.Wait()
			r.WakeUpCounts++
			if r.GetPendingFileCount() == 0 {
				//glog.Errorf("This is a logic ERROR, got event but we ignore it right now and lately we should review this code logic.")
				continue
			}
		}

		file := r.nextFile()
		if len(file) == 0 {
			continue
		}
		r.ProcessedFileCounts++

		startTime := time.Now()
		glog.V(3).Infof("Begin to process file %v last pos %v", file, r.status.LastPos(file))
		r.fr.ReadFile(file, r.status.LastPos(file))
		glog.V(3).Infof("Finished to process file %v", file)
		r.status.OnFileProcessingFinished(file, startTime, r.fr.Offset())
	}

	return nil
}

func (r *DirReader) nextFile() string {
	var e string
	r.mutex.Lock()
	if len(r.files) == 0 {
		return ""
	}
	for e, _ = range r.files {
		break
	}
	delete(r.files, e)
	r.mutex.Unlock()

	glog.V(3).Infof("Got a next file : %v", e)
	return e
}

func (r *DirReader) GetPendingFileCount() int {
	r.mutex.Lock()
	c := len(r.files)
	r.mutex.Unlock()
	return c
}

func (r *DirReader) Wait() int {
	atomic.AddInt32(&r.waiting, 1)
	event := <-r.wakeup
	atomic.AddInt32(&r.waiting, -1)
	return event
}
