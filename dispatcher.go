package gsnake

import (
	"github.com/golang/glog"
	"github.com/howeyc/fsnotify"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

type Dispatcher struct {
	watcher    *fsnotify.Watcher
	dirs       []string
	Handler    *FilesHandler
	conf       *Conf
	textModule TextModule

	EventCounts int64
}

func NewDispatcher(conf *Conf) (d *Dispatcher, err error) {
	glog.V(3).Infof("NewDispatcher")
	d = &Dispatcher{}
	d.watcher, err = fsnotify.NewWatcher()
	conf.owner = d
	d.conf = conf
	if err != nil {
		glog.Fatal(err)
	}

	d.dirs, err = filepath.Glob(d.conf.DirPattern)
	glog.V(3).Infof("Process %v dirs %v %v", d.conf.DirPattern, d.dirs, err)
	for _, dir := range d.dirs {
		if !IsDir(dir) {
			glog.Fatal("Can't Process !dir path " + dir)
		}
	}

	d.Handler, err = NewFilesHandler(conf, d.dirs)
	if err != nil {
		glog.Fatal(err)
	}

	return d, err
}

func (d *Dispatcher) Run() {
	glog.Infof("Watching <%v>", d.dirs)
	glog.Infof("Current Module Type <%v>", d.textModule)
	for _, dir := range d.dirs {
		err := d.watcher.Watch(dir)
		if err != nil {
			glog.Fatal("Watch event of " + dir + " FAILED: " + err.Error())
		}
	}
	//start to watch the file event and wait the goroutine started
	var wg sync.WaitGroup
	wg.Add(1)
	go d.watchEvent(&wg)
	wg.Wait()

	//start file Handler to run
	d.Handler.Run()
}

func (d *Dispatcher) Stop() {
	d.Handler.Stop()
}

func (d *Dispatcher) watchSignal(wg *sync.WaitGroup) {
	glog.Infof("stop dispacher who is Watching <%v>", d.dirs)
	defer wg.Done()

	// Set up channel on which to send signal notifications.
	c := make(chan os.Signal, 1)
	signal.Notify(c)

	// Block until a signal is received.
	go func() {
		defer close(c)
		for {
			s := <-c
			glog.Errorf("Got signal %v", s)
			if s == syscall.SIGHUP || s == syscall.SIGINT || s == syscall.SIGTERM {
				signal.Stop(c)
				d.Stop()
				break
			}
		}
	}()
}

func (d *Dispatcher) Close() {
	d.watcher.Close()
}

func (d *Dispatcher) Register(m TextModule) {
	d.textModule = m
}

func (d *Dispatcher) onCreate(ev *fsnotify.FileEvent) {
	if IsDir(ev.Name) {
		d.watcher.Watch(ev.Name)
		//Ignore this : FIXME if we renamed ev.Name later, we should add the new name to the watching list.
	} else {
		if ok, _ := filepath.Match(d.conf.FilePattern, filepath.Base(ev.Name)); ok {
			d.Handler.OnFileCreated(ev.Name)
		} else {
			glog.V(3).Infof("Create a file <%v> but does not match the file pattern <%v>", ev.Name, d.conf.FilePattern)
		}
	}
}

func (d *Dispatcher) onDelete(ev *fsnotify.FileEvent) {
	d.Handler.OnFileDeleted(ev.Name)
}

func (d *Dispatcher) onModify(ev *fsnotify.FileEvent) {
	d.Handler.OnFileModified(ev.Name)
}

func (d *Dispatcher) watchEvent(wg *sync.WaitGroup) {
	wg.Done()
	for {
		select {
		case ev := <-d.watcher.Event:
			d.EventCounts++
			if ev != nil && strings.ToLower(ev.Name) != strings.ToLower(d.conf.StatusFile) {
				glog.V(3).Info("event:", ev, " name=", ev.Name)
				if ev.IsCreate() {
					d.onCreate(ev)
				} else if ev.IsDelete() {
					d.onDelete(ev)
				} else if ev.IsModify() {
					d.onModify(ev)
				} else {
					glog.V(3).Info("don't care this event:", ev)
				}
			}
		case err := <-d.watcher.Error:
			if err != nil {
				glog.V(3).Info("error:", err)
			}
		}
	}
}
