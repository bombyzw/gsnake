package gsnake

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
)

type FileProcessingTime struct {
	Start   time.Time
	End     time.Time
	ReadPos int
	f       string
}

func (t *FileProcessingTime) String() string {
	b := bytes.Buffer{}
	b.WriteString(t.Start.Format("2006/01/02-15:04:05.9999\t"))
	b.WriteString(t.End.Format("2006/01/02-15:04:05.9999\t"))
	b.WriteString(t.f + "\t")
	b.WriteString(strconv.Itoa(t.ReadPos))
	b.WriteString("\n")
	return b.String()
}

type ProcessStatus struct {
	processedFiles map[string]FileProcessingTime // The processed files and the time when starting to process and end

	// The content format of the status file :
	//  It is a text file. Every line represents a processed file.
	//  The line has 3 part
	//      1. start processing date time
	//      2. end of processing date time
	//      3. the name of the file
	//      4. the read position of file
	// For example: 2015/08/28-20:42:12.1231 2015/08/28-20:43:23.3123 /home/s/data/log/xxx.log 100
	StatusFile   string   // The path of the status file which used to store the status information of all processed files
	StatusFileFp *os.File // The file pointer to the status file

	counter int //processed count when
	mutex   sync.Mutex
}

func NewProcessStatus(StatusFile string) (ps *ProcessStatus, err error) {
	ps = &ProcessStatus{}
	ps.StatusFile = StatusFile
	ps.processedFiles = make(map[string]FileProcessingTime)

	if IsExist(StatusFile) {
		ps.StatusFileFp, err = os.OpenFile(StatusFile, os.O_RDWR, 0755)
		if err != nil {
			glog.Errorf("open status file <%v> failed : %v\n", StatusFile, err.Error())
			return nil, err
		}
		if err = ps.parse(); err != nil {
			return nil, err
		}
		ps.StatusFileFp.Seek(0, os.SEEK_END)
	} else {
		ps.StatusFileFp, err = os.OpenFile(StatusFile, os.O_CREATE|os.O_RDWR, 0755)
		if err != nil {
			glog.Errorf("open status file <%v> failed : %v\n", StatusFile, err.Error())
			return nil, err
		}
	}

	return ps, nil
}

func (ps *ProcessStatus) IsProcessed(file string) bool {
	ps.mutex.Lock()
	_, ok := ps.processedFiles[file]
	ps.mutex.Unlock()
	return ok
}

func (ps *ProcessStatus) GetProcessedFiles() map[string]FileProcessingTime {
	return ps.processedFiles
}

func (ps *ProcessStatus) OnFileProcessingFinished(path string, startProcessing time.Time, pos int) {
	ps.counter++
	ps.mutex.Lock()
	var t FileProcessingTime
	ot, found := ps.processedFiles[path]
	t.Start = startProcessing
	t.End = time.Now()
	t.ReadPos = pos
	t.f = path
	ps.processedFiles[path] = t
	ps.mutex.Unlock()

	//TODO fix this may lost last seconds info
	if found {
		if t.End.Unix() == ot.End.Unix() {
			return
		}
	}

	w := ps.StatusFileFp
	w.WriteString(t.String())
	w.Sync()
	ps.counter++
	if ps.counter >= 10000 {
		ps.saveAll()
		ps.counter = 0
	}
}

func (ps *ProcessStatus) OnFileDeleted(path string) {
	ps.mutex.Lock()
	delete(ps.processedFiles, path)
	ps.mutex.Unlock()
}

func (ps *ProcessStatus) Close() {
	defer ps.StatusFileFp.Close()
	if err := ps.saveAll(); err != nil {
		panic(err.Error())
	} // flush all data to files
}
func (ps *ProcessStatus) LastPos(file string) int {
	ps.mutex.Lock()
	t, ok := ps.processedFiles[file]
	ps.mutex.Unlock()
	if ok {
		return t.ReadPos
	}
	return 0
}

func (ps *ProcessStatus) parse() error {
	r := bufio.NewReader(ps.StatusFileFp)
	for {
		line, err := r.ReadString('\n')

		if err == io.EOF {
			break
		}

		if len(line) == 0 {
			continue
		}

		line = strings.TrimSpace(line)
		var start, end, path string
		var pos int
		fmt.Sscanf(line, "%s\t%s\t%s\t%d", &start, &end, &path, &pos)
		var t FileProcessingTime
		t.Start, err = time.Parse("2006/01/02-15:04:05.9999", start)
		if err != nil {
			return fmt.Errorf("ERROR line <%v> %v", line, err.Error())
		}
		t.End, err = time.Parse("2006/01/02-15:04:05.9999", end)
		if err != nil {
			return fmt.Errorf("ERROR line <%v> %v", line, err.Error())
		}
		if len(path) == 0 {
			return fmt.Errorf("ERROR line <%v>, path empty", line)
		}
		t.f = path
		t.ReadPos = pos
		ps.mutex.Lock()
		ps.processedFiles[path] = t
		ps.mutex.Unlock()
	}
	return nil
}

type StringArray []string

func (ss StringArray) Len() int {
	return len(ss)
}

func (ss StringArray) Less(i, j int) bool {
	return ss[i] < ss[j]
}

func (ss StringArray) Swap(i, j int) {
	ss[i], ss[j] = ss[j], ss[i]
}

func (ps *ProcessStatus) saveAll() error {
	//TODO not create bak file
	//bakFilePath := ps.StatusFile + ".bak." + strconv.FormatInt(time.Now().UnixNano(), 10)
	//fp, err := os.OpenFile(bakFilePath, os.O_CREATE|os.O_RDWR, 0755)
	//if err != nil {
	//	return err
	//}
	//ps.StatusFileFp.Seek(0, os.SEEK_SET)
	//io.Copy(fp, ps.StatusFileFp)
	//fp.Sync()
	//fp.Close()

	_, err := ps.StatusFileFp.Seek(0, os.SEEK_SET)
	if err != nil {
		log.Printf("Seek <%s> failed : %v\n", ps.StatusFile, err.Error())
		return err
	}
	err = ps.StatusFileFp.Truncate(0)
	if err != nil {
		log.Printf("Truncate <%s> failed : %v\n", ps.StatusFile, err.Error())
		return err
	}
	//stat, err := ps.StatusFileFp.Stat()
	//log.Printf("%v len=%v", stat.Name(), stat.Size())
	var files StringArray
	ps.mutex.Lock()
	for k, _ := range ps.processedFiles {
		files = append(files, k)
	}
	ps.mutex.Unlock()
	sort.Sort(files)
	//log.Print(files)

	w := ps.StatusFileFp
	for _, f := range files {
		ps.mutex.Lock()
		if t, ok := ps.processedFiles[f]; ok {
			w.WriteString(t.String())
		}
		ps.mutex.Unlock()
	}
	w.Sync()
	return nil
}
