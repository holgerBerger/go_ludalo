package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"strconv"
	"time"
)

// TorqueLogfile is an open logfile
type TorqueLogfile struct {
	name   string
	file   *os.File
	mongo  MongoInserter
	reader *bufio.Reader
}

// newLogfile opens a file and reads to current end
func newTorqueLogfile(name string, mongo MongoInserter) *TorqueLogfile {
	file, err := os.Open(name)
	if err != nil {
		panic("could not open file" + name)
	}
	reader := bufio.NewReaderSize(file, 1024*1024) // reader with 1MB buffer
	logfile := TorqueLogfile{name, file, mongo, reader}
	log.Println("opened new logfile", name)

	logfile.readToEnd()

	return &logfile
}

func (l *TorqueLogfile) getName() string {
	return l.name
}

// readToEnd reads and process the file to the end
func (l *TorqueLogfile) readToEnd() {

	var (
		start, end int
		owner      string
		nodelist   []string
		jobstarts  map[string]Jobentry
		jobends    map[string]Jobentry
		nids       string
	)

	jobstarts = make(map[string]Jobentry)
	jobends = make(map[string]Jobentry)

	for {
		line, err := l.reader.ReadBytes('\n')
		//fmt.Println(line)
		if err != nil {
			break
		}

		tokens := bytes.Split(bytes.TrimSpace(line), []byte(";"))

		if len(tokens) > 1 &&
			(bytes.Compare(tokens[1], []byte("S")) == 0 ||
				bytes.Compare(tokens[1], []byte("E")) == 0 ||
				bytes.Compare(tokens[1], []byte("A")) == 0 ||
				bytes.Compare(tokens[1], []byte("D")) == 0) {

			jobid := string(tokens[2])
			datestr := string(tokens[0])

			if bytes.Compare(tokens[1], []byte("S")) == 0 {
				fields := bytes.Split(tokens[3], []byte(" "))

				for _, i := range fields {

					if bytes.Index(i, []byte("start")) == 0 {
						start, err = strconv.Atoi((string(bytes.Split(i, []byte("="))[1])))
						if err != nil {
							panic(err)
						}
					}

					if bytes.Index(i, []byte("user")) == 0 {
						owner = string(bytes.Split(i, []byte("="))[1])
					}

					if bytes.Index(i, []byte("exec_host")) == 0 {
						nids = ""
						nodelist = make([]string, 0, 1024)
					outerloop:
						for _, n := range bytes.Split(bytes.Split(i, []byte("="))[1], []byte("+")) {
							// append of not yet in list
							node := string(bytes.Split(n, []byte("/"))[0])
							for _, s := range nodelist {
								if node == s {
									continue outerloop
								}
							}
							nodelist = append(nodelist, node)
							if nids != "" {
								nids = nids + "," + node
							} else {
								nids = node
							}
						}
					}

				}

				end = -1
				jobstarts[jobid] = Jobentry{jobid, jobid, owner, int32(start), int32(end), "", nids, -1}
				log.Println("  new job:", jobid, owner, time.Unix(int64(start), 0).Format(time.RFC822), len(nodelist), "nodes")
				totaljobs++

			} else {

				start = -1
				owner = ""
				nodelist = nil
				// ex:  2016-08-19T00:16:57.339913+02:00
				fmtstr := "01/02/2006 15:04:05"
				t, err := time.Parse(fmtstr, datestr)
				if err != nil {
					panic(err)
				}
				end = int(t.Unix())
				log.Println("  end job:", jobid, time.Unix(int64(end), 0).Format(time.RFC822))
				jobends[jobid] = Jobentry{jobid, jobid, owner, int32(start), int32(end), "", "", -1}

			}
		}
	}

	// see if we have start and end and avoid insert+update, replace by single insert
	for j := range jobstarts {
		_, ok := jobends[j]
		if !ok {
			// insert jobstarts[j]
			l.mongo.InsertCompleteJob(jobstarts[j])
		} else {
			tmp := jobstarts[j]
			tmp.End = jobends[j].End
			// insert tmp
			l.mongo.InsertCompleteJob(tmp)
		}
	}
	// update remaining ending jobs
	for j := range jobends {
		_, ok := jobstarts[j]
		if !ok {
			// update jobends[j]
			l.mongo.EndJob(jobends[j].Jobid, time.Unix(int64(jobends[j].End), 0))
		}
	}

}
