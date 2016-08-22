package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"strings"
)

// Logfile is an open logfile
type AlpsLogfile struct {
	name   string
	file   *os.File
	mongo  MongoInserter
	reader *bufio.Reader
}

// newLogfile opens a file and reads to current end
func newAlpsLogfile(name string, mongo MongoInserter) *AlpsLogfile {
	file, err := os.Open(name)
	if err != nil {
		panic("could not open file" + name)
	}
	reader := bufio.NewReaderSize(file, 1024*1024) // reader with 1MB buffer
	logfile := AlpsLogfile{name, file, mongo, reader}
	log.Println("opened new logfile", name)

	logfile.readToEnd()

	return &logfile
}

func (l *AlpsLogfile) getName() string {
	return l.name
}

// readToEnd reads and process the file to the end
func (l *AlpsLogfile) readToEnd() {

	for {
		line, err := l.reader.ReadBytes('\n')
		//fmt.Println(line)
		if err != nil {
			break
		}

		// Bound apid lines are mapping resids to batchIds, this is the start of
		// a job, but we do not know more that resevration id and batch id
		// and we create a mapping between the two
		if bytes.Index(line, []byte("Bound apid")) > 0 {
			tokens := bytes.Split(bytes.TrimSpace(line), []byte(" "))
			jobid := getvalue(tokens, "batchId")
			resid := getvalue(tokens, "resId")
			time := parsetime(string(tokens[0]))
			log.Println("  new job", jobid, resid, time)
			l.mongo.InsertJob(jobid, time)
			totaljobs++
			res2job.setJob(resid, jobid)
			continue
		}

		// paced apid is a aprun in a reservation, here we learn about owner of job
		if bytes.Index(line, []byte("Placed apid")) > 0 {
			tokens := bytes.Split(bytes.TrimSpace(line), []byte(" "))

			resid := getvalue(tokens, "resId")
			uid := getvalue(tokens, "uid")
			cmd := getvalue(tokens, "cmd0")
			nids := getvalue(tokens, "nids:")

			jobid, err := res2job.getJob(resid)
			if err != nil {
				jobid = unknownjob
			}

			log.Println("    placed job", jobid, resid, uid, cmd, nids)
			l.mongo.AddJobInfo(jobid, uid, cmd, nids)
			continue
		}

		// this is the end of a aprun in a reservation, there can be several of those
		if bytes.Index(line, []byte("Released apid")) > 0 {
			tokens := bytes.Split(bytes.TrimSpace(line), []byte(" "))
			resid := getvalue(tokens, "resId")
			jobid, error := res2job.getJob(resid)
			if error != nil {
				jobid = unknownjob
			}
			log.Println("    end of aprun", jobid, resid)
			continue
		}

		// placeApp message:0x1 'No entry for resId seems to indicate that no further
		// apruns are in queue for that reservration, and the reservation is freed,
		// this is the end of the job
		if bytes.Index(line, []byte("placeApp message:0x1 'No entry for resId")) > 0 {
			tokens := bytes.Split(bytes.TrimSpace(line), []byte(" "))
			resid := strings.TrimRight(getvalue(tokens, "resId"), "'")
			jobid, err := res2job.getJob(resid)
			if err != nil {
				jobid = unknownjob
			}
			time := parsetime(string(tokens[0]))
			log.Println("  end of job", jobid, resid, time)
			l.mongo.EndJob(jobid, time)
			// be so kind and free storage in local DB
			res2job.delJob(resid)
			continue
		}

	}
}
