package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"strings"
	"time"
)

const unknownjob = "unknown_job"

// Logfile is an open logfile
type Logfile struct {
	name  string
	file  *os.File
	mongo MongoInserter
}

// get value from key value pairs where value is after key
func getvalue(tokens [][]byte, key string) string {
	for i := range tokens {
		if bytes.Compare(tokens[i], []byte(key)) == 0 {
			return string(tokens[i+1])
		}
	}
	return ""
}

// parsetime from a string, in the format of alps scheduler log
func parsetime(line string) time.Time {
	// ex:  2016-08-19T00:16:57.339913+02:00
	fmt := "2006-01-02T15:04:05.999999-07:00"
	t, err := time.Parse(fmt, line)
	if err != nil {
		log.Println("could not parse time in", line, err)
	}
	return t
}

// newLogfile opens a file and reads to current end
func newLogfile(name string, mongo MongoInserter) *Logfile {
	file, err := os.Open(name)
	if err != nil {
		panic("could not open file" + name)
	}
	logfile := Logfile{name, file, mongo}
	log.Println("opened new logfile", name)
	logfile.readToEnd()

	return &logfile
}

// readToEnd reads and process the file to the end
func (l *Logfile) readToEnd() {
	reader := bufio.NewReader(l.file)

	for {
		line, err := reader.ReadBytes('\n')
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
