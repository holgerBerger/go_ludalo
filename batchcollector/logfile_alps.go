package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"strings"
	"time"
)

// AlpsLogfile is an open logfile
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
	if res2job == nil {
		res2job = NewRes2job("res2job.db")
	}
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

	var (
		jobstarts  map[string]Jobentry
		jobends    map[string]Jobentry
		jobupdates map[string]Jobentry
	)

	jobstarts = make(map[string]Jobentry)
	jobends = make(map[string]Jobentry)
	jobupdates = make(map[string]Jobentry)

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
			jobid := strings.Trim(getvalue(tokens, "batchId"), "'")
			resid := getvalue(tokens, "resId")
			time := parsetime(string(tokens[0]))
			log.Println("  new job", jobid, resid, time)

			// l.mongo.InsertJob(jobid, time)
			jobstarts[jobid] = Jobentry{jobid, jobid, "", int32(time.Unix()), -1, "", "", -1}

			totaljobs++
			res2job.setJob(resid, jobid)
			continue
		}

		// paced apid is a aprun in a reservation, here we learn about owner of job
		if bytes.Index(line, []byte("Placed apid")) > 0 {
			tokens := bytes.Split(bytes.TrimSpace(line), []byte(" "))

			resid := getvalue(tokens, "resId")
			uid := getvalue(tokens, "uid")
			cmd := strings.Trim(getvalue(tokens, "cmd0"), "'")
			nids := getvalue(tokens, "nids:")

			jobid, err := res2job.getJob(resid)
			if err != nil {
				jobid = unknownjob
			}

			log.Println("    new aprun", jobid, resid, uid, cmd, nids)

			// l.mongo.AddJobInfo(jobid, uid, cmd, nids)
			jobupdates[jobid] = Jobentry{jobid, jobid, uid, -1, -1, cmd, nids, -1}

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
			log.Println("    end aprun", jobid, resid)
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
			log.Println("  end job", jobid, resid, time)

			//l.mongo.EndJob(jobid, time)
			jobends[jobid] = Jobentry{jobid, jobid, "", -1, int32(time.Unix()), "", "", -1}

			// be so kind and free storage in local DB
			res2job.delJob(resid)
			continue
		}

	}

	// following code is an optimization of above inserts,
	// it avoids inserts+updates if a block read at once contains more information
	// about one job and reduces the number of mongo updates, around x5 faster
	// for whole files inserted

	// see if we have start and end and avoid insert+update, replace by single insert
	for j := range jobstarts {
		_, ok := jobupdates[j]
		// get startdate from startm, rest from update
		if ok {
			tmp := jobupdates[j]
			tmp.Start = jobstarts[j].Start
			jobstarts[j] = tmp
		}
		_, ok = jobends[j]
		if ok {
			tmp := jobstarts[j]
			tmp.End = jobends[j].End
			jobstarts[j] = tmp
		}
		l.mongo.InsertCompleteJob(jobstarts[j])
	}
	// update remaining updates (we did not have start for them)
	for j, job := range jobupdates {
		_, ok := jobstarts[j]
		if !ok {
			l.mongo.AddJobInfo(job.Jobid, job.Owner, job.Cmd, job.Nids)
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
