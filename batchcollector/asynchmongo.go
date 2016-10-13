package main

// AsynchMongoDB is usually slower, not worth the effort

import (
	"log"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// AsynchMongoDB is a mongo connection and methods to insert data into db
type AsynchMongoDB struct {
	session    *mgo.Session
	db         *mgo.Database
	collection *mgo.Collection
	insert     chan Jobentry
	update     chan updatepair
	shutdown   chan int
}

type updatepair struct {
	query  bson.M
	change bson.M
}

// NewAsynchMongo creates inserter and workers
func NewAsynchMongo() *AsynchMongoDB {
	mongo := new(AsynchMongoDB)
	var err error

	mongo.session, err = mgo.Dial(config.MongoServer)
	if err != nil {
		panic("could not access mongo DB on " + config.MongoServer)
	}

	mongo.db = mongo.session.DB(config.MongoDB)
	mongo.collection = mongo.db.C(config.Collection)

	mongo.insert = make(chan Jobentry)
	mongo.update = make(chan updatepair)
	mongo.shutdown = make(chan int)

	go mongo.AsynchWorker()

	return mongo
}

// AsynchWorker is doing aynschronous work
func (m *AsynchMongoDB) AsynchWorker() {
	for {
		select {
		case data := <-m.insert: // we flush updates and insert
			//log.Println(data)
			var delay = 1
		retryInsert:
			err := m.collection.Insert(data)
			//if err != nil {
			//log.Println(err.Error())
			//}
			if err != nil && !mgo.IsDup(err) && (strings.Contains(err.Error(), "no reachable") || err.Error() == "EOF") && delay < retryCount {
				log.Println("    error in insert, refreshing session and waiting...", err, delay, "/", retryCount)
				m.session.Refresh()
				time.Sleep(retryDelay * time.Second)
				delay += 1
				goto retryInsert
			}
		case data := <-m.update: // we flush inserts and update
			var delay = 1
		retryUpdate:
			err := m.collection.Update(data.query, data.change)
			//if err != nil {
			//log.Println(err.Error())
			//}
			if err != nil && (strings.Contains(err.Error(), "no reachable") || err.Error() == "EOF") && delay < retryCount {
				log.Println("    error in update, refreshing session and waiting...", err, delay, "/", retryCount)
				m.session.Refresh()
				time.Sleep(retryDelay * time.Second)
				delay += 1
				goto retryUpdate
			}
		case <-m.shutdown: // we flush all data
			m.shutdown <- 1
			return
		}
	}
}

// Shutdown flushes all date from bulk worker
func (m *AsynchMongoDB) Shutdown() {
	m.shutdown <- 1
	// wait for shutdown
	<-m.shutdown
}

// InsertJob inserts a job into database
func (m *AsynchMongoDB) InsertJob(jobid string, start time.Time) {
	year := strconv.Itoa(time.Now().Year())
	m.insert <- Jobentry{
		strings.Trim(jobid, "'") + "-" + year,
		strings.Trim(jobid, "'") + "-" + year,
		"",
		int32(start.Unix()),
		-1,
		"",
		"",
		-1,
	}
}

// InsertCompleteJob inserts a filled jobentry struct
func (m *AsynchMongoDB) InsertCompleteJob(job Jobentry) {
	m.insert <- job
}

// AddJobInfo inserts a job into database
func (m *AsynchMongoDB) AddJobInfo(jobid, uid, cmd, nids string) {
	year := strconv.Itoa(time.Now().Year())
	query := bson.M{"_id": strings.Trim(jobid, "'") + "-" + year}
	change := bson.M{"$set": bson.M{"owner": uid, "cmd": strings.Trim(cmd, "'"), "nids": nids}}
	m.update <- updatepair{query, change}
}

// EndJob inserts a job into database
func (m *AsynchMongoDB) EndJob(jobid string, end time.Time) {
	year := strconv.Itoa(time.Now().Year())
	query := bson.M{"_id": strings.Trim(jobid, "'") + "-" + year}
	change := bson.M{"$set": bson.M{"end": int32(end.Unix())}}
	m.update <- updatepair{query, change}
}
