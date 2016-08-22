package main

import (
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
	insert     chan bson.M
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

	mongo.insert = make(chan bson.M)
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
			m.collection.Insert(data)
		case data := <-m.update: // we flush inserts and update
			m.collection.Update(data.query, data.change)
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
	m.insert <- bson.M{
		"_id":   strings.Trim(jobid, "'"),
		"jobid": strings.Trim(jobid, "'"),
		"owner": "",
		"start": int32(start.Unix()),
		"end":   -1,
		"nids":  "",
		"cmd":   "",
		"calc":  -1,
	}
}

// AddJobInfo inserts a job into database
func (m *AsynchMongoDB) AddJobInfo(jobid, uid, cmd, nids string) {
	query := bson.M{"_id": strings.Trim(jobid, "'")}
	change := bson.M{"$set": bson.M{"uid": uid, "cmd": strings.Trim(cmd, "'"), "nids": nids}}
	m.update <- updatepair{query, change}
}

// EndJob inserts a job into database
func (m *AsynchMongoDB) EndJob(jobid string, end time.Time) {
	query := bson.M{"_id": strings.Trim(jobid, "'")}
	change := bson.M{"$set": bson.M{"end": int32(end.Unix())}}
	m.update <- updatepair{query, change}
}
