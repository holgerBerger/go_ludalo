package main

import (
	"log"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// parameters for retry
const (
	retryCount = 120
	retryDelay = 5
)

// MongoDB is a mongo connection and methods to insert data into db
type MongoDB struct {
	session    *mgo.Session
	db         *mgo.Database
	collection *mgo.Collection
}

// Jobentry mongo document
type Jobentry struct {
	ID    string `bson:"_id,omitempty"`
	Jobid string `bson:"jobid"`
	Owner string `bson:"owner"`
	Start int32  `bson:"start"`
	End   int32  `bson:"end"`
	Cmd   string `bson:"cmd"`
	Nids  string `bson:"nids"`
	Calc  int    `bson:"calc"`
}

// NewMongo creates db connection
func NewMongo() *MongoDB {
	mongo := new(MongoDB)
	var err error

	mongo.session, err = mgo.Dial(config.MongoServer)
	if err != nil {
		panic("could not access mongo DB on " + config.MongoServer)
	}

	mongo.db = mongo.session.DB(config.MongoDB)
	mongo.collection = mongo.db.C(config.Collection)

	mongo.collection.EnsureIndexKey("start", "end")
	mongo.collection.EnsureIndexKey("jobid")

	return mongo
}

// InsertJob inserts a job into database
func (m *MongoDB) InsertJob(jobid string, start time.Time) {
	var delay = 1
retryInsert:
	err := m.collection.Insert(bson.M{
		"_id":   strings.Trim(jobid, "'"),
		"jobid": strings.Trim(jobid, "'"),
		"owner": "",
		"start": int32(start.Unix()),
		"end":   -1,
		"nids":  "",
		"cmd":   "",
		"calc":  -1,
	})
	if err != nil && !mgo.IsDup(err) && (strings.Contains(err.Error(), "no reachable") || err.Error() == "EOF") && delay < retryCount {
		log.Println("    error in insert, refreshing session and waiting...", err, delay, "/", retryCount)
		m.session.Refresh()
		time.Sleep(retryDelay * time.Second)
		delay += 1
		goto retryInsert
	}
}

// InsertCompleteJob inserts a filled jobentry struct
func (m *MongoDB) InsertCompleteJob(job Jobentry) {
	var delay = 1
retryInsert:
	err := m.collection.Insert(&job)
	if err != nil && !mgo.IsDup(err) && (strings.Contains(err.Error(), "no reachable") || err.Error() == "EOF") && delay < retryCount {
		log.Println("    error in insert, refreshing session and waiting...", err, delay, "/", retryCount)
		m.session.Refresh()
		time.Sleep(retryDelay * time.Second)
		delay += 1
		goto retryInsert
	}
}

// AddJobInfo inserts a job into database
func (m *MongoDB) AddJobInfo(jobid, uid, cmd, nids string) {
	query := bson.M{"_id": strings.Trim(jobid, "'")}
	change := bson.M{"$set": bson.M{"owner": uid, "cmd": strings.Trim(cmd, "'"), "nids": nids}}
	var delay = 1
retryUpdate:
	err := m.collection.Update(query, change)
	if err != nil && (strings.Contains(err.Error(), "no reachable") || err.Error() == "EOF") && delay < retryCount {
		log.Println("    error in update, refreshing session and waiting...", err, delay)
		m.session.Refresh()
		time.Sleep(retryDelay * time.Second)
		delay += 1
		goto retryUpdate
	}
}

// EndJob inserts a job into database
func (m *MongoDB) EndJob(jobid string, end time.Time) {
	query := bson.M{"_id": strings.Trim(jobid, "'")}
	change := bson.M{"$set": bson.M{"end": int32(end.Unix())}}
	var delay = 1
retryUpdate:
	err := m.collection.Update(query, change)
	if err != nil && (strings.Contains(err.Error(), "no reachable") || err.Error() == "EOF") && delay < retryCount {
		log.Println("    error in update, refreshing session and waiting...", err, delay)
		m.session.Refresh()
		time.Sleep(retryDelay * time.Second)
		delay += 1
		goto retryUpdate
	}
}

// Shutdown dummy for synch inserter
func (m *MongoDB) Shutdown() {

}
