package main

import (
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// MongoDB is a mongo connection and methods to insert data into db
type MongoDB struct {
	session    *mgo.Session
	db         *mgo.Database
	collection *mgo.Collection
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

	return mongo
}

// InsertJob inserts a job into database
func (m *MongoDB) InsertJob(jobid string, start time.Time) {
	m.collection.Insert(bson.M{
		"_id":   strings.Trim(jobid, "'"),
		"jobid": strings.Trim(jobid, "'"),
		"owner": "",
		"start": int32(start.Unix()),
		"end":   -1,
		"nids":  "",
		"cmd":   "",
		"calc":  -1,
	})
}

// AddJobInfo inserts a job into database
func (m *MongoDB) AddJobInfo(jobid, uid, cmd, nids string) {
	query := bson.M{"_id": strings.Trim(jobid, "'")}
	change := bson.M{"$set": bson.M{"uid": uid, "cmd": strings.Trim(cmd, "'"), "nids": nids}}
	err := m.collection.Update(query, change)
	if err != nil {
		// log.Println("could not update", jobid)
	}
}

// EndJob inserts a job into database
func (m *MongoDB) EndJob(jobid string, end time.Time) {
	query := bson.M{"_id": strings.Trim(jobid, "'")}
	change := bson.M{"$set": bson.M{"end": int32(end.Unix())}}
	err := m.collection.Update(query, change)
	if err != nil {
		// log.Println("could not update", jobid)
	}
}
