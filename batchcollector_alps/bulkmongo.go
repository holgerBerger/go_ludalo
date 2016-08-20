package main

import (
	"log"
	"reflect"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// BulkMongoDB is a mongo connection and methods to insert data into db
type BulkMongoDB struct {
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

// NewBulkMongo creates inserter and workers
func NewBulkMongo() *BulkMongoDB {
	mongo := new(BulkMongoDB)
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

	go mongo.BulkWorker()

	return mongo
}

// I makes []Interface{} from []...
func I(array interface{}) []interface{} {

	v := reflect.ValueOf(array)
	t := v.Type()

	if t.Kind() != reflect.Slice {
		log.Panicf("`array` should be %s but got %s", reflect.Slice, t.Kind())
	}

	result := make([]interface{}, v.Len(), v.Len())

	for i := 0; i < v.Len(); i++ {
		result[i] = v.Index(i).Interface()
	}

	return result
}

// BulkWorker is doing aynschronous work
func (m *BulkMongoDB) BulkWorker() {
	insertbuffer := make([]bson.M, 0, 1024)
	querybuffer := make([]bson.M, 0, 1024)
	changebuffer := make([]bson.M, 0, 1024)

	inserted := false
	updated := false

loop:
	for {
		select {
		case data := <-m.insert: // we flush updates and insert
			//fmt.Println("insert")
			if updated {
				//fmt.Println(" run update")
				for i := range querybuffer {
					m.collection.Update(querybuffer[i], changebuffer[i])
				}
				querybuffer = querybuffer[:0]
				changebuffer = changebuffer[:0]
				updated = false
			}
			insertbuffer = append(insertbuffer, data)
			inserted = true
		case data := <-m.update: // we flush inserts and update
			//fmt.Println("update")
			if inserted {
				//fmt.Println(" run insert")
				m.collection.Insert(I(insertbuffer)...)
				insertbuffer = insertbuffer[:0]
				inserted = false
			}
			querybuffer = append(querybuffer, data.query)
			changebuffer = append(changebuffer, data.change)
			updated = true
		case <-m.shutdown: // we flush all data
			if inserted {
				m.collection.Insert(I(insertbuffer)...)
				inserted = false
			}
			if updated {
				for i := range querybuffer {
					m.collection.Update(querybuffer[i], changebuffer[i])
				}
				updated = false
			}
			break loop
		}
	}

	m.shutdown <- 1
}

// Shutdown flushes all date from bulk worker
func (m *BulkMongoDB) Shutdown() {
	m.shutdown <- 1
	// wait for shutdown
	<-m.shutdown
}

// InsertJob inserts a job into database
func (m *BulkMongoDB) InsertJob(jobid string, start time.Time) {
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
func (m *BulkMongoDB) AddJobInfo(jobid, uid, cmd, nids string) {
	query := bson.M{"_id": strings.Trim(jobid, "'")}
	change := bson.M{"$set": bson.M{"uid": uid, "cmd": strings.Trim(cmd, "'"), "nids": nids}}
	m.update <- updatepair{query, change}
}

// EndJob inserts a job into database
func (m *BulkMongoDB) EndJob(jobid string, end time.Time) {
	query := bson.M{"_id": strings.Trim(jobid, "'")}
	change := bson.M{"$set": bson.M{"end": int32(end.Unix())}}
	m.update <- updatepair{query, change}
}
