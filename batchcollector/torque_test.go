package main

import (
	"bufio"
	"log"
	"os"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// TestSimple1 tests a simple entry from a file
func TestTorqueSimple1(t *testing.T) {
	readConf("testinput/torque.config")

	mongo := NewMongo()

	// go silent
	defaultOut := os.Stderr
	null := new(devnull)
	log.SetOutput(null)

	// read file
	_ = newTorqueLogfile("testinput/Simple1Torque", mongo)

	// get db entry
	result := Jobentry{}

	// try complete entry
	mongo.collection.Find(bson.M{"_id": "799170"}).One(&result)

	reference := Jobentry{"799170", "799170", "user1", 1471843328, 1471861570, "", "n091601,n091502,n091501,n091402,n091401,n091302,n091301,n091202,n091201,n091002,n091001,n090902,n090901", -1}

	if reference != result {
		t.Fail()
	}

	//  try incomplete entry
	mongo.collection.Find(bson.M{"_id": "799171"}).One(&result)

	reference = Jobentry{"799171", "799171", "user1", 1471843328, -1, "", "n091601,n091502,n091501,n091402,n091401,n091302,n091301,n091202,n091201,n091002,n091001,n090902,n090901", -1}

	if reference != result {
		t.Fail()
	}

	// go noisy again
	log.SetOutput(defaultOut)

	// drop DB
	mongo.db.DropDatabase()

}

// TestEvent1 tests the event loop writing in background into todayfile()
func TestTorqueEvent1(t *testing.T) {

	QUIT = make(chan int)

	readConf("testinput/torque.config")

	logtype = TORQUE

	mongo := NewMongo()

	// go silent
	defaultOut := os.Stderr
	null := new(devnull)
	log.SetOutput(null)

	fin, err := os.Open("testinput/Simple1Torque")
	if err != nil {
		panic(err)
	}
	fout, err := os.Create(todayname())
	if err != nil {
		panic(err)
	}

	inreader := bufio.NewReader(fin)

	// read and write first line, so file exists
	line, err := inreader.ReadBytes('\n')
	if err != nil {
		t.Fail()
	}
	fout.Write(line)

	// send eventloop in background
	go eventloop(mongo)

	// read and write remaining lines
	for {
		line, err := inreader.ReadBytes('\n')
		if err != nil {
			break
		}
		fout.Write(line)
		time.Sleep(100 * time.Millisecond)
	}

	// end eventloop
	QUIT <- 1

	// get db entry
	result := Jobentry{}
	mongo.collection.Find(bson.M{"_id": "799170"}).One(&result)

	// go noisy again
	log.SetOutput(defaultOut)

	reference := Jobentry{"799170", "799170", "user1", 1471843328, 1471861570, "", "n091601,n091502,n091501,n091402,n091401,n091302,n091301,n091202,n091201,n091002,n091001,n090902,n090901", -1}

	if reference != result {
		t.Fail()
	}

	//  try incomplete entry
	mongo.collection.Find(bson.M{"_id": "799171"}).One(&result)

	reference = Jobentry{"799171", "799171", "user1", 1471843328, -1, "", "n091601,n091502,n091501,n091402,n091401,n091302,n091301,n091202,n091201,n091002,n091001,n090902,n090901", -1}

	if reference != result {
		t.Fail()
	}

	// delete temp faile
	os.Remove(todayname())

	// drop DB
	mongo.db.DropDatabase()
}
