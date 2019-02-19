package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// TestSimple1 tests a simple entry from a file
func TestAlpsSimple1(t *testing.T) {
	readConf("testinput/alps.config")

	mongo := NewMongo()

	// go silent
	defaultOut := os.Stderr
	null := new(devnull)
	log.SetOutput(null)

	// read file
	_ = newAlpsLogfile("testinput/Simple1Alps", mongo)

	// get db entry
	result := Jobentry{}

	// try complete entry
	mongo.collection.Find(bson.M{"_id": "815842-2019"}).One(&result)

	reference := Jobentry{"815842-2019", "815842-2019", "31251", 1470866890, 1470866959, "rule_the_world", "2133-2139,2141-2149,2200-2202,2215-2216,2287-2303,2436,2454-2480,2494,2496-2510,2562,2564,2566-2569,2574-2578,2590-2595,2597-2605,2665-2668,2670-2674,2680-2686,2692,3124-3132,3218-3245,3264-3266,4098-4099,4104-4107,4109-4110,4915-4917,4981,5371,5839,5843-5844,5905,5921,6136,6142-6143,6242,6256-6257,6265-6271,6276,6281-6283", -1, [4]int32{0, 0, 0, 0}, [4]float32{0, 0, 0, 0}}

	if reference != result {
		fmt.Println(result)
		t.Fail()
	}

	//  try incomplete entry
	mongo.collection.Find(bson.M{"_id": "815843-2019"}).One(&result)

	reference = Jobentry{"815843-2019", "815843-2019", "", 1470866890, -1, "", "", -1, [4]int32{0, 0, 0, 0}, [4]float32{0, 0, 0, 0}}

	if reference != result {
		fmt.Println(result)
		t.Fail()
	}

	// go noisy again
	log.SetOutput(defaultOut)

	// drop DB
	mongo.db.DropDatabase()

}

// TestEvent1 tests the event loop writing in background into todayfile()
func TestAlpsEvent1(t *testing.T) {

	QUIT = make(chan int)

	readConf("testinput/alps.config")

	logtype = ALPS

	mongo := NewMongo()

	// go silent
	defaultOut := os.Stderr
	null := new(devnull)
	log.SetOutput(null)

	fin, err := os.Open("testinput/Simple1Alps")
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

	// go noisy again
	log.SetOutput(defaultOut)

	// get db entry
	result := Jobentry{}
	mongo.collection.Find(bson.M{"_id": "815842-2019"}).One(&result)

	reference := Jobentry{"815842-2019", "815842-2019", "31251", 1470866890, 1470866959, "rule_the_world", "2133-2139,2141-2149,2200-2202,2215-2216,2287-2303,2436,2454-2480,2494,2496-2510,2562,2564,2566-2569,2574-2578,2590-2595,2597-2605,2665-2668,2670-2674,2680-2686,2692,3124-3132,3218-3245,3264-3266,4098-4099,4104-4107,4109-4110,4915-4917,4981,5371,5839,5843-5844,5905,5921,6136,6142-6143,6242,6256-6257,6265-6271,6276,6281-6283", -1, [4]int32{0, 0, 0, 0}, [4]float32{0, 0, 0, 0}}

	if reference != result {
		fmt.Println("1", result)
		fmt.Println("1", reference)
		t.Fail()
	}

	//  try incomplete entry
	mongo.collection.Find(bson.M{"_id": "815843-2019"}).One(&result)

	reference = Jobentry{"815843-2019", "815843-2019", "", 1470866890, -1, "", "", -1, [4]int32{0, 0, 0, 0}, [4]float32{0, 0, 0, 0}}

	if reference != result {
		fmt.Println("2", result)
		t.Fail()
	}

	// delete temp faile
	os.Remove(todayname())

	// drop DB
	mongo.db.DropDatabase()
}
