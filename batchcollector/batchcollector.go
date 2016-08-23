package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"golang.org/x/exp/inotify"
)

var (
	totaljobs int64
	res2job   *Res2job
)

// BuildID contains Build Date
var BuildID string

// Hash contains git hash of commit
var Hash string

// logfile types
const (
	ALPS   = iota
	TORQUE = iota
)

// channel for ending endless eventloop (needed for testing)
var QUIT chan int

func todayname() string {
	// format: "Mon Jan 2 15:04:05 -0700 MST 2006"
	return config.WatchDirectory + "/" + config.FilePrefix + time.Now().Format("20060102")
}

// eventloop prepares the inotify events and waits for them
func eventloop(mongo MongoInserter) {

	var currentlog LogFileReader

	// open current file
	if logtype == ALPS {
		currentlog = newAlpsLogfile(todayname(), mongo)
	} else {
		currentlog = newTorqueLogfile(todayname(), mongo)
	}
	watcher, err := inotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}

	err = watcher.Watch(config.WatchDirectory)
	if err != nil {
		log.Fatal(err)
	}

	// endless event loop, can be ended by QUIT channel
	for {

		select {

		case ev := <-watcher.Event:
			if (ev.Mask & inotify.IN_CREATE) > 0 {
				// if a file is created, check if it is same file or not, and if it could be
				// todays file
				//if ev.Name != currentlog.name && ev.Name == "testdata/apsched-c2-0c0s0n1-20160820" {
				if ev.Name != currentlog.getName() && ev.Name == todayname() {
					currentlog.readToEnd() // read rest of file in case we missed something
					if logtype == ALPS {
						currentlog = newAlpsLogfile(ev.Name, mongo)
					} else {
						currentlog = newTorqueLogfile(ev.Name, mongo)
					}
				}
			} else if (ev.Mask & inotify.IN_MODIFY) > 0 {
				// if a file is updated, read file to end if it is current file
				if ev.Name == currentlog.getName() {
					currentlog.readToEnd()
				}
			} else {
				// log.Println("inotify event:", ev)
			}

		case err := <-watcher.Error:
			log.Println("inotify error:", err)

		case <-QUIT: // end for testing
			return
		}

	}

}

// dummy writer implements writer interface
type devnull int

var logtype int

func (d *devnull) Write(p []byte) (int, error) {
	return len(p), nil
}

// main programm
func main() {

	/*
		// profiling code
		f, err := os.Create("profile")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	*/

	fmt.Println("batchcollector_alps - build:", BuildID, Hash)

	// runtime switch which logfiles we read
	if strings.Index(os.Args[0], "batchcollector_alps") > 0 {
		logtype = ALPS
		fmt.Println("alps mode")
	} else if strings.Index(os.Args[0], "batchcollector_torque") > 0 {
		logtype = TORQUE
		fmt.Println("torque mode")
	} else {
		panic("unknown logfile type, executable should be called either batchcollector_alps or batchcollector_torque!")
	}

	// init some stuff
	if logtype == ALPS {
		readConf("batchcollector_alps.conf")
	} else {
		readConf("batchcollector_torque.conf")
	}

	totaljobs = 0

	// mongo setup
	// async is not faster with merging opt, can be avoided
	mongo := NewAsynchMongo()
	// mongo := NewMongo()

	// switch of output for the files on command line
	defaultOut := os.Stderr
	null := new(devnull)
	log.SetOutput(null)

	readsomething := false

	// read files from commandline
	for _, file := range os.Args[1:] {
		if file != todayname() {
			fmt.Println("silently processing", file)
			if logtype == ALPS {
				_ = newAlpsLogfile(file, mongo)
			} else {
				_ = newTorqueLogfile(file, mongo)
			}
			readsomething = true
		} else {
			fmt.Println("skipping", file)
		}
	}

	// stop worker
	mongo.Shutdown()

	// switch back to normal
	log.SetOutput(defaultOut)
	if readsomething {
		log.Println("read", totaljobs, "jobs from files on command line, now waiting...")
	}

	// wait
	eventloop(mongo)
}
