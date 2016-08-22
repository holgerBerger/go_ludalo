package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"golang.org/x/exp/inotify"
)

var res2job *Res2job
var totaljobs int64

var Id string
var Hash string

func todayname() string {
	// format: "Mon Jan 2 15:04:05 -0700 MST 2006"
	return config.WatchDirectory + "/" + config.FilePrefix + time.Now().Format("20060102")
}

// eventloop prepares the inotify events and waits for them
func eventloop(mongo MongoInserter) {

	// open current file
	currentlog := newLogfile(todayname(), mongo)

	watcher, err := inotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}

	err = watcher.Watch(config.WatchDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case ev := <-watcher.Event:
			if (ev.Mask & inotify.IN_CREATE) > 0 {
				// if a file is created, check if it is same file or not, and if it could be
				// todays file
				//if ev.Name != currentlog.name && ev.Name == "testdata/apsched-c2-0c0s0n1-20160820" {
				if ev.Name != currentlog.name && ev.Name == todayname() {
					currentlog.readToEnd() // read rest of file in case we missed something
					currentlog = newLogfile(ev.Name, mongo)
				}
			} else if (ev.Mask & inotify.IN_MODIFY) > 0 {
				// if a file is updated, read file to end if it is current file
				if ev.Name == currentlog.name {
					currentlog.readToEnd()
				}
			} else {
				log.Println("inotify event:", ev)
			}
		case err := <-watcher.Error:
			log.Println("inotify error:", err)
		}
	}
}

// dummy writer implements writer interface
type devnull int

func (d *devnull) Write(p []byte) (int, error) {
	return len(p), nil
}

// main programm
func main() {

	fmt.Println("batchcollector_alps - build:", Id, Hash)

	/*
		// profiling code
		f, err := os.Create("profile")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	*/

	// init some stuff
	readConf()
	res2job = NewRes2job("res2job.db")
	totaljobs = 0

	// mongo setup
	mongo := NewAsynchMongo()

	// switch of output for the files on command line
	defaultOut := os.Stderr
	null := new(devnull)
	log.SetOutput(null)

	// read files from commandline
	for _, file := range os.Args[1:] {
		if file != todayname() {
			fmt.Println("silently processing", file)
			_ = newLogfile(file, mongo)
		} else {
			fmt.Println("skipping", file)
		}
	}

	// stop worker
	mongo.Shutdown()

	// switch back to normal
	log.SetOutput(defaultOut)
	log.Println("read", totaljobs, "jobs from files on command line, now waiting...")

	// wait
	eventloop(mongo)
}
