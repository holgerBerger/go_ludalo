// aggregator

// fetches data from collectors and inserts into MongoDB

/*
 *  architecture:
 *
 *  collectors get spawnd after reading config file
 *  a central clock signals go routines to fetach data from collectors
 *  and insert data into a channel to another go routine inserting data into
 *  MongoDB, this is a buffered channel, so the data collecting go routine will
 *  block in case the inserter can not insert fast enough/for a period, so we
 *  will lack data from the collectors.
 *  Therefor time of last collection/current collection has to be taken for
 *  computing the rates, as the interval might be bigger than expected
 */

package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/holgerBerger/go_ludalo/lustreserver"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net/rpc"
	"os/exec"
	"strconv"
	"time"
)

// config file definition
type configT struct {
	Collector collectorConfig
	Database  databaseConfig
}

type collectorConfig struct {
	Hosts              []string
	LocalcollectorPath string
	CollectorPath      string
	MaxEntries         int
	Port               int
	Interval           int
	SnapInterval       int
}

type databaseConfig struct {
	Server string
	Name   string
}

// end config file definition

// glocal config read in main
var conf configT

// spawnCollectors starts collectors, retrying 10 times/max 20 seconds
// waiting 1 second between retries
func spawnCollector(c string) {
	var count int
	count = 0
	t1 := time.Now()

	log.Println("installing collector on " + c + " in " + conf.Collector.CollectorPath)
	out, err := exec.Command("scp", conf.Collector.LocalcollectorPath, c+":"+conf.Collector.CollectorPath).CombinedOutput()
	if err != nil {
		log.Println("error: unexpected end on " + c)
		log.Println(string(out))
	}
	log.Println("installed collector on " + c)

	for {
		log.Println("starting collector on " + c)
		count++
		out, err := exec.Command("ssh", c, conf.Collector.CollectorPath).CombinedOutput()
		if err != nil {
			log.Println("error: unexpected end on " + c)
			log.Println(string(out))
		}
		t2 := time.Now()
		// if we restart 10 times within 20 seconds, something is strange,
		// we bail out that one, may be the config is bad, and it will
		// never work, so do not waste cycles
		if count >= 10 && t2.Sub(t1).Seconds() < 20 {
			log.Println("error: could not start for 10 times, giving up on host " + c)
			break
		}
		time.Sleep(1 * time.Second)
	}
}

// collect data from collectors, and push them into channel
// towards database inserter. The channel is buffered,
// to limit amount of RAM used
// we take time here, as this avoid problems with non-synchronous clocks
// on servers and allows snapping to a certain intervals
// FIXME: MDS Version needed
func collect(server string, signal chan int, inserter chan lustreserver.OstValues) {
	var replyOSS lustreserver.OstValues

	for {
		// setup RPC
		log.Print("connecting to " + server + ":" + strconv.Itoa(conf.Collector.Port))
		client, err := rpc.Dial("tcp", server+":"+strconv.Itoa(conf.Collector.Port))
		if err != nil {
			log.Print("dialing:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Print("connected to " + server + ":" + strconv.Itoa(conf.Collector.Port))

		// init call for differences
		// FIXME replace random with real thing
		err = client.Call("OssRpcT.GetRandomValues", true, &replyOSS)
		if err != nil {
			log.Print("rpcerror:", err)
			time.Sleep(1 * time.Second) // wait a sec
			continue
		}

		// loop endless as long as RPC works, otherwise exit and reconnect
		for {
			signal <- 1 // signal we are ready
			<-signal    // wait for signal
			t1 := time.Now()
			// get timestamp and snap it to a configured interval, which allows more efficient
			// DB access later
			now := t1.Unix()
			timestamp := (now / int64(conf.Collector.SnapInterval)) * int64(conf.Collector.SnapInterval)
			// FIXME replace random with real thing
			err := client.Call("OssRpcT.GetRandomValues", false, &replyOSS)
			if err != nil {
				log.Print("rpc problems for server " + server)
				log.Print("rpcerror:", err)
				log.Print("trying to reconnect...")
				time.Sleep(1 * time.Second)
				break
			}
			replyOSS.Timestamp = timestamp
			inserter <- replyOSS
			t2 := time.Now()

			log.Println(server, "collect cycle", t2.Sub(t1).Seconds(), "secs")
		}
	}
}

// insert data into MongoDB
// FIXME: MDS Version needed
func insert(server string, inserter chan lustreserver.OstValues, session *mgo.Session) {
	// mongo session
	db := session.DB(conf.Database.Name)
	collection := db.C("performance")

	var vals [4]int

	for {
		v := <-inserter
		// fmt.Println("received and pushing!")
		// fmt.Println(v)
		_ = fmt.Println
		t1 := time.Now()
		for ost := range v.OstTotal {
			// temp array to insert int array instead of struct
			vals[0] = int(v.OstTotal[ost].WRqs)
			vals[1] = int(v.OstTotal[ost].WBs)
			vals[2] = int(v.OstTotal[ost].RRqs)
			vals[3] = int(v.OstTotal[ost].RBs)

			collection.Insert(bson.M{"t": "ostt",
				"ost": ost,
				"v":   vals,
			})
			for nid := range v.NidValues[ost] {
				// temp array to insert int array instead of struct
				vals[0] = int(v.NidValues[ost][nid].WRqs)
				vals[1] = int(v.NidValues[ost][nid].WBs)
				vals[2] = int(v.NidValues[ost][nid].RRqs)
				vals[3] = int(v.NidValues[ost][nid].RBs)

				collection.Insert(bson.M{"ts": int(v.Timestamp),
					"t":   "ostn",
					"ost": ost,
					"nid": nid,
					"v":   vals,
				})
			}
		}
		t2 := time.Now()
		log.Println(server, "mongo insert", t2.Sub(t1).Seconds(), "secs")
	}
}

// starts go routines to
//  - spawn the collectors
//  - run the central clock
//  - spawn the inserters for the mongo db
func aggrRun(session *mgo.Session) {

	collectors := conf.Collector.Hosts

	// show list of hosts
	var hostlist string
	for _, h := range collectors {
		hostlist = hostlist + h
	}
	log.Print("hosts to start collector on: " + hostlist)

	// create channels between collector and inserter go routines
	inserters := make([]chan lustreserver.OstValues, len(collectors))
	for i := range inserters {
		inserters[i] = make(chan lustreserver.OstValues, conf.Collector.MaxEntries)
	}

	// create channels to signal to collectors
	// a blocking channel is used
	//   collectors sends if ready and blocks in receive
	//   central timing loop selects to see if ready, and sends to those beeing ready
	ready := make([]chan int, len(collectors))
	for i := range ready {
		ready[i] = make(chan int)
	}

	// create collector processes on the servers, do not wait for them, they are endless
	// this will not return, they will be restarted if the fail/end
	for _, c := range collectors {
		go spawnCollector(c)
	}

	// wait a second to allow collectors to start
	// FIXME might need tuning? is 1 sec enough?
	time.Sleep(1 * time.Second)

	// create inserters to push data into mongodb
	// using Copy of session (may be Clone is better? would reuse socket)
	for i, c := range collectors {
		go insert(c, inserters[i], session.Copy())
	}

	// create collect goroutines to collect data and push it down the channels
	// towards inserters
	for i, c := range collectors {
		go collect(c, ready[i], inserters[i])
	}

	// main loop, sending signals to collectors
	// we check if collector was signalling readiness,
	// if yes, we send time signal to gather information
	// and push it into buffered channel to inserter,
	// otherwise, we skip it in this cycle.
	// this should never block, even if inserter channel is full.
	for {
		for i := range collectors {
			select {
			case <-ready[i]:
				ready[i] <- 1
			default:
				// skip this one, it is busy, channel is probably filled or RPC is stuck
			}
		}
		time.Sleep(time.Duration(conf.Collector.Interval) * time.Second)
	}
}

func main() {

	log.Print("starting ludalo aggregator")
	if _, err := toml.DecodeFile("ludalo.config", &conf); err != nil {
		// handle error
		log.Print("error in reading ludalo.config:")
		log.Fatal(err)
	} else {
		log.Print("config <ludalo.config> read succesfully")
	}

	// prepare mongo connection
	session, err := mgo.Dial(conf.Database.Server)
	if err != nil {
		log.Print("could not connected to mongo server " + conf.Database.Server)
		log.Fatal(err)
	} else {
		log.Print("connected to mongo server " + conf.Database.Server)
	}

	// do work
	// FIXME OSS only so far
	aggrRun(session)

	/*
	       t1 := time.Now()
	   	err = client.Call("OssRpc.GetRandomValues", false, &replyOSS)
	       t2 := time.Now()
	   	if err != nil {
	   		log.Fatal("rpcerror:", err)
	   	}
	   	fmt.Printf("\n%v\n", replyOSS)
	       fmt.Printf("%f secs\n",t2.Sub(t1).Seconds())
	*/

	// MDS example

	/*  var replyMDS lustreserver.MdsValues
		err = client.Call("MdsRpc.GetValues", true, &replyMDS)
		if err != nil {
			log.Fatal("rpcerror:", err)
		}

	    t1 := time.Now()
		err = client.Call("MdsRpc.GetValues", false, &replyMDS)
	    t2 := time.Now()
		if err != nil {
			log.Fatal("rpcerror:", err)
		}
		fmt.Printf("\n%v\n", replyMDS)
	    fmt.Printf("%f secs\n",t2.Sub(t1).Seconds())
	*/
}
