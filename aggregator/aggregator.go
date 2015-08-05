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
	"bufio"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/holgerBerger/go_ludalo/lustreserver"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// config file definition
type configT struct {
	Collector  collectorConfig
	Database   databaseConfig
	Nidmapping nidmappingConfig
}

type collectorConfig struct {
	OSS                []string
	MDS                []string
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

type nidmappingConfig struct {
	Hostfile string
	Pattern  string
	Replace  string
}

// end config file definition

// hostfile cache
type hostfile struct {
	ip2name map[string]string
	re      *regexp.Regexp
}

// glocal config read in main
var (
	conf    configT
	hostmap hostfile
)

// readFile read hostfile as specified in config
func (m *hostfile) readFile(filename string) {
	m.ip2name = make(map[string]string)
	// compile regexp once
	re, err := regexp.Compile(conf.Nidmapping.Pattern)
	if err != nil {
		log.Print("could not compile regexp pattern for nid mapping from config!")
		log.Panic(err)
	}
	m.re = re

	// read hostfile as specified in config, ignoe empty lines and comments
	f, err := os.Open(filename)
	defer f.Close()
	if err == nil {
		r := bufio.NewReader(f)
		line, isPrefix, err := r.ReadLine()
		for err == nil && !isPrefix {
			s := string(line)
			fields := strings.Fields(s)
			if len(fields) > 0 {
				if !strings.HasPrefix(fields[0], "#") {
					m.ip2name[fields[0]] = fields[1]
					// log.Println("hostmap", fields[0], fields[1])
				}
			}
			line, isPrefix, err = r.ReadLine()
		}
	}
}

// ip2name maps a IP address to a name with some manipulation read from config
func (m *hostfile) mapip2name(ip string) string {
	name, ok := m.ip2name[ip]
	if ok {
		return m.re.ReplaceAllString(name, conf.Nidmapping.Replace)
	} else {
		return ip
	}
}

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

// collect OSS data from collectors, and push them into channel
// towards database inserter. The channel is buffered,
// to limit amount of RAM used
// we take time here, as this avoid problems with non-synchronous clocks
// on servers and allows snapping to a certain intervals
func ossCollect(server string, signal chan int, inserter chan lustreserver.OstValues) {
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
		err = client.Call("OssRpcT.GetValuesDiff", true, &replyOSS)
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
			err := client.Call("OssRpcT.GetValuesDiff", false, &replyOSS)
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

// collect MDS data from collectors, and push them into channel
// towards database inserter. The channel is buffered,
// to limit amount of RAM used
// we take time here, as this avoid problems with non-synchronous clocks
// on servers and allows snapping to a certain intervals
func mdsCollect(server string, signal chan int, inserter chan lustreserver.MdsValues) {
	var replyMDS lustreserver.MdsValues

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
		err = client.Call("MdsRpcT.GetValuesDiff", true, &replyMDS)
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
			err := client.Call("MdsRpcT.GetValuesDiff", false, &replyMDS)
			if err != nil {
				log.Print("rpc problems for server " + server)
				log.Print("rpcerror:", err)
				log.Print("trying to reconnect...")
				time.Sleep(1 * time.Second)
				break
			}
			replyMDS.Timestamp = timestamp
			inserter <- replyMDS
			t2 := time.Now()

			log.Println(server, "collect cycle", t2.Sub(t1).Seconds(), "secs")
		}
	}
}

// insert OSS data into MongoDB
func ossInsert(server string, inserter chan lustreserver.OstValues, session *mgo.Session) {
	// mongo session
	db := session.DB(conf.Database.Name)
	// cache for collections
	collections := make(map[string]*mgo.Collection)

	var vals [4]int

	_ = fmt.Println

	for {
		v := <-inserter
		// fmt.Println("received and pushing!")
		// fmt.Println(v)
		t1 := time.Now()
		for ost := range v.OstTotal {
			// ost contains FS name in form FS-OST
			names := strings.Split(ost, "-")
			fsname := names[0]
			ostname := names[1]
			// we cache mongo collections here, not created each time
			_, ok := collections[fsname]
			if !ok {
				collections[fsname] = db.C(fsname)
			}
			collection := collections[fsname]

			// temp array to insert int array instead of struct
			vals[0] = int(v.OstTotal[ost].WRqs)
			vals[1] = int(v.OstTotal[ost].WBs)
			vals[2] = int(v.OstTotal[ost].RRqs)
			vals[3] = int(v.OstTotal[ost].RBs)

			// insert aggregate data for OST
			collection.Insert(bson.M{"ts": int(v.Timestamp),
				"ost": ostname,
				"nid": "aggr",
				"v":   vals,
			})
			for nid := range v.NidValues[ost] {
				// temp array to insert int array instead of struct
				vals[0] = int(v.NidValues[ost][nid].WRqs)
				vals[1] = int(v.NidValues[ost][nid].WBs)
				vals[2] = int(v.NidValues[ost][nid].RRqs)
				vals[3] = int(v.NidValues[ost][nid].RBs)

				// NID name translation, splitting at @ + IP resolution in case of IP address
				nidname := strings.Split(nid, "@")[0]
				// if it is IP address, map with rules from config e.g. to remove -ib postfix
				if strings.ContainsAny(nidname, ".") {
					nidname = hostmap.mapip2name(nidname)
				}

				collection.Insert(bson.M{"ts": int(v.Timestamp),
					"ost": ostname,
					"nid": nidname,
					"v":   vals,
				})
			}
		}
		t2 := time.Now()
		log.Println(server, "mongo insert", t2.Sub(t1).Seconds(), "secs")
	}
}

// insert MDS data into MongoDB
func mdsInsert(server string, inserter chan lustreserver.MdsValues, session *mgo.Session) {
	// mongo session
	db := session.DB(conf.Database.Name)
	// cache for collections
	collections := make(map[string]*mgo.Collection)

	var vals int

	_ = fmt.Println

	for {
		v := <-inserter
		// fmt.Println("received and pushing!")
		// fmt.Println(v)
		t1 := time.Now()
		for mdt := range v.MdsTotal {
			// mdt contains FS name in form FS-OST
			names := strings.Split(mdt, "-")
			fsname := names[0]
			mdtname := names[1]
			// we cache mongo collections here, not created each time
			_, ok := collections[fsname]
			if !ok {
				collections[fsname] = db.C(fsname)
			}
			collection := collections[fsname]

			// temp array to insert int array instead of struct
			vals = int(v.MdsTotal[mdt])

			// insert aggregate data for OST
			collection.Insert(bson.M{"ts": int(v.Timestamp),
				"mdt": mdtname,
				"nid": "aggr",
				"v":   vals,
			})
			for nid := range v.NidValues[mdt] {
				// temp array to insert int array instead of struct
				vals = int(v.NidValues[mdt][nid])

				// NID name translation, splitting at @ + IP resolution in case of IP address
				nidname := strings.Split(nid, "@")[0]
				// if it is IP address, map with rules from config e.g. to remove -ib postfix
				if strings.ContainsAny(nidname, ".") {
					nidname = hostmap.mapip2name(nidname)
				}

				collection.Insert(bson.M{"ts": int(v.Timestamp),
					"mdt": mdtname,
					"nid": nidname,
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
//  - do this for mds and oss
func aggrRun(session *mgo.Session) {

	ossCollectors := conf.Collector.OSS
	mdsCollectors := conf.Collector.MDS

	// show list of hosts
	var hostlist string
	for _, h := range ossCollectors {
		hostlist = hostlist + h
	}
	log.Print("hosts to start OSS collector on: " + hostlist)

	hostlist = ""
	for _, h := range mdsCollectors {
		hostlist = hostlist + h
	}
	log.Print("hosts to start MDS collector on: " + hostlist)

	// create channels between collector and inserter go routines
	ossInserters := make([]chan lustreserver.OstValues, len(ossCollectors))
	for i := range ossInserters {
		ossInserters[i] = make(chan lustreserver.OstValues, conf.Collector.MaxEntries)
	}
	mdsInserters := make([]chan lustreserver.MdsValues, len(mdsCollectors))
	for i := range mdsInserters {
		mdsInserters[i] = make(chan lustreserver.MdsValues, conf.Collector.MaxEntries)
	}

	// create channels to signal to collectors
	// a blocking channel is used
	//   collectors sends if ready and blocks in receive
	//   central timing loop selects to see if ready, and sends to those beeing ready
	ready := make(map[string]chan int, len(ossCollectors)+len(mdsCollectors))
	for _, i := range ossCollectors {
		ready[i] = make(chan int)
	}
	for _, i := range mdsCollectors {
		ready[i] = make(chan int)
	}

	// create collector processes on the servers, do not wait for them, they are endless
	// this will not return, they will be restarted if the fail/end
	for _, c := range ossCollectors {
		go spawnCollector(c)
	}
	for _, c := range mdsCollectors {
		go spawnCollector(c)
	}

	// wait a second to allow collectors to start
	// FIXME might need tuning? is 1 sec enough?
	time.Sleep(5 * time.Second)

	// create inserters to push data into mongodb
	// using Copy of session (may be Clone is better? would reuse socket)
	for i, c := range ossCollectors {
		go ossInsert(c, ossInserters[i], session.Copy())
	}
	for i, c := range mdsCollectors {
		go mdsInsert(c, mdsInserters[i], session.Copy())
	}

	// create collect goroutines to collect data and push it down the channels
	// towards inserters
	for i, c := range ossCollectors {
		go ossCollect(c, ready[c], ossInserters[i])
	}
	for i, c := range mdsCollectors {
		go mdsCollect(c, ready[c], mdsInserters[i])
	}

	// main loop, sending signals to collectors
	// we check if collector was signalling readiness,
	// if yes, we send time signal to gather information
	// and push it into buffered channel to inserter,
	// otherwise, we skip it in this cycle.
	// this should never block, even if inserter channel is full.
	for {
		for _, o := range ossCollectors {
			select {
			case <-ready[o]:
				ready[o] <- 1
			default:
				// skip this one, it is busy, channel is probably filled or RPC is stuck
			}
		}
		for _, m := range mdsCollectors {
			select {
			case <-ready[m]:
				ready[m] <- 1
			default:
				// skip this one, it is busy, channel is probably filled or RPC is stuck
			}
		}
		time.Sleep(time.Duration(conf.Collector.Interval) * time.Second)
	}
}

//////////////////////////////////////////////////////
func main() {

	log.Print("starting ludalo aggregator")
	if _, err := toml.DecodeFile("ludalo.config", &conf); err != nil {
		// handle error
		log.Print("error in reading ludalo.config:")
		log.Fatal(err)
	} else {
		log.Print("config <ludalo.config> read succesfully")
	}

	// hostmapping
	hostmap.readFile(conf.Nidmapping.Hostfile)

	// prepare mongo connection
	session, err := mgo.Dial(conf.Database.Server)
	if err != nil {
		log.Print("could not connected to mongo server " + conf.Database.Server)
		log.Fatal(err)
	} else {
		log.Print("connected to mongo server " + conf.Database.Server)
	}

	// do work
	aggrRun(session)

}
