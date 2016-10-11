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
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/holgerBerger/go_ludalo/lustreserver"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
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

// types for mongo inserts
type ostData struct {
	ID  string     `bson:"_id,omitempty"`
	Ts  int        `bson:"ts"`
	Ost string     `bson:"ost"`
	Nid string     `bson:"nid"`
	V   [4]float32 `bson:"v"`
	Dt  int32      `bson:"dt"`
}

type mdsData struct {
	ID  string   `bson:"_id,omitempty"`
	Ts  int      `bson:"ts"`
	Mdt string   `bson:"mdt"`
	Nid string   `bson:"nid"`
	V   [4]int32 `bson:"v"`
	Dt  int32    `bson:"dt"`
}

// glocal config read in main
var (
	conf    configT
	hostmap hostfile
)

// global variables for timing
var (
	timingLock     sync.Mutex
	collectTimes   map[string]float32
	insertTimes    map[string]float32
	insertMDSItems map[string]int32
	insertOSSItems map[string]int32
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
	var killed bool

	// compare sha224 of local and remote collector, and skip installation if same,
	// install if anything goes wrong (like not existing)
	out, lerr := exec.Command("sha224sum", conf.Collector.LocalcollectorPath).CombinedOutput()
	localsha := strings.Fields(string(out))[0]
	out, rerr := exec.Command("ssh", c, "sha224sum", conf.Collector.CollectorPath).CombinedOutput()
	remotesha := strings.Fields(string(out))[0]

	if (localsha != remotesha) || (lerr != nil) || (rerr != nil) {
		// kill runnning processes in case there is one to avoid busy binary error for scp
		_, err := exec.Command("ssh", c, "killall", "-e", conf.Collector.CollectorPath).CombinedOutput()
		killed = true
		log.Println("installing collector on " + c + " in " + conf.Collector.CollectorPath)
		out, err = exec.Command("scp", conf.Collector.LocalcollectorPath, c+":"+conf.Collector.CollectorPath).CombinedOutput()
		if err != nil {
			log.Println("error: unexpected end on " + c)
			log.Println(string(out))
		}
		log.Println("installed collector on " + c)
	} else {
		log.Println("same hash, skipped collector installation on", c)
		killed = false
	}

	t1 := time.Now()
	for {
		// kill runnning processes in case there is one
		if !killed {
			exec.Command("ssh", c, "killall", "-e", conf.Collector.CollectorPath).CombinedOutput()
			killed = true
		}

		log.Println("starting collector on " + c)
		count++
		out, err := exec.Command("ssh", c, conf.Collector.CollectorPath, strconv.Itoa(conf.Collector.Port)).CombinedOutput()
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
		log.Print("connecting RPC to " + server + ":" + strconv.Itoa(conf.Collector.Port))
		client, err := rpc.Dial("tcp", server+":"+strconv.Itoa(conf.Collector.Port))
		if err != nil {
			log.Print("dialing:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Print("connected RPC to " + server + ":" + strconv.Itoa(conf.Collector.Port))

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
			replyOSS.Timestamp = int32(timestamp)
			t2 := time.Now()
			timingLock.Lock()
			collectTimes[server] = float32(t2.Sub(t1).Seconds())
			timingLock.Unlock()

			// copy data for RPC server
			OssData[server] = replyOSS

			// push data to mongo inserter
			inserter <- replyOSS

			t3 := time.Now()

			if int(t3.Sub(t1).Seconds()) > conf.Collector.Interval {
				log.Println("WARNING: for", server, "cycle is exceeding interval by",
					int(t3.Sub(t1).Seconds())-conf.Collector.Interval, "secs")
			}

			// log.Println(server, "collect cycle", t2.Sub(t1).Seconds(), "secs")
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
		log.Print("connecting RPC to " + server + ":" + strconv.Itoa(conf.Collector.Port))
		client, err := rpc.Dial("tcp", server+":"+strconv.Itoa(conf.Collector.Port))
		if err != nil {
			log.Print("dialing:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Print("connected RPC to " + server + ":" + strconv.Itoa(conf.Collector.Port))

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
			replyMDS.Timestamp = int32(timestamp)
			t2 := time.Now()
			timingLock.Lock()
			collectTimes[server] = float32(t2.Sub(t1).Seconds())
			timingLock.Unlock()

			inserter <- replyMDS

			t3 := time.Now()

			if int(t3.Sub(t1).Seconds()) > conf.Collector.Interval {
				log.Println("WARNING: for", server, "cycle is exceeding interval by",
					int(t3.Sub(t1).Seconds())-conf.Collector.Interval, "secs")
			}

			// log.Println(server, "collect cycle", t2.Sub(t1).Seconds(), "secs")
		}
	}
}

// insert OSS data into MongoDB
func ossInsert(server string, inserter chan lustreserver.OstValues, session *mgo.Session) {
	// mongo session
	db := session.DB(conf.Database.Name)
	// cache for collections
	collections := make(map[string]*mgo.Collection)

	var vals [4]float32
	var insertItems int32

	for {
		v := <-inserter
		// fmt.Println("received and pushing!")
		// fmt.Println(v)
		insertItems = 0
		t1 := time.Now()
		for ost := range v.OstTotal {
			// ost contains FS name in form FS-OST
			names := strings.Split(ost, "-")
			fsname := names[0]
			ostname := names[1]

			// we add year+month to the collection name to keep collections smaller
			now := time.Now()
			fsname = fmt.Sprintf("%s%2.2d%4.4d", fsname, now.Month(), now.Year())

			// we cache mongo collections here, not created each time
			_, ok := collections[fsname]
			if !ok {
				collections[fsname] = db.C(fsname)
				collections[fsname].EnsureIndexKey("ts", "nid")
			}
			collection := collections[fsname]

			// temp array to insert int array instead of struct
			vals[0] = float32(v.OstTotal[ost].WRqs)
			vals[1] = float32(v.OstTotal[ost].WBs)
			vals[2] = float32(v.OstTotal[ost].RRqs)
			vals[3] = float32(v.OstTotal[ost].RBs)

			// insert aggregate data for OST
			insertItems++
			/*
				err := collection.Insert(bson.D{{"ts", int(v.Timestamp)},
					{"ost", ostname},
					{"nid", "aggr"},
					{"v", vals},
					{"dt", v.Delta},
				})
			*/

			err := collection.Insert(ostData{"", int(v.Timestamp),
				ostname,
				"aggr",
				vals,
				v.Delta,
			})

			if err != nil {
				log.Println("WARNING: insert error in ossInsert for", server)
				log.Println(err)
				session.Refresh()
			}

			for nid := range v.NidValues[ost] {
				// temp array to insert int array instead of struct
				vals[0] = float32(v.NidValues[ost][nid].WRqs)
				vals[1] = float32(v.NidValues[ost][nid].WBs)
				vals[2] = float32(v.NidValues[ost][nid].RRqs)
				vals[3] = float32(v.NidValues[ost][nid].RBs)

				// NID name translation, splitting at @ + IP resolution in case of IP address
				nidname := strings.Split(nid, "@")[0]
				// if it is IP address, map with rules from config e.g. to remove -ib postfix
				if strings.ContainsAny(nidname, ".") {
					nidname = hostmap.mapip2name(nidname)
				}

				insertItems++
				/*
					err := collection.Insert(bson.D{{"ts", int(v.Timestamp)},
						{"ost", ostname},
						{"nid", nidname},
						{"v", vals},
						{"dt", v.Delta},
					})
				*/

				err := collection.Insert(ostData{"", int(v.Timestamp),
					ostname,
					nidname,
					vals,
					v.Delta,
				})

				if err != nil {
					log.Println("WARNING: insert error in ossInsert for", server)
					log.Println(err)
					session.Refresh()
				}
			}
		}

		// write latest timestamp into DB as marker for end of transaction, so client won't read incomplete data
		_, ok := collections["latesttimestamp"]
		if !ok {
			collections["latesttimestamp"] = db.C("latesttimestamp")
		}
		_, err := collections["latesttimestamp"].Upsert(bson.M{"latestts": bson.M{"$exists": true}},
			bson.M{"$set": bson.M{"latestts": int(v.Timestamp)}})
		if err != nil {
			log.Println("WARNING: error in update of last timestamp")
			log.Println(err)
		}

		t2 := time.Now()

		timingLock.Lock()
		insertTimes[server] = float32(t2.Sub(t1).Seconds())
		insertOSSItems[server] = insertItems
		timingLock.Unlock()
		// log.Println(server, "mongo insert", t2.Sub(t1).Seconds(), "secs")
	}
}

// insert MDS data into MongoDB
func mdsInsert(server string, inserter chan lustreserver.MdsValues, session *mgo.Session) {
	// mongo session
	db := session.DB(conf.Database.Name)
	// cache for collections
	collections := make(map[string]*mgo.Collection)

	var vals [4]int32
	var insertItems int32

	for {
		v := <-inserter
		// fmt.Println("received and pushing!")
		// fmt.Println(v)
		insertItems = 0
		t1 := time.Now()
		for mdt := range v.MdsTotal {
			// mdt contains FS name in form FS-OST
			names := strings.Split(mdt, "-")
			fsname := names[0]
			mdtname := names[1]

			// we add year+month to the collection name to keep collections smaller
			now := time.Now()
			fsname = fmt.Sprintf("%s%2.2d%4.4d", fsname, now.Month(), now.Year())

			// we cache mongo collections here, not created each time
			_, ok := collections[fsname]
			if !ok {
				collections[fsname] = db.C(fsname)
				collections[fsname].EnsureIndexKey("ts", "nid")
			}
			collection := collections[fsname]

			// temp array to insert int array instead of struct
			vals[0] = int32(v.MdsTotal[mdt].Opens)
			vals[1] = int32(v.MdsTotal[mdt].Creates)
			vals[2] = int32(v.MdsTotal[mdt].Queries)
			vals[3] = int32(v.MdsTotal[mdt].Updates)

			// insert aggregate data for OST
			insertItems++
			/*
				err := collection.Insert(bson.D{{"ts", int(v.Timestamp)},
					{"mdt", mdtname},
					{"nid", "aggr"},
					{"v", vals},
					{"dt", v.Delta},
				})
			*/

			err := collection.Insert(mdsData{"", int(v.Timestamp),
				mdtname,
				"aggr",
				vals,
				v.Delta,
			})

			if err != nil {
				log.Println("WARNING: insert error in mdsInsert for", server)
				log.Println(err)
				session.Refresh()
			}
			for nid := range v.NidValues[mdt] {
				// temp array to insert int array instead of struct
				vals[0] = int32(v.NidValues[mdt][nid].Opens)
				vals[1] = int32(v.NidValues[mdt][nid].Creates)
				vals[2] = int32(v.NidValues[mdt][nid].Queries)
				vals[3] = int32(v.NidValues[mdt][nid].Updates)

				// NID name translation, splitting at @ + IP resolution in case of IP address
				nidname := strings.Split(nid, "@")[0]
				// if it is IP address, map with rules from config e.g. to remove -ib postfix
				if strings.ContainsAny(nidname, ".") {
					nidname = hostmap.mapip2name(nidname)
				}

				insertItems++

				/*
					err := collection.Insert(bson.D{{"ts", int(v.Timestamp)},
						{"mdt", mdtname},
						{"nid", nidname},
						{"v", vals},
						{"dt", v.Delta},
					})
				*/

				err := collection.Insert(mdsData{"", int(v.Timestamp),
					mdtname,
					nidname,
					vals,
					v.Delta,
				})

				if err != nil {
					log.Println("WARNING: insert error in mdsInsert for", server)
					log.Println(err)
					session.Refresh()
				}
			}
		}
		t2 := time.Now()

		timingLock.Lock()
		insertTimes[server] = float32(t2.Sub(t1).Seconds())
		insertMDSItems[server] = insertItems
		timingLock.Unlock()
		// log.Println(server, "mongo insert", t2.Sub(t1).Seconds(), "secs")
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
		hostlist = hostlist + " " + h
	}
	log.Print("hosts to start OSS collector on: " + hostlist)

	hostlist = ""
	for _, h := range mdsCollectors {
		hostlist = hostlist + " " + h
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
	// FIXME might need tuning? is 5 sec enough?
	time.Sleep(5 * time.Second)

	// create inserters to push data into mongodb
	// using Copy of session (may be Clone is better? would reuse socket)
	// changed to Clone()
	for i, c := range ossCollectors {
		go ossInsert(c, ossInserters[i], session.Clone())
	}
	for i, c := range mdsCollectors {
		go mdsInsert(c, mdsInserters[i], session.Clone())
	}

	// create collect goroutines to collect data and push it down the channels
	// towards inserters
	for i, c := range ossCollectors {
		go ossCollect(c, ready[c], ossInserters[i])
	}
	for i, c := range mdsCollectors {
		go mdsCollect(c, ready[c], mdsInserters[i])
	}

	/////////////////////////////////////////////////////////////////////////
	// MAIN LOOP, sending signals to collectors
	// we check if collector was signalling readiness,
	// if yes, we send time signal to gather information
	// and push it into buffered channel to inserter,
	// otherwise, we skip it in this cycle.
	// this should never block, even if inserter channel is full.
	/////////////////////////////////////////////////////////////////////////
	collectTimes = make(map[string]float32)
	insertTimes = make(map[string]float32)
	insertMDSItems = make(map[string]int32)
	insertOSSItems = make(map[string]int32)
	for {
		for _, m := range mdsCollectors {
			select {
			case <-ready[m]:
				ready[m] <- 1
			default:
				// skip this one, it is busy, channel is probably filled or RPC is stuck
			}
		}
		// oss last, as oss writes the timestamps into DB
		for _, o := range ossCollectors {
			select {
			case <-ready[o]:
				ready[o] <- 1
			default:
				// skip this one, it is busy, channel is probably filled or RPC is stuck
			}
		}
		time.Sleep(time.Duration(conf.Collector.Interval) * time.Second)
		Timing()
		Statistics()
	}
}

// print Statistics of Mongo inserts, does not reflect server activity but #client activity
func Statistics() {
	log.Print("Cycle statistics:")
	var (
		mdstotal int32
		osstotal int32
	)

	mdstotal = 0
	osstotal = 0

	for _, v := range insertMDSItems {
		mdstotal += v
	}
	log.Println(" active mds nids:", mdstotal)

	for _, v := range insertOSSItems {
		osstotal += v
	}
	log.Println(" active oss nids:", osstotal)
}

// print times of go routines doing collection and insertion
func Timing() {
	// timing
	var (
		max, avg float32
		maxs     string
		count    int
	)

	log.Print("Cycle timings:")
	max = 0.0
	avg = 0.0
	count = 0
	for s, v := range collectTimes {
		if v > max {
			maxs = s
			max = v
		}
		avg += v
		if v != 0.0 {
			count++
		}
	}
	log.Printf(" collect : max %5.3f(%s) avg %5.3f secs", max, maxs, avg/float32(count))

	max = 0.0
	avg = 0.0
	count = 0
	for s, v := range insertTimes {
		if v > max {
			maxs = s
			max = v
		}
		avg += v
		if v != 0.0 {
			count++
		}
	}
	log.Printf(" insert  : max %5.3f(%s) avg %5.3f secs", max, maxs, avg/float32(count))
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
	// make session a Safe Session with error checking FIXME good idea???
	session.SetSafe(&mgo.Safe{})

	// RPC server
	OssData = make(map[string]lustreserver.OstValues)
	go startServer()

	// do work
	aggrRun(session)

}
