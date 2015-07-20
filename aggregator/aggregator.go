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
 *  computing the rates, as interval might be bigger than expected
 */

package main

import (
	"fmt"
	"log"
	"github.com/holgerBerger/go_ludalo/lustreserver"
	"net/rpc"
    "time"
    "os/exec"
    "github.com/BurntSushi/toml"
    "strconv"
)


// config file definition
type configT struct {
    Name string
    Collector collectorConfig
    Database databaseConfig
}

type collectorConfig struct {
    Hosts []string
    CollectorPath string
    MaxEntries int
    Port int
    Interval int
}

type databaseConfig struct {
    Server string
    Name string
}
// end config file definition

// glocal config read in main
var conf configT

// spawn_collectors starts collectors, retrying 10 times/max 20 seconds
// waiting 1 second between retries
func spawn_collector(c string) {
    var count int;
    count = 0
    t1 := time.Now()
 
    for {
		log.Println("starting collector on "+c)
        count++
        out, err := exec.Command("ssh", c, conf.Collector.CollectorPath).CombinedOutput()
        if err != nil {
            log.Println("error: unexpected end on "+c)
            log.Println(string(out))
        }
        t2 := time.Now()
        // if we restart 10 times within 20 seconds, something is strange,
        // we bail out that one, may be the config is bad, and it will
        // never work, so do not waste cycles
        if count>=10 && t2.Sub(t1).Seconds() < 20 {
            log.Println("error: could not start for 10 times, giving up on host "+c)
            break
        }
        time.Sleep(1 * time.Second)
    }
}


// collect data from collectors, and push them into channel
// towards database inserter. The channel is buffered,
// to limit amount of RAM used
// FIXME: MDS Version needed
func collect(server string, signal chan int, inserter chan lustreserver.OstValues) {
    var replyOSS lustreserver.OstValues 
    
    for {
		// setup RPC
		log.Print("connecting to " + server+":"+strconv.Itoa(conf.Collector.Port))
		client, err := rpc.Dial("tcp", server+":"+strconv.Itoa(conf.Collector.Port))
		if err != nil {
			log.Print("dialing:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Print("connected to " + server+":"+strconv.Itoa(conf.Collector.Port))

		// init call for differences
		
		// FIXME replace random with real thing
		err = client.Call("OssRpc.GetRandomValues", true, &replyOSS)
		if err != nil {
			log.Print("rpcerror:", err)
			time.Sleep(1 * time.Second)  // wait a sec 
			continue
		} 
		
		// loop endless as long as RPC works, otherwise exit and reconnect
		for {
			<-signal
			err := client.Call("OssRpc.GetRandomValues", false, &replyOSS)
			if err != nil {
				log.Print("rpc problems for server "+server)
				log.Print("rpcerror:", err)
				log.Print("trying to reconnect...")
				time.Sleep(1 * time.Second)
				break
			} 
			inserter <- replyOSS
		}
	}
}

// insert data into MongoDb
// FIXME: MDS Version needed
func insert(inserter chan lustreserver.OstValues) {
    for {
        v := <- inserter
        fmt.Println("received and pushing!")
        fmt.Println(v)
    }
}

// starts go routines to
//  - spawn the collectors
//  - run the central clock
//  - spawn the inserters for the mongo db
func aggrRun() {

    collectors := conf.Collector.Hosts

    // show list of hosts
    var hostlist string
    for _, h := range collectors {
        hostlist = hostlist + h
    }
    log.Print("hosts to start collector on: "+hostlist)

    // create channels between collector and inserter go routines
    inserters := make([]chan lustreserver.OstValues, len(collectors))
    for i,_ := range inserters {
        inserters[i] = make(chan lustreserver.OstValues, conf.Collector.MaxEntries)
    }

    // create collector processes on the servers, do not wait for them, they are endless
    // this will not return, they will be restarted if the fail/end
    for _,c := range collectors {
        go spawn_collector(c)
    }
    
    // wait a second to allow collectors to start 
    // FIXME might need tuning? is 1 sec enough?
    time.Sleep(1 * time.Second)
    
    // unbuffered signal channel to signal clock to collect()
    signal := make(chan int)
    
    // create collect goroutines to collect data and push it down the channels
    // towards inserters
    for i,c := range collectors {
        go collect(c, signal, inserters[i])
    }    
    
    // create inserters to push data into mongodb, can be created after
    // collectors, channels will block until read
    for i,_ := range collectors {
        go insert(inserters[i])
    }
    
    // main loop, sending signals to collectors
    // each collector will get a signal to collect and push down the
    // channel towards inserter
    for {
        for _,_ = range collectors {
            signal <- 1
        } 
        time.Sleep(1 * time.Second)       
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
    
        
    // do work
    // FIXME OSS only so far
	aggrRun()  
        
    
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
