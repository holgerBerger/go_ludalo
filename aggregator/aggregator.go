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
)


// FIXME read data from config file
var collectors = []string{"collector/collecto","collector/collector"}


// spawn_collectors starts collectors, retrying 10 times/max 20 seconds
// waiting 1 second between retries
// FIXME read data from config file
func spawn_collector(c string) {
    var count int;
    count = 0
    t1 := time.Now()
    fmt.Println("INFO: starting "+c)
    for {
        count++
        out, err := exec.Command(c).CombinedOutput()
        if err != nil {
            fmt.Println("ERROR could not start "+c)
            fmt.Println(string(out))
            fmt.Println("END of output")
        }
        t2 := time.Now()
        // if we restart 10 times within 20 seconds, something is strange,
        // we bail out that one, may be the config is bad, and it will
        // never work
        if count>=10 && t2.Sub(t1).Seconds() < 20 {
            fmt.Println("ERROR: could not start for 10 times, giving up on "+c)
            break
        }
        time.Sleep(1 * time.Second)
    }
    fmt.Println("INFO: ending for "+c)
}


func collect(signal chan int) {
    for {
        fmt.Println("Waiting...")
        <-signal
        fmt.Println("Yo!")
    }
}


// starts go routines to
//  - spawn the collectors
//  - run the central clock
//  - spawn the inserters for the mongo db
func aggrRun() {

    // spawn the collectors, do not wait for them, the are endless
    for _,c := range collectors {
        go spawn_collector(c)
    }
    
    // wait a second to allow collectors to start
    time.Sleep(1 * time.Second)
    
    // unbuffered signal channel to signal clock to collect()
    signal := make(chan int)
    for _,_ = range collectors {
        go collect(signal)
    }    
    
    // main loop, sending signals
    for {
        for _,_ = range collectors {
            signal <- 1
        } 
        time.Sleep(1 * time.Second)       
    }
}


func main() {

    aggrRun()

	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// OSS example

	var replyOSS lustreserver.OstValues
	//replyOSS.OstTotal = make(map[string]lustreserver.OstStats)
	//replyOSS.NidValues = make(map[string]map[string]lustreserver.OstStats)

	err = client.Call("OssRpc.GetRandomValues", true, &replyOSS)
	if err != nil {
		log.Fatal("rpcerror:", err)
	}
	
    t1 := time.Now()
	err = client.Call("OssRpc.GetRandomValues", false, &replyOSS)
    t2 := time.Now()
	if err != nil {
		log.Fatal("rpcerror:", err)
	}
	fmt.Printf("\n%v\n", replyOSS)
    fmt.Printf("%f secs\n",t2.Sub(t1).Seconds())

	// MDS example

	var replyMDS lustreserver.MdsValues
	//replyMDS.MdsTotal = make(map[string]int64)
	//replyMDS.NidValues = make(map[string]map[string]int64)

/*
	err = client.Call("MdsRpc.GetValues", true, &replyMDS)
	if err != nil {
		log.Fatal("rpcerror:", err)
	}
*/
    t1 = time.Now()
	err = client.Call("MdsRpc.GetValues", /*false*/ 0, &replyMDS)
    t2 = time.Now()
	if err != nil {
		log.Fatal("rpcerror:", err)
	}
	fmt.Printf("\n%v\n", replyMDS)
    fmt.Printf("%f secs\n",t2.Sub(t1).Seconds())

}
