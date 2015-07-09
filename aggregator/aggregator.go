// sample aggregator, doing nothing except fetching values

package main

import (
	"fmt"
	"log"
	"github.com/holgerBerger/go_ludalo/lustreserver"
	"net/rpc"
    "time"
)

func main() {

	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// OSS example

	var replyOSS lustreserver.OstValues
	replyOSS.OstTotal = make(map[string]lustreserver.OstStats)
	replyOSS.NidValues = make(map[string]map[string]lustreserver.OstStats)

    t1 := time.Now()
	err = client.Call("OssRpc.GetValues", 1, &replyOSS)
    t2 := time.Now()
	if err != nil {
		log.Fatal("rpcerror:", err)
	}
	fmt.Printf("\n%v\n", replyOSS)
    fmt.Printf("%f secs\n",t2.Sub(t1).Seconds())

	// MDS example

	var replyMDS lustreserver.MdsValues
	replyMDS.MdsTotal = make(map[string]int64)
	replyMDS.NidValues = make(map[string]map[string]int64)

    t1 = time.Now()
	err = client.Call("MdsRpc.GetValues", 1, &replyMDS)
    t2 = time.Now()
	if err != nil {
		log.Fatal("rpcerror:", err)
	}
	fmt.Printf("\n%v\n", replyMDS)
    fmt.Printf("%f secs\n",t2.Sub(t1).Seconds())

}
