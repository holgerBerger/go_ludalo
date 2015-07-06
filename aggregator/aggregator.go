// sample aggregator, doing nothing except fetching values

package main

import (
	"fmt"
	"log"
	"lustreserver"
	"net/rpc"
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

	err = client.Call("OssRpc.GetValues", 1, &replyOSS)
	if err != nil {
		log.Fatal("rpcerror:", err)
	}
	fmt.Printf("\n%v\n", replyOSS)

	// MDS example

	var replyMDS lustreserver.MdsValues
	replyMDS.MdsTotal = make(map[string]int64)
	replyMDS.NidValues = make(map[string]map[string]int64)

	err = client.Call("MdsRpc.GetValues", 1, &replyMDS)
	if err != nil {
		log.Fatal("rpcerror:", err)
	}
	fmt.Printf("\n%v\n", replyMDS)

}
