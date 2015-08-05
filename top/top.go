/*

	top like tool

	idea:
		- list FS with IO sorted according to BW/s or IOPS/s or META/s
		- after selecting an FS
			- list OSTs of FS with IO sorted according to BW/s or IOPS/s or META/s

*/
package main

import (
	"fmt"
	"log"
	"net/rpc"
)

func ossList(client *rpc.Client) []string {
	var reply []string
	err := client.Call("ServerRpcT.OssList", 0, &reply)
	if err != nil {
		log.Panic("rpcerror:", err)
	}
	return reply
}

func ostList(client *rpc.Client) []string {
	var reply []string
	err := client.Call("ServerRpcT.OstList", 0, &reply)
	if err != nil {
		log.Panic("rpcerror:", err)
	}
	return reply
}

func main() {

	client, err := rpc.Dial("tcp", "localhost:2345")
	if err != nil {
		log.Panic(err)
	}

	ostlist := ostList(client)
	fmt.Println(ostlist)
}
