package main

import (
	"github.com/holgerBerger/go_ludalo/lustreserver"
	"log"
	"net"
	"net/rpc"
)

// global variables for RPC access
var (
	OssData map[string]lustreserver.OstValues
)

type ServerRpcT int

// StartServer starts the HTTP RPC server
func startServer() {
	server := new(ServerRpcT)
	rpc.Register(server)
	l, e := net.Listen("tcp", "localhost:2345") // FIXME get port from config file
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// this serves endless
	rpc.Accept(l)
}

// OssList returns list of OSS
func (*ServerRpcT) OssList(in int, result *[]string) error {
	*result = make([]string, len(OssData))
	i := 0
	for v := range OssData {
		(*result)[i] = v
		i++
	}
	return nil
}

// OstList return list of all OSTs of all OSSes, in form FS-TARGET
func (*ServerRpcT) OstList(in int, result *[]string) error {
	c := 0
	// count osts
	for v := range OssData {
		c += len(OssData[v].OstTotal)
	}
	*result = make([]string, c)
	// assemble list
	i := 0
	for v := range OssData {
		for o := range OssData[v].OstTotal {
			(*result)[i] = o
			i++
		}
	}
	return nil
}
