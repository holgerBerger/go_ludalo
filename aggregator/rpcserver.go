package main

import (
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/holgerBerger/go_ludalo/lustreserver"
)

// global variables for RPC access
var (
	OssData      map[string]lustreserver.OstValues
	OssDataMutex sync.RWMutex
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
	OssDataMutex.RLock()
	for v := range OssData {
		(*result)[i] = v
		i++
	}
	OssDataMutex.RUnlock()
	return nil
}

// OstList return list of all OSTs of all OSSes, in form FS-TARGET
func (*ServerRpcT) OstList(in int, result *[]string) error {
	c := 0
	// count osts
	OssDataMutex.RLock()
	for v := range OssData {
		c += len(OssData[v].OstTotal)
	}
	OssDataMutex.RUnlock()
	*result = make([]string, c)
	// assemble list
	i := 0
	OssDataMutex.RLock()
	for v := range OssData {
		for o := range OssData[v].OstTotal {
			(*result)[i] = o
			i++
		}
	}
	OssDataMutex.RUnlock()
	return nil
}
