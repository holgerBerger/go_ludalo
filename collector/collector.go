// collector main programm, sets up rpc servers for OSS and MDS

// serves both in any case, not available data will deliver a rpc.error
// so in any case, the collector will wait endless for ost and mdt data
// and deliver data the moment it is available and it is requested.

package main

import (
	"fmt"
	"github.com/holgerBerger/go_ludalo/lustreserver"
	"os"
)

func main() {

	hostname, _ := os.Hostname()
	fmt.Printf("go collector running on " + hostname + "\n")

	lustreserver.MakeServerRPC()

	if _, err := os.Stat(lustreserver.Procdir + "ost"); err == nil {
		fmt.Printf(" looks like ost, serving ost\n")
		lustreserver.IsOST = true
	} else {
		fmt.Printf(" waiting for ost data\n")
		lustreserver.IsOST = false
	}
	lustreserver.MakeOssRPC()

	if _, err := os.Stat(lustreserver.Procdir + "mds"); err == nil {
		fmt.Printf(" looks like mdt, serving mdt\n")
		lustreserver.IsMDT = true
	} else {
		fmt.Printf(" waiting for mds data\n")
		lustreserver.IsMDT = true
	}
	lustreserver.MakeMdsRPC()

	// here we block endless
	lustreserver.StartServer()
}
