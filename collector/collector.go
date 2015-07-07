// collector main programm, sets up rpc servers for OSS and MDS

package main

import (
	"fmt"
	"github.com/holgerBerger/go_ludalo/lustreserver"
	"os"
)

func main() {

	hostname, _ := os.Hostname()
	fmt.Printf("go collector running on " + hostname + "\n")

	if _, err := os.Stat(lustreserver.Procdir + "ost"); err == nil {
		fmt.Printf(" serving oss data\n")
		lustreserver.OssRPC()
	}

	if _, err := os.Stat(lustreserver.Procdir + "mds"); err == nil {
		fmt.Printf(" serving mds data\n")
		lustreserver.MdsRPC()
	}

	// here we block endless
	lustreserver.StartServer()

}
