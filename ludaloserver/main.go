package main

/*

ludalo servers task is to run on DB server,
and do some postprocessing/aggregation on data
that was inserted by aggregator and server
as gui server

tasks are
- go over all currently running and recently finished jobs,
  and accumulate the data volumes written/read. This is to
  speed up display of job information
- go over all currently running and recently finished jobs,
  and acuumulate time series data from NIDSxOSTS into a jobs
  aggregated timeseries (format to be defined) which is part of
  job database and will not be discarded with old data
- serve as a webserver for a gui to query the data

*/

import (
	"log"

	"github.com/BurntSushi/toml"
)

var conf configT

func main() {

	log.Print("starting ludalo server")
	if _, err := toml.DecodeFile("ludalo_server.config", &conf); err != nil {
		log.Print("error in reading ludalo_server.config:")
		log.Fatal(err)
	} else {
		log.Print("config <ludalo_server.config> read succesfully")
	}

	aggregation()

}
