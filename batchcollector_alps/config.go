package main

import (
	"log"

	"github.com/BurntSushi/toml"
)

type Config struct {
	WatchDirectory string
	FilePrefix     string
}

// global variable with config
var config Config

// readConf reads the config file, nothing more
func readConf() {
	// read config
	if _, err := toml.DecodeFile("batchcollector_alps.conf", &config); err != nil {
		// handle error
		log.Print("error in reading batchcollector_alps.conf:")
		log.Fatal(err)
	}
}
