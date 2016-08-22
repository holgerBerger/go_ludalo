package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
)

// Logfile is an open logfile
type TorqueLogfile struct {
	name   string
	file   *os.File
	mongo  MongoInserter
	reader *bufio.Reader
}

// newLogfile opens a file and reads to current end
func newTorqueLogfile(name string, mongo MongoInserter) *TorqueLogfile {
	file, err := os.Open(name)
	if err != nil {
		panic("could not open file" + name)
	}
	reader := bufio.NewReaderSize(file, 1024*1024) // reader with 1MB buffer
	logfile := TorqueLogfile{name, file, mongo, reader}
	log.Println("opened new logfile", name)

	logfile.readToEnd()

	return &logfile
}

func (l *TorqueLogfile) getName() string {
	return l.name
}

// readToEnd reads and process the file to the end
func (l *TorqueLogfile) readToEnd() {

	for {
		line, err := l.reader.ReadBytes('\n')
		//fmt.Println(line)
		if err != nil {
			break
		}

		tokens := bytes.Split(bytes.TrimSpace(line), []byte(";"))

		if len(tokens) > 1 &&
			(bytes.Compare(tokens[1], []byte("S")) == 0 ||
				bytes.Compare(tokens[1], []byte("E")) == 0 ||
				bytes.Compare(tokens[1], []byte("A")) == 0 ||
				bytes.Compare(tokens[1], []byte("D")) == 0) {
			//			jobid := tokens[2]
			//			datestr := tokens[0]
		}

	}
}
