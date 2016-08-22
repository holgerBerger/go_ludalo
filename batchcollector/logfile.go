package main

import (
	"bytes"
	"log"
	"time"
)

const unknownjob = "unknown_job"

type LogFileReader interface {
	readToEnd()
	getName() string
}

// get value from key value pairs where value is after key
func getvalue(tokens [][]byte, key string) string {
	for i := range tokens {
		if bytes.Compare(tokens[i], []byte(key)) == 0 {
			return string(tokens[i+1])
		}
	}
	return ""
}

// parsetime from a string, in the format of alps scheduler log
func parsetime(line string) time.Time {
	// ex:  2016-08-19T00:16:57.339913+02:00
	fmt := "2006-01-02T15:04:05.999999-07:00"
	t, err := time.Parse(fmt, line)
	if err != nil {
		log.Println("could not parse time in", line, err)
	}
	return t
}
