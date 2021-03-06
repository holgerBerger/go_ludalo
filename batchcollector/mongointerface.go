package main

import "time"

// MongoInserter is interface for database inserters
type MongoInserter interface {
	InsertCompleteJob(job Jobentry)
	InsertJob(jobid string, start time.Time)
	AddJobInfo(jobid, uid, cmd, nids string)
	EndJob(jobid string, end time.Time)
	Shutdown()
}
