// +build alps

package main

/*
  persistent mapping from resid to jobid
  uses leveldb
	is not required for non-alps build
*/

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

// Res2job is used to map resid to jobid using leveldb
type Res2job struct {
	db *leveldb.DB
}

// NewRes2job creates a db object
func NewRes2job(name string) *Res2job {
	db, err := leveldb.OpenFile(name, nil)
	if err != nil {
		fmt.Println(err)
		panic("could not open cache for jobmappings.")
	}
	res2job := new(Res2job)
	res2job.db = db
	return res2job
}

func (r *Res2job) getJob(res string) (string, error) {
	str, err := r.db.Get([]byte(res), nil)
	return string(str), err
}

func (r *Res2job) setJob(res, job string) {
	r.db.Put([]byte(res), []byte(job), nil)
}

func (r *Res2job) delJob(res string) {
	r.db.Delete([]byte(res), nil)
}
