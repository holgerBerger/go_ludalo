// +build !alps

package main

import "errors"

/*
  dummy persistent mapping from resid to jobid,
	not using any db, to reduce dependencies for torque build
*/

// Res2job is used to map resid to jobid using leveldb
type Res2job struct {
	db map[string]string
}

// NewRes2job creates a db object
func NewRes2job(name string) *Res2job {
	res2job := new(Res2job)
	res2job.db = make(map[string]string)
	return res2job
}

func (r *Res2job) getJob(res string) (string, error) {
	str, ok := r.db[res]
	if !ok {
		return string(str), errors.New("not in db")
	}
	return string(str), nil
}

func (r *Res2job) setJob(res, job string) {
	r.db[res] = job
}

func (r *Res2job) delJob(res string) {
	delete(r.db, res)
}
