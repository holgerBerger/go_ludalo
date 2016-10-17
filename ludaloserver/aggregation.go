package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Jobentry mongo document
type Jobentry struct {
	ID      string     `bson:"_id,omitempty"`
	Jobid   string     `bson:"jobid"`
	Owner   string     `bson:"owner"`
	Start   int32      `bson:"start"`
	End     int32      `bson:"end"`
	Cmd     string     `bson:"cmd"`
	Nids    string     `bson:"nids"`
	Calc    int        `bson:"calc"`
	Cachets int32      `bson:"cachets"`
	MetaV   [4]int32   `bson:"metav"`
	DataV   [4]float32 `bson:"datav"`
}

// aggregation iterates over database and updates job data
// is endless, triggers go routine aggregation_worker
func aggregation() {

	// prepare and cache database connections
	var (
		session      map[string]*mgo.Session
		db           map[string]*mgo.Database
		err          error
		databasechan chan *mgo.Database
	)

	databasechan = make(chan *mgo.Database)

	session = make(map[string]*mgo.Session, len(conf.Systems))
	db = make(map[string]*mgo.Database, len(conf.Systems))

	for system, _ := range conf.Systems {
		session[system], err = mgo.Dial(conf.Systems[system].Dbserver)
		if err != nil {
			log.Println("could not connect to DB", err)
		}
		db[system] = session[system].DB(conf.Systems[system].Dbname)
	}

	// launch background worker
	go aggregation_worker(databasechan)

	// endless loop
	for {
		for system, _ := range conf.Systems {
			databasechan <- db[system]
		}
		// wait until next interval
		time.Sleep(time.Duration(conf.Interval) * time.Second)
	}

}

// aggregation_worker go routine to iterate in background over data
func aggregation_worker(databasechan chan *mgo.Database) {
	var database *mgo.Database
	var m1, m2, m3, m4 int32
	var d1, d2, d3, d4 float32

	// regex to match name, month and year of a collection name
	// a collection matching this pattern is assumed to be a performance collection
	regex, _ := regexp.Compile(`(.*)(\d\d)(\d\d\d\d)`)

	for {
		database = <-databasechan
		log.Println("started aggregation worker cycle, ql =", len(databasechan))
		t1 := time.Now()
		jobcollection := database.C("jobs")

		// get all jobs which are still running
		var jobs []Jobentry
		err := jobcollection.Find(bson.M{"end": -1}).All(&jobs)
		if err == nil {
			for i := range jobs {
				// fmt.Println(jobs[i].Jobid)

				now := time.Now().Unix()
				jobstart := jobs[i].Start
				jobend := jobs[i].End
				cachets := jobs[i].Cachets
				if cachets > 0 {
					jobstart = cachets
					m1 = jobs[i].MetaV[0]
					m2 = jobs[i].MetaV[1]
					m3 = jobs[i].MetaV[2]
					m4 = jobs[i].MetaV[3]
					d1 = jobs[i].DataV[0]
					d2 = jobs[i].DataV[1]
					d3 = jobs[i].DataV[2]
					d4 = jobs[i].DataV[3]
				} else {
					m1 = 0
					m2 = 0
					m3 = 0
					m4 = 0
					d1 = 0.0
					d2 = 0.0
					d3 = 0.0
					d4 = 0.0
				}
				if jobend == -1 {
					jobend = int32(now)
				}

				// fmt.Println("start:", time.Unix(int64(jobstart),0))
				// fmt.Println("end:", time.Unix(int64(jobend),0))

				jsm := int(time.Unix(int64(jobstart), 0).Month())
				jsy := time.Unix(int64(jobstart), 0).Year()

				jem := int(time.Unix(int64(jobend), 0).Month())
				jey := time.Unix(int64(jobend), 0).Year()

				// construct a nodelist
				// FIXME expand nids for alps
				// nodelist := strings.Split(jobs[i].Nids, ",")
				nodelist := nidexpander(jobs[i].Nids)

				collections, _ := database.CollectionNames()
				for _, collname := range collections {
					m := regex.FindStringSubmatch(collname)
					if m != nil {
						// fmt.Println(">>", m[0], m[1], m[2])
						month, _ := strconv.Atoi(m[2])
						year, _ := strconv.Atoi(m[3])

						// is collection between start and end?
						if (jsy <= year && jsm <= month) && (jey >= year && jem >= month) {
							// fmt.Println("match:", collname)
							// self.perfcoll.find({"$and": [ {"ts": {"$gt": start}}, {"ts": {"$lt": end}}, {"nid": {"$in": jobs[j].nodelist}} ] })
							var data []bson.M
							err := database.C(collname).Find(bson.M{
								"$and": []bson.M{
									bson.M{"ts": bson.M{"$gt": jobstart}},
									bson.M{"ts": bson.M{"$lt": jobend}},
									bson.M{"nid": bson.M{"$in": nodelist}},
								},
							}).All(&data)
							if err == nil {
								for _, d := range data {
									// some trickery with type assertions and casts
									_, ok := d["mdt"]
									if ok {
										if v, ok := d["v"].([]interface{}); ok {
											m1 += int32(v[0].(int))
											m2 += int32(v[1].(int))
											m3 += int32(v[2].(int))
											m4 += int32(v[3].(int))
										}
									} else {
										_, ok := d["ost"]
										if ok {
											if v, ok := d["v"].([]interface{}); ok {
												d1 += float32(v[0].(float64))
												d2 += float32(v[1].(float64))
												d3 += float32(v[2].(float64))
												d4 += float32(v[3].(float64))
											}
										}
									}
								}
							} else {
								fmt.Println(err)
							}
						}
					}
				} // collections

				// update DB
				err := database.C("jobs").Update(bson.M{"_id": jobs[i].ID},
					bson.M{"$set": bson.M{"cachets": int32(now),
						"metav": [4]int32{m1, m2, m3, m4},
						"datav": [4]float32{d1, d2, d3, d4},
					}})
				if err == nil {
					log.Println("  updated", jobs[i].ID)
				} else {
					// most probably 'not found'
					//log.Println(" ", err, jobs[i].Jobid)
				}

			} // jobs
		} else {
			log.Println(err)
		}
		log.Println("ended aggregation worker cycle after", time.Now().Sub(t1))
	}
}

// nidexpander expands comma separated lists containing ranges (only if tokens
// do not contain letters)
//  "n1,n2" -> ["n1","n2"]
//  "1-3" -> ["1","2","3"]
//  "n1-n3" -> NOT DEFINED
func nidexpander(nids string) []string {
	result := make([]string, 0, 0)

	for _, i := range strings.Split(nids, ",") {
		if strings.Contains(i, "-") {
			sp := strings.Split(i, "-")
			start, _ := strconv.Atoi(sp[0])
			end, _ := strconv.Atoi(sp[1])
			for ii := start; ii <= end; ii++ {
				result = append(result, strconv.Itoa(ii))
			}
		} else {
			result = append(result, i)
		}
	}
	return result
}
