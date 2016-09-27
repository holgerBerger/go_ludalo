// Package lustreserver exposes oss and mds performance counters over rpc
// this includes for OSS number of read and write requests and number of bytes
// written and read
// for MDT it delivers total number of requests only (to be extended and precised
// down to single requests)
// TODO
//  fix mds for differences
//  offer difference + absolute mode for OST as for MDS
//	offer inquire rpc function if mdt or ost
package lustreserver

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	//	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

// path to lustre proc (used for testing, should start with / for production!!)
const Procdir = "/proc/fs/lustre/"

const ostprocpath = Procdir + "obdfilter/"

// different lustre versions have different locations for performance files
var mdtprocpath = map[string]string{
	"1.8": "/proc/fs/lustre/mds",
	"2.5": "/proc/fs/lustre/mdt",
}
var mdtstatname = map[string]string{
	"1.8": "/stats",
	"2.5": "/md_stats",
}

// real pathnames after check with exists
var realmdtprocpath string
var realstatname string

// OstStats gives write and read requests and bytes read and written
type OstStats struct {
	WRqs, WBs, RRqs, RBs int64
}

// MdsValues contains maps with total values for each MDT and for values each nid for each MDT
type MdsValues struct {
	Timestamp int32 // will be filled by aggregator and is used to transfer difference
	Delta     int32 // time difference
	MdsTotal  map[string]int64
	NidValues map[string]map[string]int64
}

// OstValues contains maps with total values for each OST and for values for each nid for each OST
type OstValues struct {
	Timestamp int32 // will be filled by aggregator
	Delta     int32 // time difference
	OstTotal  map[string]OstStats
	NidValues map[string]map[string]OstStats
}

// new and old ostvalues to build difference
var (
	ostvalues [2]OstValues
	mdsvalues [2]MdsValues
	oldpos    int = 0
	newpos    int = 1
)

// flags to show status
var (
	IsOST bool
	IsMDT bool
)

// ServerRpcT type, for inquiries lile server type
type ServerRpcT int

// OssRpcT type
type OssRpcT int

// MdsRpcT type
type MdsRpcT int64

// subtract b from a
func (a OstStats) sub(b OstStats) OstStats {
	var result OstStats
	result.WRqs = a.WRqs - b.WRqs
	result.RRqs = a.RRqs - b.RRqs
	result.WBs = a.WBs - b.WBs
	result.RBs = a.RBs - b.RBs
	return result
}

// check for zero
func (a OstStats) nonzero() bool {
	if (a.WRqs == 0) && (a.RRqs == 0) && (a.WBs == 0) && (a.RBs == 0) {
		return false
	}
	return true
}

// check for positive values
func (a OstStats) positive() bool {
	if (a.WRqs < 0) || (a.RRqs < 0) || (a.WBs < 0) || (a.RBs < 0) {
		return false
	}
	return true
}

// MakeServerRPC register RPC server for inquiries like OST/MDT
func MakeServerRPC() {
	server := new(ServerRpcT)
	rpc.Register(server)
}

// MakeMdsRPC registers RPC server for MDS
func MakeMdsRPC() {
	var ok bool
	mds := new(MdsRpcT)
	rpc.Register(mds)

	f, err := os.Open("/proc/fs/lustre/version")
	if err == nil {
		r := bufio.NewReader(f)

		line, _, err := r.ReadLine()
		if err == nil {
			version := strings.Fields(string(line))[1][:3]
			fmt.Println("lustre version", version)
			realmdtprocpath, ok = mdtprocpath[version]
			if !ok {
				fmt.Println("unknown lustre version", version)
			}
			realstatname, ok = mdtstatname[version]
		} else {
			log.Panic("could not read lustre version file!")
		}
	} else {
		log.Panic("no lustre version file found!")
	}
	f.Close()
}

// MakeOssRPC registers RPC server for OSS
func MakeOssRPC() {
	oss := new(OssRpcT)
	rpc.Register(oss)
}

// StartServer starts the HTTP RPC server
func StartServer(port int) {
	l, e := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// this serves endless
	rpc.Accept(l)
}

// IsOST RPC call returns if this is a OST
func (*ServerRpcT) IsOST(in int, result *bool) error {
	*result = IsOST
	return nil
}

// IsMDT RPC call returns if this is a MDT
func (*ServerRpcT) IsMDT(in int, result *bool) error {
	*result = IsMDT
	return nil
}

// GetRandomValues RPC call for OST, returns random values for testing
func (*OssRpcT) GetRandomValues(init bool, result *OstValues) error {
	result.OstTotal = make(map[string]OstStats)
	result.NidValues = make(map[string]map[string]OstStats)

	for i := 0; i < 10; i++ {
		ost := "OST" + strconv.Itoa(int(rand.Int63n(10)))

		t := result.OstTotal[ost]
		t.RBs = rand.Int63n(10)
		t.WBs = rand.Int63n(10)
		t.RRqs = rand.Int63n(100)
		t.WRqs = rand.Int63n(100)
		result.OstTotal[ost] = t

		result.NidValues[ost] = make(map[string]OstStats)
		for j := 0; j < 100; j++ {
			nid := "nid" + strconv.Itoa(int(rand.Int63n(100)))
			t := result.NidValues[ost][nid]
			t.RBs = rand.Int63n(10)
			t.WBs = rand.Int63n(10)
			t.RRqs = rand.Int63n(100)
			t.WRqs = rand.Int63n(100)
			result.NidValues[ost][nid] = t
		}
	}
	return nil
}

// GetValuesDiff RPC call for OST, return all performance counters which are not zero
// FIXME no absolute version yet
func (*OssRpcT) GetValuesDiff(init bool, result *OstValues) error {
	//fmt.Printf("RPC oss\n")

	/* PROFILING CODE
	pf, err := os.Create("/tmp/gocollector.profile")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(pf)
	*/

	var last, now int32
	if _, err := os.Stat(Procdir + "ost"); err == nil {
		if init {
			// we init old and new once to have both, as they cycle, otherwise panic
			ostvalues[newpos].OstTotal = make(map[string]OstStats)
			ostvalues[newpos].NidValues = make(map[string]map[string]OstStats)
			ostvalues[oldpos].OstTotal = make(map[string]OstStats)
			ostvalues[oldpos].NidValues = make(map[string]map[string]OstStats)
			ostvalues[oldpos].Timestamp = int32(time.Now().Unix())
		}

		// get values
		now = int32(time.Now().Unix())
		ostvalues[newpos].Timestamp = int32(now)
		ostlist, nidSet := getOstAndNidlist()
		for _, ost := range ostlist {
			ostvalues[newpos].OstTotal[ost] = readOstStatfile(ostprocpath + ost + "/stats")
			ostvalues[newpos].NidValues[ost] = make(map[string]OstStats)
			for nid := range nidSet {
				ostvalues[newpos].NidValues[ost][nid] = readOstStatfile(ostprocpath + ost + "/exports/" + nid + "/stats")
			}
		}

		// if not init, subtract and assign return values
		if !init {
			result.OstTotal = make(map[string]OstStats)
			result.NidValues = make(map[string]map[string]OstStats)

			// we check for nonzero and positive
			// non positive values could show up when counters overflow
			// zero values are ommited for space reasons

			for _, ost := range ostlist {
				result.NidValues[ost] = make(map[string]OstStats)
				_, ok := ostvalues[oldpos].OstTotal[ost]
				if !ok {
					continue // old value does not exist, we skip this one
					// this happens e.g. after a OST failover
				}
				diff := ostvalues[newpos].OstTotal[ost].sub(ostvalues[oldpos].OstTotal[ost])
				if diff.nonzero() && diff.positive() {
					result.OstTotal[ost] = diff
					last = ostvalues[oldpos].Timestamp
					result.Delta = int32(now - last)
					for nid := range nidSet {
						_, ok := (ostvalues[oldpos].NidValues[ost][nid])
						if !ok {
							continue // old value does not exist, we skip this one
							// this happens e.g. after a OST failover, or when a NID issues first IO
						}
						diff := ostvalues[newpos].NidValues[ost][nid].sub(ostvalues[oldpos].NidValues[ost][nid])
						if diff.nonzero() && diff.positive() {
							result.NidValues[ost][nid] = diff
						}
					}
				}
			}
		}
		newpos = (newpos + 1) % 2
		oldpos = (oldpos + 1) % 2
	} /* else {
		return errors.New("this is no ost")
	} */
	//fmt.Printf("RPC result %v\n", result)
	/* PROFILING CODE
	pprof.StopCPUProfile()
	*/
	return nil
}

// GetValuesDiff RPC call for MDS, return counters which are not zero
func (*MdsRpcT) GetValuesDiff(init bool, result *MdsValues) error {
	// fmt.Printf("RPC mds\n")
	var last, now int32
	if _, err := os.Stat(Procdir + "mds"); err == nil {
		if init {
			mdsvalues[newpos].MdsTotal = make(map[string]int64)
			mdsvalues[newpos].NidValues = make(map[string]map[string]int64)
			mdsvalues[oldpos].MdsTotal = make(map[string]int64)
			mdsvalues[oldpos].NidValues = make(map[string]map[string]int64)
			mdsvalues[oldpos].Timestamp = int32(time.Now().Unix())
		}

		now = int32(time.Now().Unix())
		mdsvalues[newpos].Timestamp = int32(now)
		mdslist, nidSet := getMdtAndNidlist()
		for _, mds := range mdslist {
			mdsvalues[newpos].MdsTotal[mds] = readMdsStatfile(realmdtprocpath + "/" + mds + realstatname)
			mdsvalues[newpos].NidValues[mds] = make(map[string]int64)
			for nid := range nidSet {
				mdsvalues[newpos].NidValues[mds][nid] = readMdsStatfile(realmdtprocpath + "/" + mds + "/exports/" + nid + "/stats")
			}
		}

		if !init {
			result.MdsTotal = make(map[string]int64)
			result.NidValues = make(map[string]map[string]int64)

			// we do not send zero and values < 0, for compression reasons
			// and as negative values indicate error conditions like counter overrun

			for _, mds := range mdslist {
				result.NidValues[mds] = make(map[string]int64)
				_, ok := mdsvalues[oldpos].MdsTotal[mds]
				if !ok {
					continue // we skip this one as no old value is available, e.g. after failover
				}
				diff := mdsvalues[newpos].MdsTotal[mds] - mdsvalues[oldpos].MdsTotal[mds]
				if diff > 0 {
					result.MdsTotal[mds] = diff
					last = mdsvalues[oldpos].Timestamp
					result.Delta = int32(now - last)
					for nid := range nidSet {
						_, ok := mdsvalues[oldpos].NidValues[mds][nid]
						if !ok {
							continue // we skip this one as no old value is available, e.g. after failover
						}
						diff := mdsvalues[newpos].NidValues[mds][nid] - mdsvalues[oldpos].NidValues[mds][nid]
						if diff > 0 {
							result.NidValues[mds][nid] = diff
						}
					}
				}
			}

		}
		newpos = (newpos + 1) % 2
		oldpos = (oldpos + 1) % 2
		last = now
	} /*else {
		return errors.New("no mdt")
	} */
	// fmt.Printf("RPC result %v\n", result)
	return nil
}

// GetValues RPC call for OST, return all performance counters
func (*MdsRpcT) GetValues(arg int, result *MdsValues) error {
	// fmt.Printf("RPC mds\n")
	if _, err := os.Stat(Procdir + "mds"); err == nil {
		result.MdsTotal = make(map[string]int64)
		result.NidValues = make(map[string]map[string]int64)

		mdslist, nidSet := getMdtAndNidlist()
		for _, mds := range mdslist {
			result.MdsTotal[mds] = readMdsStatfile(realmdtprocpath + "/" + mds + "/stats")
			result.NidValues[mds] = make(map[string]int64)
			for nid := range nidSet {
				result.NidValues[mds][nid] = readMdsStatfile(realmdtprocpath + "/" + mds + "/exports/" + nid + "/stats")
			}
		}
	} else {
		return errors.New("no mdt")
	}
	// fmt.Printf("RPC result %v\n", result)
	return nil
}

// read OST performance values from file, return struct with all 64bit values
func readOstStatfile(filename string) OstStats {
	var stats OstStats
	f, err := os.Open(filename)
	if err == nil {
		r := bufio.NewReader(f)

		line, isPrefix, err := r.ReadLine()
		for err == nil && !isPrefix {
			s := string(line)

			if strings.HasPrefix(s, "read_bytes") {
				fields := strings.Fields(s)
				nr := len(fields)
				stats.RBs, _ = strconv.ParseInt(fields[nr-1], 10, 64)
				stats.RRqs, _ = strconv.ParseInt(fields[nr-6], 10, 64)
			} else if strings.HasPrefix(s, "write_bytes") {
				fields := strings.Fields(s)
				nr := len(fields)
				stats.WBs, _ = strconv.ParseInt(fields[nr-1], 10, 64)
				stats.WRqs, _ = strconv.ParseInt(fields[nr-6], 10, 64)
			}
			line, isPrefix, err = r.ReadLine()
		}

		f.Close()
		// fmt.Printf("%s %v\n",filename, stats)
	}
	return stats
}

// read MDS performance values from file, return 64bit number of requests
func readMdsStatfile(filename string) int64 {
	var requests int64
	var v int64
	f, err := os.Open(filename)
	if err == nil {
		r := bufio.NewReader(f)

		line, isPrefix, err := r.ReadLine()
		for err == nil && !isPrefix {
			s := string(line)

			if strings.Index(s, "samples") != -1 {
				fields := strings.Fields(s)
				nr := len(fields)
				v, _ = strconv.ParseInt(fields[nr-3], 10, 64)
				requests += v
			}
			line, isPrefix, err = r.ReadLine()
		}

		f.Close()
		// fmt.Printf("%s %v\n",filename, requests)
	}
	return requests
}

// get list of OSTs and NIDs, nids is a map used as set
func getOstAndNidlist() ([]string, map[string]struct{}) {
	ostList := []string{}
	tmpfile, _ := os.Open(ostprocpath)
	files, _ := tmpfile.Readdir(-1)
	tmpfile.Close()
	for _, f := range files {
		if f.IsDir() {
			ostList = append(ostList, f.Name())
		}
	}

	// we use a map as set emulator, using an empty struct as value
	nidSet := make(map[string]struct{})

	for _, ost := range ostList {
		tmpfile, _ := os.Open(ostprocpath + ost + "/exports")
		files, _ := tmpfile.Readdirnames(-1)
		tmpfile.Close()
		for _, f := range files {
			if strings.ContainsAny(f, "@") {
				nidSet[f] = struct{}{}
			}
		}
	}

	return ostList, nidSet
}

// get list of MDTs and NIDs, nids is a map used as set
func getMdtAndNidlist() ([]string, map[string]struct{}) {
	mdtList := []string{}
	for _, mdt := range mdtprocpath {
		tmpfile, _ := os.Open(mdt)
		files, _ := tmpfile.Readdir(-1)
		tmpfile.Close()
		for _, f := range files {
			if f.IsDir() && strings.Index(f.Name(), "-MDT") != -1 {
				mdtList = append(mdtList, f.Name())
			}
		}
	}

	// we use a map as set emulator, using an empty struct as value
	nidSet := make(map[string]struct{})

	for _, mdt := range mdtList {
		tmpfile, _ := os.Open(realmdtprocpath + "/" + mdt + "/exports")
		files, _ := tmpfile.Readdirnames(-1)
		tmpfile.Close()
		for _, f := range files {
			if strings.ContainsAny(f, "@") {
				nidSet[f] = struct{}{}
			}
		}
	}

	return mdtList, nidSet
}
