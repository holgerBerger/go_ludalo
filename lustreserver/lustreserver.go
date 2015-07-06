// lustreserver exposes oss and mds performance counters over rpc

// this includes for OSS number of read and write requests and number of bytes 
// written and read
// for MDT it delivers number of requests only.

package lustreserver

import (
	"io/ioutil"
	"os"
	"bufio"
	"strings"
	"strconv"
	"net/rpc"
	"net"
	"log"
    // "fmt"
)

// path to lustre proc (used for testing, should start with / for production!!)
const Procdir = "proc/fs/lustre/"

const ostprocpath = Procdir + "obdfilter/"

// different lustre versions have different locations for performance files 
// FIXME is this true or do both exist????
var mdtprocpath = []string{Procdir+"/mds", Procdir+"/mdt"}

// real pathnames after check with exists
var realmdtprocpath = []string{}

// OstStats gives write and read requests and bytes read and written
type OstStats struct {
	W_rqs, W_bs, R_rqs, R_bs int64
}


// OstValues contains maps with total values for each OST and for values each nid for each OST
type OstValues struct {
	OstTotal map[string]OstStats
	NidValues map[string]map[string]OstStats
}


// MdsValues contains maps with total values for each MDT and for values each nid for each MDT
type MdsValues struct {
	MdsTotal map[string]int64
	NidValues map[string]map[string]int64
}


// OSS RPC type
type OssRpc int;
// MDS RPC type
type MdsRpc int64;


// Mds registers RPC server for MDS
func MdsRPC() {
    mds := new(MdsRpc)
    rpc.Register(mds)
    realmdtprocpath = make([]string,1)
    for _, mdt := range mdtprocpath {
        if _, err := os.Stat(mdt); err == nil {
            realmdtprocpath = append(realmdtprocpath, mdt)
        } 
    }
    // fmt.Printf("reallist: %v\n", realmdtprocpath)
}


// Oss registers RPC server for OSS
func OssRPC() {
	oss := new(OssRpc)
    rpc.Register(oss)  
}


// StartServer starts the HTTP RPC server
func StartServer() {
    l, e := net.Listen("tcp", "0.0.0.0:1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
    
    // this serves endless
    rpc.Accept(l)
}


// GetValues RPC call for OST, return all performance counters
func (*OssRpc) GetValues(arg int, result *OstValues) error {
	//fmt.Printf("RPC oss\n")
	
	result.OstTotal = make(map[string]OstStats)
	result.NidValues = make(map[string]map[string]OstStats)
	
    ostlist, nidSet := getOstAndNidlist()
	for _, ost := range ostlist {
		result.OstTotal[ost] = readOstStatfile(ostprocpath + ost + "/stats")
		result.NidValues[ost] = make(map[string]OstStats)
		for nid, _ := range nidSet {
			result.NidValues[ost][nid] = readOstStatfile(ostprocpath + ost + "/exports/" + nid + "/stats")
		}
	}
	//fmt.Printf("RPC result %v\n", result)
	return nil
}



// GetValues RPC call for OST, return all performance counters
func (*MdsRpc) GetValues(arg int, result *MdsValues) error {
	// fmt.Printf("RPC mds\n")
	
	result.MdsTotal = make(map[string]int64)
	result.NidValues = make(map[string]map[string]int64)
	
    mdslist, nidSet := getMdtAndNidlist()
	for _, mds := range mdslist {   
        for _, base := range realmdtprocpath {
            result.MdsTotal[mds] = readMdsStatfile(base + "/" + mds + "/stats")
            result.NidValues[mds] = make(map[string]int64)
            for nid, _ := range nidSet {
                result.NidValues[mds][nid] = readMdsStatfile(base + "/" + mds + "/exports/" + nid + "/stats")
            }
        }
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
                stats.R_bs, _ = strconv.ParseInt(fields[nr - 1], 10, 64);
                stats.R_rqs, _ = strconv.ParseInt(fields[nr - 6], 10, 64);
            } else if strings.HasPrefix(s, "write_bytes") {
                fields := strings.Fields(s)
                nr := len(fields)
                stats.W_bs, _ = strconv.ParseInt(fields[nr - 1], 10, 64);
                stats.W_rqs, _ = strconv.ParseInt(fields[nr - 6], 10, 64);
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
	var requests int64;
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
                v, _ = strconv.ParseInt(fields[nr - 3], 10, 64);
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
	files, _ := ioutil.ReadDir(ostprocpath)
	for _, f := range files {
		if f.IsDir() {
			ostList = append(ostList, f.Name())
		}
	}
	
	// we use a map as set emulator, using an empty struct as value
	nidSet := make(map[string]struct{})
	
	for _, ost := range ostList {
		files, _ := ioutil.ReadDir(ostprocpath + ost + "/exports")
		for _, f := range files {
			if strings.ContainsAny(f.Name(), "@") {
				nidSet[f.Name()] = struct{}{}
				// fmt.Printf(f.Name())
			}
		}
	}
	
	return ostList, nidSet
}


// get list of MDTs and NIDs, nids is a map used as set
func getMdtAndNidlist() ([]string, map[string]struct{}) {
	mdtList := []string{}
    for _, mdt := range mdtprocpath {
        files, _ := ioutil.ReadDir(mdt)
        for _, f := range files {
            if f.IsDir() && strings.Index(f.Name(), "-MDT") != -1 {
                mdtList = append(mdtList, f.Name())
            }
        }
    }
	
	// we use a map as set emulator, using an empty struct as value
	nidSet := make(map[string]struct{})
	
    
	for _, mdt := range mdtList {
        for _, base := range realmdtprocpath {
            files, _ := ioutil.ReadDir(base + "/" + mdt + "/exports")
            for _, f := range files {
                if strings.ContainsAny(f.Name(), "@") {
                    nidSet[f.Name()] = struct{}{}
                    // fmt.Printf(f.Name())
                }
            }
        }
	}
	
	return mdtList, nidSet
}
