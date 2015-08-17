#!/usr/bin/env python


#
# indices used:
#
#  use goludalo
#  db.<fs>.createIndex({"ts":1, "nid":1})
#
#  use ludalo
#  db.jobs.createIndex({"start":1}) 
#  db.jobs.createIndex({"end":1}) 
#  db.jobs.createIndex({"jobid":1}) 

import time,sys

# CONFIG
DBHOST="localhost"
SNAP = 5
PERFDB="goludalo"
JOBDB="ludalo"
JOBCOLLECTION="jobs"
# map filesystems to batchservers to skip some DB queries
batchservermap={
"nobnec":"intern2",
"alnec":"intern3"
}
batchskip=True   # set to false if skipping map should not be used
# END CONFIG

import pymongo 


# round current time to snap
def getCurrentSnapTime():
    return (int(time.time())/SNAP)*SNAP


# nodestats object, represents a node and its IO statistics
class nodestats(object):
    def __init__(self, nodename):
        self.nodename = nodename
        self.miops = 0
        self.wiops = 0
        self.wbw = 0
        self.riops = 0
        self.rbw = 0
        self.dt = 0

    def __repr__(self):
        return "%s: [%d,%d,%d,%d,%d]" % (self.nodename, self.miops, self.wiops, self.wbw, self.riops, self.rbw)

# jobstats object, represents a node and its IO statistics
class jobstats(object):
    def __init__(self, jobid):
        self.jobid = jobid
        self.miops = 0
        self.wiops = 0
        self.wbw = 0
        self.riops = 0
        self.rbw = 0
        self.nodelist=[]
        self.start = 0
        self.end = 0
        self.owner = ""
        self.cmd = ""
        self.dt = 1

    def addnode(self, node):
        self.miops += node.miops
        self.wiops += node.wiops
        self.wbw += node.wbw
        self.riops += node.riops
        self.rbw += node.rbw
        self.dt = node.dt

    def __repr__(self):
        return "%s: %s [%d,%d,%d,%d,%d]" % (self.jobid, self.owner, self.miops, self.wiops, self.wbw, self.riops, self.rbw)



# filesystem object, containing mongo connections
class filesystem(object):
    def __init__(self, server, fsname):
        self.fsname = fsname
        self.client = pymongo.MongoClient(server)    

        self.perfdb = self.client[PERFDB]
        self.perfcoll = self.perfdb[fsname]

        self.jobdb = self.client[JOBDB]
        self.jobcoll = self.jobdb[JOBCOLLECTION]

    # get latest timestamp, searching 5 minutes in the past
    def getLatestTs(self):
        latest=self.perfcoll.find({"ts": {"$gt":getCurrentSnapTime()-300}}).sort("ts",pymongo.DESCENDING)[0][u'ts']
        return latest
        
    # get entries for a certain timestamp
    def getEntries(self, timestamp):
        for p in self.perfcoll.find({"ts":timestamp}):
            yield p


    # get a tuple of current timestamp and a dict of nodestats of all nodes doing IO at the moment
    def currentNodesstats(self):
        nodes={}
        timestamp=self.getLatestTs()
        for e in self.getEntries(timestamp):
            node = e["nid"]
            if node == "aggr": continue
            if node not in nodes:
                nodes[node]=nodestats(node)
                nodes[node].dt = e['dt']
            if 'mdt' in e:
                nodes[node].miops += e['v']
            elif 'ost' in e:
                nodes[node].wiops += e['v'][0]
                nodes[node].wbw   += e['v'][1]
                nodes[node].riops += e['v'][2]
                nodes[node].rbw   += e['v'][3]
        return (timestamp, nodes)


    # map a dict of nodestats to running jobs at the time
    def mapNodesToJobs(self, timestamp, nodes):
        # { "_id" : ObjectId("559e8c86580eb358815b87a2"), "end" : 1436430906, "cmd" : "", "jobid" : "659096.intern2-2015", "nids" : "n151001", "start" : 1436425679, "owner" : "ppb742", "calc" : -1 }

        # FIXME -1 = running
        jobs={}
        nidtojob={}
        for j in self.jobcoll.find({ "$and" : [ {"end":-1}, {"start": {"$lt":timestamp}} ] }):
            jobid=j["jobid"]
            if jobid not in jobs:
                jobs[jobid]=jobstats(jobid)
                for nid in j["nids"].split(","):
                    nidtojob[nid]=jobid
                    jobs[jobid].nodelist.append(nid)
                    jobs[jobid].start = j["start"]
                    jobs[jobid].end = j["end"]
                    jobs[jobid].owner = j["owner"]
                    jobs[jobid].cmd = j["cmd"]
                    
        # joblist contains now list of jobs of ALL clusters!!!                

        fsjobs=set()

        for node in nodes:
            try:
                job = nidtojob[node]
                jobs[job].addnode(nodes[node])
                if job not in fsjobs:
                    fsjobs.add(job)
            except KeyError:
                jobs[node]=jobstats(node)
                jobs[node].addnode(nodes[node])
                jobs[node].nodelist.append(node)
                if node not in fsjobs:
                    fsjobs.add(node)
            
        localjobs={}
        for j in fsjobs:
            localjobs[j]=jobs[j]
        return localjobs
            
    # get all running jobs (from all clusters, can not be avoided)
    def getRunningJobs(self):
        jobs={}
        for j in self.jobcoll.find({"end":-1}):
            jobid=j["jobid"]
            if batchskip and jobid.find(batchservermap[self.fsname])<0: 
                continue
            if jobid not in jobs:
                jobs[jobid]=jobstats(jobid)
                for nid in j["nids"].split(","):
                    jobs[jobid].nodelist.append(nid)
                jobs[jobid].start = j["start"]
                jobs[jobid].end = j["end"]
                jobs[jobid].owner = j["owner"]
                jobs[jobid].cmd = j["cmd"]
                try:
                    jobs[jobid].cachets = j["cachets"]
                    jobs[jobid].miops = j["miops"]
                    jobs[jobid].wiops = j["wiops"]
                    jobs[jobid].wbw   = j["wbw"]
                    jobs[jobid].riops = j["riops"]
                    jobs[jobid].rbw   = j["rbw"]
                except KeyError:
                    # no cached data for this job
                    jobs[jobid].cachets = jobs[jobid].start
                    jobs[jobid].miops = 0
                    jobs[jobid].wiops = 0
                    jobs[jobid].wbw   = 0
                    jobs[jobid].riops = 0
                    jobs[jobid].rbw   = 0
        return jobs

    # go over all jobs in list, and add all stats of nodes in job from start to end
    # to it (end==-1 is covered, so can be used for running jobs as well)
    def accumulateJobStats(self, jobs):
        fsjobs=set()
        for j in jobs:
            if batchskip and j.find(batchservermap[self.fsname])<0: 
                continue
            if jobs[j].end == -1:
                end = int(time.time())
            else:
                end = jobs[j].end
            # we start from cached data, if nothing is cached, this is start
            start = jobs[j].cachets

            # print "scanning for",end-start, "sec for",j

            for e in self.perfcoll.find({"$and": [ {"ts": {"$gt": start}}, {"ts": {"$lt": end}}, {"nid": {"$in": jobs[j].nodelist}} ] }):
                node = e["nid"]
                if node == "aggr": continue
                if 'mdt' in e:
                    jobs[j].miops += e['v']
                elif 'ost' in e:
                    jobs[j].wiops += e['v'][0]
                    jobs[j].wbw   += e['v'][1]
                    jobs[j].riops += e['v'][2]
                    jobs[j].rbw   += e['v'][3]
            fsjobs.add(j)

            # update cache, write cachets, between start and cachets, data was already summed up
            # print "update", j
            self.jobcoll.update( 
                    {"jobid":j}, 
                    {"$set": { 
                        "cachets":end, 
                        "miops":jobs[j].miops,
                        "wiops":jobs[j].wiops,
                        "wbw":jobs[j].wbw,
                        "riops":jobs[j].riops,
                        "rbw":jobs[j].rbw   
                    } } )

        localjobs={}
        for j in fsjobs:
            localjobs[j]=jobs[j]
        return localjobs

    def readFSMaxima(self):
        self.maxcoll = self.perfdb["fsmaxima"]
        e = self.maxcoll.find_one({"fsname": self.fsname})
        if e == None:
            return [0, 0, 0, 0, 0, 0]
        else:
            return e["maxima"]
        
    #  0: nodes
    #  1: metadata
    #  2: wrqs
    #  3: rrqs
    #  4: wbw
    #  5: rbw
    def writeFSMaxima(self, maxima):
        r = self.maxcoll.update( 
                {"fsname": self.fsname},
                {"$set": { 
                    "maxima": maxima
                        } 
                } , 
                upsert=True
            ) 
        #print r

    # get AGGR values for fs from start to end
    def getFSvalues(self, start, end):
        timelist = {}
        for e in self.perfcoll.find({"$and": [ {"ts": {"$gt": start}}, {"ts": {"$lt": end}}, {"nid": "aggr"} ] }):
            ts = e["ts"]
            if ts not in timelist:
                timelist[ts]={}
                timelist[ts]["miops"] = 0
                timelist[ts]["wiops"] = 0
                timelist[ts]["wbw"]   = 0
                timelist[ts]["riops"] = 0
                timelist[ts]["rbw"]   = 0
            if 'mdt' in e:
                timelist[ts]["miops"] += e['v']
            elif 'ost' in e:
                timelist[ts]["wiops"] += e['v'][0]
                timelist[ts]["wbw"]   += e['v'][1]
                timelist[ts]["riops"] += e['v'][2]
                timelist[ts]["rbw"]   += e['v'][3]
        return timelist
        

# print TOP like list of jobs, with current rates
def printTopjobs(fsname, key):
    fs = filesystem(DBHOST, fsname)
    (timestamp, nodes) = fs.currentNodesstats()
    print time.ctime(timestamp),"\n"
    jobs = fs.mapNodesToJobs(timestamp, nodes)
    if key == "meta":
        sortf=lambda x: x.miops    
    elif key == "iops":
        sortf=lambda x: x.wiops+x.riops
    elif key == "bw":
        sortf=lambda x: x.rbw+x.wbw
    else:
        print "use meta, iops or bw as sorting key"
        sys.exit()
    print "JOBID      OWNER    NODES  META   WRITE      WrBW   READ      ReBW"
    print "                           IOPS    IOPS      MB/s   IOPS      MB/s"
    print "=================================================================="
    for j in sorted(jobs.values(), key=sortf, reverse=True):
        dt = float(j.dt)
        print "%-10s %-8s %-5s %6d %6d %9.2f %6d %9.2f" % (j.jobid.split(".")[0], j.owner, len(j.nodelist), j.miops/dt, j.wiops/dt, (j.wbw/dt)/1000000.0, j.riops/dt, (j.rbw/dt)/1000000.0)


# print TOP like list of jobs, with absolute values over runtime (sum over time)
def printJobSummary(fsname, key):
    fs = filesystem(DBHOST, fsname)
    jobs = fs.getRunningJobs()
    jobs = fs.accumulateJobStats(jobs)
    if key == "meta":
        sortf=lambda x: x.miops    
    elif key == "iops":
        sortf=lambda x: x.wiops+x.riops
    elif key == "bw":
        sortf=lambda x: x.rbw+x.wbw
    else:
        print "use meta, iops or bw as sorting key"
        sys.exit()

    print "JOBID      OWNER    NODES TIME   META  WRITE      WrBW   READ      ReBW"
    print "                          [H]   KIOPS  KIOPS      [GB]   KIOPS     [GB]"
    print "======================================================================="
    for j in sorted(jobs.values(), key=sortf, reverse=True):
        print "%-10s %-8s %-5s %4.1f %6d %6d %9.2f %6d %9.2f" % (j.jobid.split(".")[0], j.owner, len(j.nodelist), (time.time()-j.start)/3600, j.miops/1000, j.wiops/1000, j.wbw/1000000000.0, j.riops/1000, j.rbw/1000000000.0)



if __name__ == '__main__':

    if len(sys.argv)<4:
        print "usage: top.py [sum|top] fsname [meta|iops|bw]"
        print "        sum: show aggregated values over runtime of active jobs"
        print "        top: show current values of active jobs"
        print 
        print "        meta: sort for metadata operation"
        print "        iops: sort for iops"
        print "        bw: sort for bandwidth"
        sys.exit(0)

    if sys.argv[1]=="top":
        printTopjobs(sys.argv[2], sys.argv[3])
    if sys.argv[1]=="sum":
        printJobSummary(sys.argv[2], sys.argv[3])
