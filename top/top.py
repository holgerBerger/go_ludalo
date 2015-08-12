#!/usr/bin/env python

import time

# CONFIG
DBHOST="localhost"
SNAP = 5
PERFDB="goludalo"
JOBDB="ludalo"
JOBCOLLECTION="jobs"
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
		self.riops += node.riops
		self.dt = node.dt

	def __repr__(self):
		return "%s: %s [%d,%d,%d,%d,%d]" % (self.jobid, self.owner, self.miops, self.wiops, self.wbw, self.riops, self.rbw)



# filesystem object, containing mongo connections
class filesystem(object):
	def __init__(self, server, fsname):
		self.client = pymongo.MongoClient(server)	

		self.perfdb = self.client[PERFDB]
		self.perfcoll = self.perfdb[fsname]

		self.jobdb = self.client[JOBDB]
		self.jobcoll = self.jobdb[JOBCOLLECTION]

	# get latest timestamp, searching 5 minutes in the past
	def getLatestTs(self):
		return self.perfcoll.find({"ts": {"$gt":getCurrentSnapTime()-300}}).sort("ts",pymongo.ASCENDING)[0][u'ts']
		
	# get entries for a certain timestamp
	def getEntries(self, timestamp):
		for p in self.perfcoll.find({"ts":timestamp}):
			yield p


	# get a tuple of current timestamp and a dict of nodestats of all nodes doing IO at the moment
	def currentNodesstats(self):
		nodes={}
		timestamp=fs.getLatestTs()
		for e in self.getEntries(timestamp):
			node = e["nid"]
			if node not in nodes:
				nodes[node]=nodestats(node)
				nodes[node].dt = e['dt']
			if 'mdt' in e:
				nodes[node].miops = e['v']
			elif 'ost' in e:
				nodes[node].wiops = e['v'][0]
				nodes[node].wbw =   e['v'][1]
				nodes[node].riops = e['v'][2]
				nodes[node].rbw =   e['v'][3]
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
					
					
		for node in nodes:
			try:
				jobs[nidtojob[node]].addnode(nodes[node])
			except KeyError:
				jobs[node]=jobstats(node)
				jobs[node].addnode(nodes[node])
				jobs[node].nodelist.append(node)
				
			
		return jobs


fs = filesystem(DBHOST, "nobnec")

(timestamp, nodes) = fs.currentNodesstats()
jobs = fs.mapNodesToJobs(timestamp, nodes)

print "JOBID      OWNER    NODES  MIOPS   WIOS      WBW   RIOPS      RBW"
print "                                             MB/s             MB/s"
print "=================================================================="
for j in sorted(jobs.values(), key=lambda x: x.miops, reverse=True):
	dt = float(j.dt)
	if j.jobid != "aggr":
		print "%-10s %-8s %-5s %6d %6d %9.2f %6d %9.2f" % (j.jobid.split(".")[0], j.owner, len(j.nodelist), j.miops/dt, j.wiops/dt, (j.wbw/dt)/1000000.0, j.riops/dt, (j.rbw/dt)/1000000.0)
