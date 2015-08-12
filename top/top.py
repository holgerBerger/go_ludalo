#!/usr/bin/env python

import time,sys

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
		self.rbw += node.rbw
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
		timestamp=self.getLatestTs()
		for e in self.getEntries(timestamp):
			node = e["nid"]
			if node == "aggr": continue
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
			if jobid not in jobs:
				jobs[jobid]=jobstats(jobid)
				for nid in j["nids"].split(","):
					jobs[jobid].nodelist.append(nid)
					jobs[jobid].start = j["start"]
					jobs[jobid].end = j["end"]
					jobs[jobid].owner = j["owner"]
					jobs[jobid].cmd = j["cmd"]
		return jobs

	# go over all jobs in list, and add all stats of nodes in job from start to end
	# to it (end==-1 is covered, so can be used for running jobs as well)
	def accumulateJobStats(self, jobs):
		fsjobs=set()
		for j in jobs:
			for nid in jobs[j].nodelist:
				if jobs[j].end == -1:
					end = time.time()
				else:
					end = jobs[j].end
				start = jobs[j].start

				for e in self.perfcoll.find({"$and": [{"nid": nid}, {"ts": {"$gt": start}}, {"ts": {"$lt": end}} ] }):
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

		localjobs={}
		for j in fsjobs:
			localjobs[j]=jobs[j]
		return localjobs


# print TOP like list of jobs, with current rates
def printTopjobs(fsname, key):
	fs = filesystem(DBHOST, fsname)
	(timestamp, nodes) = fs.currentNodesstats()
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
	print "JOBID      OWNER    NODES  MIOPS   WIOPS     WBW   RIOPS      RBW"
	print "                                             MB/s             MB/s"
	print "=================================================================="
	for j in sorted(jobs.values(), key=sortf, reverse=True):
		dt = float(j.dt)
		print "%-10s %-8s %-5s %6d %6d %9.2f %6d %9.2f" % (j.jobid.split(".")[0], j.owner, len(j.nodelist), j.miops/dt, j.wiops/dt, (j.wbw/dt)/1000000.0, j.riops/dt, (j.rbw/dt)/1000000.0)


# print TOP like list of jobs, with absolute values over runtime
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

	print "JOBID      OWNER    NODES  MIOPS   WIOPS     WBW   RIOPS      RBW"
	print "                                             GB               GB"
	print "=================================================================="
	for j in sorted(jobs.values(), key=sortf, reverse=True):
		print "%-10s %-8s %-5s %6d %6d %9.2f %6d %9.2f" % (j.jobid.split(".")[0], j.owner, len(j.nodelist), j.miops, j.wiops, j.wbw/1000000000.0, j.riops, j.rbw/1000000000.0)


#printTopjobs(sys.argv[1], sys.argv[2])
printJobSummary(sys.argv[1], sys.argv[2])
