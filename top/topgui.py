#!/usr/bin/env python


# IDEEN
#   job anklichbar im bereich der ersten schriftzeile -> fenster mit jobs details wie
#     aggregierte werte und zeitlicher verlauf, anzahl genutzter OSTs 


import sys, time
from PySide import QtCore, QtGui
import top


INTERVAL=10
FONTSIZE=11        # FIXME some places need check for this size, is a bit hardcoded

FILESYSTEMLIST = [ "alnec", "nobnec" ]



# helper class
class Job(object):
    def __init__(self,name,owner,nodes,meta,wrqs,wbw,rrqs,rbw):
        self.name  = name
        self.owner = owner
        self.nodes = nodes
        self.meta  = meta
        self.wrqs   = wrqs 
        self.rrqs   = rrqs
        #self.readratio = float(rrqs)/float(wrqs)
        self.wbw    = wbw
        self.rbw    = rbw
    def __repr__(self):
        return self.name + " " + self.owner+ " " + str(self.nodes)
        


# Widget to show last minutes of loaf of a filesystem and name 202x102 pixels
class Filesystem(QtGui.QWidget):
    def __init__(self, name):
        super(Filesystem, self).__init__()
        #self.setGeometry(0, 0, 100, 100)
        self.setMinimumSize(203, 102)
        self.setMaximumSize(203, 102)
        self.name = name
        self.topfs = top.filesystem("localhost",self.name)
        print name
        timer = QtCore.QTimer(self)
        timer.timeout.connect(self.doUpdate)
        timer.start(1000*INTERVAL)
        #self.setMouseTracking(True)
        self.doUpdate()
        self.selected = False

    def doUpdate(self):
        self.getData()
        self.update()

    def mousePressEvent(self, event):
        self.parentWidget().changeFS(self.name)

    def getData(self):
        now = self.topfs.getLatestTs()
        self.timevalues = self.topfs.getFSvalues(now-100*INTERVAL, now) 

    def select(self):
        self.selected = True
        self.update()

    def deselect(self):
        self.selected = False
        self.update()

    def paintEvent(self, event):
        qp = QtGui.QPainter()

        # box + titel
        qp.begin(self)
        if self.selected:
            qp.setPen(QtGui.QPen(QtGui.QColor("black")))
        else:
            qp.setPen(QtGui.QPen(QtGui.QColor("white")))
        qp.setBrush(QtGui.QBrush(QtGui.QColor("white")))
        qp.drawRect(0,0,203,102) # border
        qp.setBrush(QtGui.QBrush(QtGui.QColor(0,0,0,0)))
        qp.drawRect(1,1,202,101) # border
        qp.setPen(QtGui.QPen(QtGui.QColor("black")))
        qp.setBrush(QtGui.QBrush(QtGui.QColor("black")))
        qp.setFont(QtGui.QFont('Decorative', FONTSIZE))
        qp.drawText(2,FONTSIZE+3,self.name)

        # draw data
        maxima = self.topfs.readFSMaxima()
        # take maximum of IOPS counters ratio to max as FS load
        x = 3
        for i in sorted(self.timevalues):
            ratio_meta = float(self.timevalues[i]["miops"])/float(maxima[1])
            ratio_wr   = float(self.timevalues[i]["wiops"])/float(maxima[2])
            ratio_rr   = float(self.timevalues[i]["riops"])/float(maxima[3])
            ratio = max(ratio_meta,ratio_rr,ratio_wr)
            setHeatMapColor(qp, 0, 1, ratio)
            qp.drawLine(x, 99, x, 99-min(80,ratio*80.0))    
            qp.drawLine(x+1, 99, x+1, 99-min(80,ratio*80.0))    
            x += 2  # drawn 2 pixels wide

        qp.end()


# main window, embed list of windows on left side, stacked coordinates on right side
class Window(QtGui.QWidget):
    def __init__(self):
        super(Window, self).__init__()
        self.W = 1500
        self.H = 1100

        self.so = StackedCoordinates(FILESYSTEMLIST[0])
        self.fslist = {}
        for f in FILESYSTEMLIST:
            self.fslist[f]=Filesystem(f)
        self.fslist[FILESYSTEMLIST[0]].select()

        vbox = QtGui.QVBoxLayout()
        vbox.setAlignment(QtCore.Qt.AlignTop)
        for f in self.fslist:
            vbox.addWidget(self.fslist[f])

        hbox = QtGui.QHBoxLayout()
        # hbox.addStretch(1)
        hbox.addLayout(vbox)
        hbox.addWidget(self.so)
        self.setLayout(hbox)  

        self.setGeometry(300, 300, self.W, self.H)
        self.setWindowTitle('ludalo top')

        # self.setStyleSheet("background-color:lightgrey;"); 
        
        self.show()


    # call StackedCoordinates.changeFS
    def changeFS(self, name):
        for f in self.fslist:
            if self.fslist[f].selected:
                self.fslist[f].deselect()
        self.fslist[name].select()
        self.so.changeFS(name)


# stacked coordinates widget, show jobs and load info of FS
# call changeFS to change FS and update
class StackedCoordinates(QtGui.QWidget):
    def __init__(self, filesystem):
        super(StackedCoordinates, self).__init__()
        self.W = 1400
        self.H = 1000
        
        #self.setMinimumSize(self.W, self.H)
        self.maxima = [0, 0, 0, 0, 0, 0]
        self.fsname = filesystem

        # mouseras contains tuples with y coordinates for jobnames
        self.mouseareas={}
        self.mouseareas["nodes"] = {}
        self.mouseareas["meta"]  = {}
        self.mouseareas["rqs"]   = {}
        self.mouseareas["bw"]    = {}

        self.topfs = top.filesystem("localhost",self.fsname)

        self.initUI()

        # start timer for auto update  # FIXME move to Window and Update all at once with one timer?
        timer = QtCore.QTimer(self)
        timer.timeout.connect(self.doUpdate)
        timer.start(1000*INTERVAL)
        
    # change FS and update
    def changeFS(self, fsname):
        self.fsname = fsname 
        self.topfs = top.filesystem("localhost",self.fsname)
        self.doUpdate()
        
    def initUI(self):      
        # self.showFullScreen()
        self.setMinimumHeight(800)
        self.setMinimumWidth(1000)
        self.setGeometry(300, 300, self.W, self.H+50)
        self.setWindowTitle('ludalo top')
        self.doUpdate()
        self.show()

    # map mouse position to job
    def mousePressEvent(self, e):
        x,y = e.x(), e.y()
        if x > 0 and x < 2 * self.W/8:
            col = "nodes"
        elif x > 3 * self.W/8 and x < 4 * self.W/8:
            col = "meta"
        elif x > 5 * self.W/8 and x < 6 * self.W/8:
            col = "rqs"
        elif x > 7 * self.W/8 and x < 8 * self.W/8:
            col = "bw"
        else:
            return
    
        job = ""
        for j in self.mouseareas[col]:
            yu, yl = self.mouseareas[col][j]
            if y>yu and y<yl:
                job = j
                break
        else:
            return
        
        # print "show details about job", job
        job = self.topfs.getOneRunningJob(job)
        if job != None:
            jobs = {job.jobid: job}
            jobdata = self.topfs.accumulateJobStats(jobs)
            self.tl = TimeLine(jobdata[job.jobid])


    # get data and start redraw
    def doUpdate(self):
        self.getData()
        self.update()

    # get a sorted list of Job classes, giving top N nodes
    #  in size, in meta iops, int iops, in bw
    # and a dict containing dicts giving shares of total
    # for those 4 metrics
    # and a tuple with (totalnodes, totalmeta, totalrqs, totalbw)
    def getJobList(self, N=5):
        self.fs = top.filesystem(top.DBHOST, self.fsname)
        (timestamp, nodes) = self.fs.currentNodesstats()
        jobs = self.fs.mapNodesToJobs(timestamp, nodes)
        joblist = {}
        # convert in local format 
        for j in jobs:
            cj = jobs[j]
            joblist[j] = Job(j,cj.owner, len(cj.nodelist), cj.miops/INTERVAL, cj.wiops/INTERVAL, cj.wbw/INTERVAL, cj.riops/INTERVAL, cj.rbw/INTERVAL )
            
        totalnodes= 0
        totalmeta = 0
        totalwrqs  = 0
        totalrrqs  = 0
        totalwbw   = 0
        totalrbw   = 0
        for j in joblist:
            totalnodes += joblist[j].nodes
            totalmeta  += joblist[j].meta
            totalwrqs  += joblist[j].wrqs
            totalrrqs  += joblist[j].rrqs
            totalwbw   += joblist[j].wbw
            totalrbw   += joblist[j].rbw

        totals = (totalnodes, totalmeta, totalwrqs, totalrrqs, totalwbw, totalrbw)

        # idea: make a list of
        #  N largest nodes
        #  N nodes doing most metadata
        #  N nodes doing most rqs
        #  N nodes doing most BW
        # if a node is doubled, take next from local list
        # this summed list will be display (double/dashed line inbetween??)
        toplist=[]
        for list in [
            sorted(joblist.values(), key=lambda x: x.nodes, reverse=True),
            sorted(joblist.values(), key=lambda x: x.meta, reverse=True),
            sorted(joblist.values(), key=lambda x: x.wrqs+x.rrqs, reverse=True),
            sorted(joblist.values(), key=lambda x: x.wbw+x.rbw, reverse=True),
        ]:
            i=0
            for j in list:
                if j not in toplist: 
                    toplist.append(j)
                    i+=1
                    if i>=N: break

        listnodes=0
        for j in toplist:
            listnodes += j.nodes

        # sort again, so total list os sorted now according to node number
        toplist = sorted(toplist, key=lambda x: x.nodes, reverse=True)

        shares={}
        for j in toplist:
            if totalmeta == 0: totalmeta = 1
            if (totalwrqs+totalrrqs) == 0: totalrrqs = 1
            if (totalwbw+totalrbw) == 0: totalrbw = 1
            shares[j.name]={
                "name":j.name, 
                "owner": j.owner, 
                # "nodes": float(j.nodes)/totalnodes, 
                "nodes": float(j.nodes)/listnodes, 
                "meta": float(j.meta)/totalmeta, 
                "rqs": float(j.wrqs+j.rrqs)/(totalwrqs+totalrrqs), 
                "bw": float(j.wbw+j.rbw)/(totalwbw+totalrbw)
            }

        # get maxima from DB
        self.maxima = self.fs.readFSMaxima()
        #print "from db:", self.maxima

        return (toplist, shares, totals)


    # get data from DB
    def getData(self):
        (self.jobs,self.shares,self.totals) = self.getJobList(5)    # FIXME should be decoupled from redraw!


    # draw everything
    def paintEvent(self, event):
        geometry = self.geometry()
        self.W = geometry.width()
        self.H = geometry.height()-100   # space for lower bound

        (jobs, shares, totals) = (self.jobs, self.shares, self.totals)

        # print "paintEvent"
        qp = QtGui.QPainter()

        qp.begin(self)

        # background 
        qp.setPen(QtGui.QPen(QtGui.QColor("black")))
        qp.drawRect(0, 0, self.W-1, self.H-1+100)

        # polygons

        off = [0, 0, 0, 0]
        lastline = [ QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(0,0), 
                     QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(self.W,0) ]

        # mouseras contains tuples with y coordinates for jobnames
        self.mouseareas={}
        self.mouseareas["nodes"] = {}
        self.mouseareas["meta"]  = {}
        self.mouseareas["rqs"]   = {}
        self.mouseareas["bw"]    = {}

        counter = 1
        for job in jobs:
            j = job.name
            newline = [
                        # size
                        QtCore.QPoint(0 * self.W/8, self.H*shares[j]["nodes"] + off[0]),
                        QtCore.QPoint(2 * self.W/8, self.H*shares[j]["nodes"] + off[0]),
                        # transition
                        QtCore.QPoint(3 * self.W/8, self.H*shares[j]["meta"] + off[1]),
                        # meta
                        QtCore.QPoint(4 * self.W/8, self.H*shares[j]["meta"] + off[1]),
                        # transition
                        QtCore.QPoint(5 * self.W/8, self.H*shares[j]["rqs"] + off[2]),
                        # rqs
                        QtCore.QPoint(6 * self.W/8, self.H*shares[j]["rqs"] + off[2]),
                        # transition
                        QtCore.QPoint(7 * self.W/8, self.H*shares[j]["bw"] + off[3]),
                        # bw
                        QtCore.QPoint(8 * self.W/8, self.H*shares[j]["bw"] + off[3]),
                      ]

            self.mouseareas["nodes"][j] = (off[0], off[0]+self.H*shares[j]["nodes"])
            self.mouseareas["meta"][j]  = (off[1], off[1]+self.H*shares[j]["meta"])
            self.mouseareas["rqs"][j]   = (off[2], off[2]+self.H*shares[j]["rqs"])
            self.mouseareas["bw"][j]    = (off[3], off[3]+self.H*shares[j]["bw"])

            off[0] += self.H*shares[j]["nodes"]
            off[1] += self.H*shares[j]["meta"]
            off[2] += self.H*shares[j]["rqs"]
            off[3] += self.H*shares[j]["bw"]

            points=[]
            points.extend(list(reversed(lastline)))
            points.extend(newline)  

            lastline = newline
	        
            # print counter
            brush = QtGui.QBrush(QtGui.QColor(*rgb(1,len(jobs),len(jobs)-counter+1)))
            qp.setBrush(brush)
            pen = QtGui.QPen(QtGui.QColor(*rgb(1,len(jobs),len(jobs)-counter+1)))
            qp.setPen(pen)
            qp.drawPolygon(points, QtCore.Qt.OddEvenFill)

            # labels
            pen = QtGui.QPen(QtGui.QColor(0,0,0,255))
            qp.setPen(pen)
            qp.setFont(QtGui.QFont('Decorative', FONTSIZE))
            displayed=False
            # name + owner + nodes
            if self.H*shares[j]["nodes"] > FONTSIZE:
                qp.drawText(10, off[0], j.split("-")[0])
                qp.drawText(150, off[0], job.owner)
                qp.drawText(250, off[0], str(job.nodes)+" nodes")
                displayed=True

            # meta
            if self.H*shares[j]["meta"] > FONTSIZE:
                qp.drawText(2 + 3 * self.W/8, off[1], str(job.meta)+" meta ops/s")
                if (not displayed and (self.H*shares[j]["meta"] > FONTSIZE * 2)) or (self.H*shares[j]["meta"] > FONTSIZE * 3):
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE-2))
                    qp.drawText(2 + 3 * self.W/8, off[1]-FONTSIZE-2, j.split("-")[0]+" "+job.owner)
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE))
                    displayed=True

            # rqs
            if self.H*shares[j]["rqs"] > FONTSIZE:
                qp.drawText(2 + 5 * self.W/8, off[2], str(job.wrqs+job.rrqs)+" iops/s")
                if (not displayed and (self.H*shares[j]["rqs"] > FONTSIZE * 2)) or (self.H*shares[j]["rqs"] > FONTSIZE * 3):
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE-2))
                    qp.drawText(2 + 5 * self.W/8, off[2]-FONTSIZE-2, j.split("-")[0]+" "+job.owner)
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE))
                    displayed=True
                if self.H*shares[j]["rqs"] > FONTSIZE * 4:
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE-3))
                    qp.drawText(2 + 5 * self.W/8, off[2]-2*FONTSIZE-2, "read %-1.2f" % (job.rrqs))
                    qp.drawText(2 + 5 * self.W/8, off[2]-3*FONTSIZE-2, "write %-1.2f" % (job.wrqs))
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE))
                
            # bw
            if self.H*shares[j]["bw"] > FONTSIZE:
                (bw,unit) = normalize_bw(job.wbw+job.rbw)
                qp.drawText(2 + 7 * self.W/8, off[3], "%-1.2f" % (bw)+unit)
                if (not displayed and (self.H*shares[j]["bw"] > FONTSIZE * 2)) or (self.H*shares[j]["bw"] > FONTSIZE * 3):
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE-2))
                    qp.drawText(2 + 7 * self.W/8, off[3]-FONTSIZE-2, j.split("-")[0]+" "+job.owner)
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE))
                    displayed=True
                if self.H*shares[j]["bw"] > FONTSIZE * 4:
                    (wbw,wunit) = normalize_bw(job.wbw)
                    (rbw,runit) = normalize_bw(job.rbw)
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE-3))
                    qp.drawText(2 + 7 * self.W/8, off[3]-2*FONTSIZE-2, "read %-1.2f%s" % (rbw,runit))
                    qp.drawText(2 + 7 * self.W/8, off[3]-3*FONTSIZE-2, "write %-1.2f%s" % (wbw,wunit))
                    qp.setFont(QtGui.QFont('Decorative', FONTSIZE))



            counter += 1

        # fill remainder at bottom (in case the X jobs do not fill the 100%)
        
        newline = [ QtCore.QPoint(0,self.H), QtCore.QPoint(self.W,self.H) ]
        points=[]
        points.extend(list(reversed(lastline)))
        points.extend(newline)  
        brush = QtGui.QBrush(QtGui.QColor(180,180,180))
        qp.setBrush(brush)
        pen = QtGui.QPen(QtGui.QColor(200,200,200))
        qp.setPen(pen)
        qp.drawPolygon(points, QtCore.Qt.OddEvenFill)

        # search maxima
        
        changed = False
        for i in range(0,6):
            if totals[i] > self.maxima[i]:
                self.maxima[i] = totals[i]
                changed = True
        # update DB
        if changed:
            self.fs.writeFSMaxima(self.maxima)

        # print totals

        pen = QtGui.QPen(QtGui.QColor(0,0,0,255))
        qp.setPen(pen)
        brush = QtGui.QBrush(QtGui.QColor(0,0,0,0))
        qp.setBrush(brush)
        qp.setFont(QtGui.QFont('Decorative', FONTSIZE+2))
        qp.drawText(10, self.H+30, self.fsname+" TOTAL")
        setHeatMapColor(qp, 0, self.maxima[0],totals[0])
        qp.drawText(250, self.H+30, str(totals[0])+" nodes")
        s = 98.0*(float(totals[0])/float(self.maxima[0]))
        qp.drawRect(250-20, self.H+100-s-1, 10, s)

        # meta + bar
        setHeatMapColor(qp, 0, self.maxima[1],totals[1])
        qp.drawText(2 + 3 * self.W/8, self.H+30, str(totals[1])+" meta ops/s")
        s = 98.0*(float(totals[1])/float(self.maxima[1]))
        qp.drawRect(2 + 3 * self.W/8-20, self.H+100-s-1, 10, s)

        qp.setFont(QtGui.QFont('Decorative', (FONTSIZE+1)))

        # write iops + bar
        setHeatMapColor(qp, 0, self.maxima[2],totals[2])
        qp.drawText(2 + 5 * self.W/8, self.H+20, str(totals[2])+" write iops/s")
        s = 98.0*(float(totals[2])/float(self.maxima[2]))
        qp.drawRect(2 + 5 * self.W/8-30, self.H+100-s-1, 10, s)

        # read iops + bar
        setHeatMapColor(qp, 0, self.maxima[3],totals[3])
        qp.drawText(2 + 5 * self.W/8, self.H+40, str(totals[3])+" read iops/s")
        s = 98.0*(float(totals[3])/float(self.maxima[3]))
        qp.drawRect(2 + 5 * self.W/8-20, self.H+100-s-1, 10, s)

        # write BW + bar
        setHeatMapColor(qp, 0, self.maxima[4],totals[4])
        (bw,unit) = normalize_bw(totals[4])
        qp.drawText(2 + 7 * self.W/8, self.H+20, "write %6.2f" % (bw)+unit)
        s = 98.0*(float(totals[4])/float(self.maxima[4]))
        qp.drawRect(2 + 7 * self.W/8-30, self.H+100-s-1, 10, s)

        # read BW + bar
        setHeatMapColor(qp, 0, self.maxima[5],totals[5])
        (bw,unit) = normalize_bw(totals[5])
        qp.drawText(2 + 7 * self.W/8, self.H+40, "read %6.2f" % (bw)+unit)
        s = 98.0*(float(totals[5])/float(self.maxima[5]))
        qp.drawRect(2 + 7 * self.W/8-20, self.H+100-s-1, 10, s)

 

        # print maxima

        setBlack(qp)

        qp.setFont(QtGui.QFont('Decorative', FONTSIZE+2))
        qp.drawText(10, self.H+70, self.fsname+" MAXIMUM")
        qp.drawText(250, self.H+70, str(self.maxima[0])+" nodes")
        qp.drawText(2 + 3 * self.W/8, self.H+70, str(self.maxima[1])+" meta ops/s")

        qp.setFont(QtGui.QFont('Decorative', (FONTSIZE+1)))
        qp.drawText(2 + 5 * self.W/8, self.H+65, str(self.maxima[2])+" write iops/s")

        qp.drawText(2 + 5 * self.W/8, self.H+85, str(self.maxima[3])+" read iops/s")

        qp.setFont(QtGui.QFont('Decorative', (FONTSIZE+1)))
        (bw,unit) = normalize_bw(self.maxima[4])
        qp.drawText(2 + 7 * self.W/8, self.H+65, "write %1.2f" % (bw)+unit)
        (bw,unit) = normalize_bw(self.maxima[5])
        qp.drawText(2 + 7 * self.W/8, self.H+85, "read %1.2f" % (bw)+unit)

        qp.end()
        


# widget to show timeline of job
class TimeLine(QtGui.QWidget):
    def __init__(self, job):
        super(TimeLine, self).__init__()
        self.job = job
        self.jobid = job.jobid
        self.setMinimumSize(800, 200)
        self.setWindowTitle('job details for'+self.jobid)
        self.setGeometry(300, 400, 800, 200)
        self.setStyleSheet("background-color:white;"); 
        self.setWindowFlags(QtCore.Qt.Window)
        self.show()

    def paintEvent(self, e):
        qp = QtGui.QPainter()
        qp.begin(self)
        qp.setFont(QtGui.QFont('Decorative', FONTSIZE))
        qp.drawText(10,20,              "Jobid: "+self.jobid)
        qp.drawText(10,20+FONTSIZE*2,   "Owner: "+self.job.owner)
        qp.drawText(10,20+FONTSIZE*4,   "Nodes: "+str(len(self.job.nodelist)))
        qp.drawText(250,20,             "Start  : "+time.ctime(self.job.start))
        if self.job.end==-1:
            qp.drawText(250,20+FONTSIZE*2,  "End    : still running")
            end = time.time()
        else:
            qp.drawText(250,20+FONTSIZE*2,  "End    : "+time.ctime(self.job.end))
            end = self.job.end
        qp.drawText(250,20+FONTSIZE*4,  "Walltime: "+normalize_time(end-self.job.start))
        qp.drawText(550,20,             "Cmd : "+self.job.cmd)
    
        qp.drawText(10,20+FONTSIZE*6,   "Bytes written: "+normalize_size(self.job.wbw))
        qp.drawText(250,20+FONTSIZE*6,  "Bytes read: "+normalize_size(self.job.rbw))
        qp.drawText(550,20+FONTSIZE*6,  "Metadata operations: "+str(self.job.miops))
        qp.drawText(10,20+FONTSIZE*8,   "Write Requests: "+str(self.job.wiops))
        qp.drawText(250,20+FONTSIZE*8,  "Read Requests: "+str(self.job.riops))
        qp.end()


# rgb heatmap from stackoverflow 
# http://stackoverflow.com/questions/20792445/calculate-rgb-value-for-a-range-of-values-to-create-heat-map
def rgb(minimum, maximum, value, t=128):
    minimum, maximum = float(minimum), float(maximum)
    ratio = 2 * (value-minimum) / (maximum - minimum)
    b = int(max(0, 255*(1 - ratio)))
    r = int(max(0, 255*(ratio - 1)))
    g = 255 - b - r
    # print r,g,b
    return r, g, b, t


# reset qp to black
def setBlack(qp):
    pen = QtGui.QPen(QtGui.QColor(0,0,0,255))
    qp.setPen(pen)
    brush = QtGui.QBrush(QtGui.QColor(0,0,0,255))
    qp.setBrush(brush)

# heatmap color helper, no alpha
def setHeatMapColor(qp, min, max, value):
    if value > max: value = max
    pen = QtGui.QPen(QtGui.QColor(*rgb(min, max, value, 255)))
    qp.setPen(pen)
    brush = QtGui.QBrush(QtGui.QColor(*rgb(min, max, value, 255)))
    qp.setBrush(brush)


# helper for time difference normalizaton, returns string
def normalize_time(secs):
    s = 0
    m = 0
    h = 0
    if secs > 60:
        s = secs % 60
        secs /= 60
    if secs > 60:
        m = secs % 60
        h = secs / 60
    return "%2.2d:%2.2d:%2.2d" % (h,m,s)
        

# helper for BW units
def normalize_bw(bw):
        bw = float(bw)
        unit = " B/s"
        if bw > 1000:
            bw /= 1000
            unit = " KB/s"
        if bw > 1000:
            bw /= 1000
            unit = " MB/s"
        if bw > 1000:
            bw /= 1000
            unit = " GB/s"
        return (bw,unit)

# helper for BW units
def normalize_size(bw):
        bw = float(bw)
        unit = " B"
        if bw > 1000:
            bw /= 1000
            unit = " KB"
        if bw > 1000:
            bw /= 1000
            unit = " MB"
        if bw > 1000:
            bw /= 1000
            unit = " GB"
        return "%1.2f %s" % (bw,unit)


# MAIN MESS

if __name__ == '__main__':
    app = QtGui.QApplication(sys.argv)
    window = Window()
    window.show()
    sys.exit(app.exec_())
