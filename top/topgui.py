#!/usr/bin/env python


# IDEEN
#   maxima in DB speichern, und persistent machen
#   schnitte/knoten bilden, und text rot faerben/fett wenn ueber schnitt
#   job anklichbar im bereich der ersten schriftzeile -> fenster mit jobs details wie
#     aggregierte werte und zeitlicher verlauf, anzahl genutzter OSTs 


import sys
from PySide import QtCore, QtGui
import top

FONTSIZE=11
FILESYSTEM="alnec"
FILESYSTEM="nobnec"

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
        



#class Window(QtGui.QWidget):
    #def __init__(self):
        #super(Window, self).__init__()
        #self.W = 1500
        #self.H = 1100
#
        #self.so = StackedCoordinates()
#
        #vbox = QtGui.QVBoxLayout()
        #vbox.addStretch(1)
        #vbox.addWidget(self.so)
        #self.setLayout(vbox)  
#
        #self.setGeometry(300, 300, self.W, self.H)
        #self.setWindowTitle('ludalo top')
        #
        #self.show()

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


class Window(QtGui.QWidget):
    def __init__(self):
        super(Window, self).__init__()
        self.W = 1400
        self.H = 1000
        
        #self.setMinimumSize(self.W, self.H)
        self.initUI()
        timer = QtCore.QTimer(self)
        timer.timeout.connect(self.doUpdate)
        timer.start(1000*10)
        self.maxima = [0, 0, 0, 0, 0, 0]
        
        
    def initUI(self):      

        # self.showFullScreen()
        self.setMinimumHeight(800)
        self.setMinimumWidth(1000)
        self.setGeometry(300, 300, self.W, self.H+50)
        self.setWindowTitle('ludalo top')
        self.show()

    def doUpdate(self):
        self.update()

    # get a sorted list of Job classes, giving top N nodes
    #  in size, in meta iops, int iops, in bw
    # and a dict containing dicts giving shares of total
    # for those 4 metrics
    # and a tuple with (totalnodes, totalmeta, totalrqs, totalbw)
    def getJobList(self, N=5):
        self.fs = top.filesystem(top.DBHOST, FILESYSTEM)
        (timestamp, nodes) = self.fs.currentNodesstats()
        jobs = self.fs.mapNodesToJobs(timestamp, nodes)
        joblist = {}
        # convert in local format 
        for j in jobs:
            cj = jobs[j]
            # FIXME SAMPLE INTERVAL
            joblist[j] = Job(j,cj.owner, len(cj.nodelist), cj.miops/10, cj.wiops/10, cj.wbw/10, cj.riops/10, cj.rbw/10 )
            
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

    def paintEvent(self, event):
        geometry = self.geometry()
        self.W = geometry.width()
        self.H = geometry.height()-100   # space for lower bound

        # get data from DB
        (jobs,shares,totals) = self.getJobList(5) 

        # print "paintEvent"
        qp = QtGui.QPainter()

        qp.begin(self)


        off = [0, 0, 0, 0]
        lastline = [ QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(0,0), QtCore.QPoint(self.W,0) ]
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

        # fill remainder at bottom
        
        newline = [ QtCore.QPoint(0,self.H), QtCore.QPoint(self.W,self.H) ]
        points=[]
        points.extend(list(reversed(lastline)))
        points.extend(newline)  
        brush = QtGui.QBrush(QtGui.QColor(180,180,180))
        qp.setBrush(brush)
        pen = QtGui.QPen(QtGui.QColor(200,200,200))
        qp.setPen(pen)
        qp.drawPolygon(points, QtCore.Qt.OddEvenFill)

        # print totals

        pen = QtGui.QPen(QtGui.QColor(0,0,0,255))
        qp.setPen(pen)
        brush = QtGui.QBrush(QtGui.QColor(0,0,0))
        qp.setBrush(brush)
        qp.setFont(QtGui.QFont('Decorative', FONTSIZE+2))
        qp.drawText(10, self.H+30, FILESYSTEM+" TOTAL")
        qp.drawText(250, self.H+30, str(totals[0])+" nodes")
        qp.drawText(2 + 3 * self.W/8, self.H+30, str(totals[1])+" meta ops/s")
        qp.drawText(2 + 5 * self.W/8, self.H+30, str(totals[2]+totals[3])+" iops/s")
        (bw,unit) = normalize_bw(totals[4]+totals[5])
        qp.drawText(2 + 7 * self.W/8, self.H+30, "%6.2f" % (bw)+unit)

        # search maxima
        
        changed = False
        for i in range(0,6):
            if totals[i] > self.maxima[i]:
                self.maxima[i] = totals[i]
                changed = True
        # update DB
        if changed:
            self.fs.writeFSMaxima(self.maxima)

        # print maxima

        pen = QtGui.QPen(QtGui.QColor(0,0,0,255))
        qp.setPen(pen)
        brush = QtGui.QBrush(QtGui.QColor(0,0,0))
        qp.setBrush(brush)
        qp.setFont(QtGui.QFont('Decorative', FONTSIZE+2))
        qp.drawText(10, self.H+50, FILESYSTEM+" MAXIMUM")
        qp.drawText(250, self.H+50, str(self.maxima[0])+" nodes")
        qp.drawText(2 + 3 * self.W/8, self.H+50, str(self.maxima[1])+" meta ops/s")
        qp.drawText(2 + 5 * self.W/8, self.H+50, str(self.maxima[2]+self.maxima[3])+" iops/s")

        (bw,unit) = normalize_bw(self.maxima[4]+self.maxima[5])
        qp.drawText(2 + 7 * self.W/8, self.H+50, "%6.2f" % (bw)+unit)

        qp.end()
        
    def drawText(self, event, qp):
        qp.setPen(QtGui.QColor(168, 34, 3))
        qp.setFont(QtGui.QFont('Decorative', 10))
        qp.drawText(event.rect(), QtCore.Qt.AlignCenter, self.text) 


# rgb heatmap from stackoverflow 
# http://stackoverflow.com/questions/20792445/calculate-rgb-value-for-a-range-of-values-to-create-heat-map
def rgb(minimum, maximum, value):
    minimum, maximum = float(minimum), float(maximum)
    ratio = 2 * (value-minimum) / (maximum - minimum)
    b = int(max(0, 255*(1 - ratio)))
    r = int(max(0, 255*(ratio - 1)))
    g = 255 - b - r
    # print r,g,b
    return r, g, b, 128

if __name__ == '__main__':

    app = QtGui.QApplication(sys.argv)
    window = Window()
    window.show()
    sys.exit(app.exec_())
