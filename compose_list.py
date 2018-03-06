#!/usr/bin/env python

import os
import sys
import re
import json
import ast
import subprocess
import bisect
import time
import glob

class composeNodeList(object):
    """ Example class that compose node list from remote machine"""
    def __init__(self,mname):
        print "machine name: ",mname
        self.machineName=mname
        self.serverDict={}
        self.nodeList={}
        self.nodeList[self.machineName]={}
        self.serverStructure='central' # choose from: central, clustered 
        self.nodeListFileName=mname+'-NodeList-'+self.serverStructure+'.txt'

        f = open(self.nodeListFileName, 'w')
        f.close()

    def setServerList(self):
        self.serverDict['yellowstone_central']=['yellowstone']
        self.serverDict['yellowstone_clustered']=['ysadmin1','ysadmin2','ysadmin3','ysadmin4','ysadmin5','ysadmin6'] 
        self.serverDict['cheyenne_central']=['Cheyenne Rack 1','Cheyenne Rack 2','Cheyenne Rack 3','Cheyenne Rack 4','Cheyenne Rack 5','Cheyenne Rack 6','Cheyenne Rack 7','Cheyenne Rack 8','Cheyenne Rack 9','Cheyenne Rack 10','Cheyenne Rack 11','Cheyenne Rack 12','Cheyenne Rack 13','Cheyenne Rack 14'] 
        print self.serverDict

    def buildExpectScript(self):
        f = open('expect_ysadmin', 'w')
        message="""#!/usr/bin/expect
set timeout 1
set ip [lindex $argv 0]
spawn telnet $ip 13900
expect "'^]'."
send -- "cd yellowstone\\r"
expect "$"
send -- "ls\\r"
expect eof
"""
        f.write(message)
        f.close()        

        f = open('expect_glcentral', 'w')
        message="""#!/usr/bin/expect
set timeout 1
set ip [lindex $argv 0]
spawn telnet ganglia 13900
expect "'^]'."
send -- "cd $ip\\r"
expect "$"
send -- "ls\\r"
expect eof
"""
        f.write(message)
        f.close()

    def findYSNode(self,sname):
        #scommand="expect expect_ysadmin "+sname+" 13900"
        scommand="expect expect_glcentral "+sname+" 13900"
        print scommand
        sout=runSysCommand(scommand)
        self.nodeList[self.machineName][sname]=[] 
        for itxt in sout:
            if len(itxt)==10 and ("d ys" in itxt) and ("d ysmgt" not in itxt):
                self.nodeList[self.machineName][sname].append("ys"+itxt[4:8])
            else:
                pass

    def findCHNode(self,sname):
        scommand="expect expect_glcentral \""+sname.split()[0]+"\\\" \\\""+sname.split()[1]+"\\\" \\\""+sname.split()[2]+"\""+" 13900"
        print scommand
        sout=runSysCommand(scommand)
        self.nodeList[self.machineName][sname]=[]
        for itxt in sout:
            if ("d r" in itxt):
                self.nodeList[self.machineName][sname].append(itxt[2:-2])
            else:
                pass

    def writeout(self):
        f = open(self.nodeListFileName, 'a+')
        f.write(str(self.nodeList))
        f.close()

class composeRrdList(object):
    """ Example class that compose rrd list from remote machine"""
    def __init__(self,mname):
        print "machine name: ",mname
        self.machineName=mname
        self.CHTestServer='Cheyenne Rack 1'
        self.CHTestNode='r1i0n0'
        self.YSTestServer='yellowstone'
        self.YSTestNode='ys0101'
        self.metricListFileName=mname+'-RRDsList.txt'
        self.findMetric="findMetric_"+self.machineName
        self.metricList=[]

        f = open(self.metricListFileName, 'w')
        f.close()

    def buildExpectScript(self):
        f = open('expect_chrack_metric', 'w')
        message="""#!/usr/bin/expect
set timeout 1
spawn telnet ganglia 13900
expect "'^]'."
send -- "cd  """+'%s'+"""\\r"
expect "$"
send -- "ls\\r"
expect eof
"""
        f = open('expect_chrack_metric', 'w')
        path=self.CHTestServer.split()[0]+"\\\" \\\""+self.CHTestServer.split()[1]+"\\\" \\\""+self.CHTestServer.split()[2]+"/"+self.CHTestNode
        f.write(message%path)
        f.close()

        f = open('expect_yellowstone_metric', 'w')
        path=self.YSTestServer+"/"+self.YSTestNode
        f.write(message%path)
        f.close()

    def findMetric_cheyenne(self):
        scommand="expect expect_chrack_metric 13900"
        sout=runSysCommand(scommand)
        self.metricList=[]
        for itxt in sout:
            #print itxt
            if (".rrd" in itxt) and (" Te" not in itxt):
                self.metricList.append(itxt[2:-6])
            else:
                pass
        self.metricList.sort() 
        print self.metricList," in findMetric_cheyenne\n" 

    def findMetric_yellowstone(self):
        scommand="expect expect_yellowstone_metric 13900"
        sout=runSysCommand(scommand)
        self.metricList=[]
        for itxt in sout:
            #print itxt
            if (".rrd" in itxt):
                self.metricList.append(itxt[2:-6])
            else:
                pass
        self.metricList.sort()


    def writeout(self):
        f = open(self.metricListFileName, 'a+')
        print str(self.metricList)," in writeout\n" 
        f.write(str(self.metricList))
        f.close()

class buildEmptyPcpFolder(object):
    """ Example class that compose empty pcp archive folder"""
    def __init__(self,mname):
        print "machine name: ",mname
        self.machine=mname
        self.serverStructure='central' # choose from: central, clustered
        self.nodeListFile=self.machine+'-NodeList-'+self.serverStructure+'.txt'
        self.outputDir="/home/xdmod/data/pcp-logs/"+self.machine
        self.buildDir="buildPCPArchiveDir_"+self.machine
        self.nodeDict={}

    def readNodeDictionary(self):
        #print "fetch {ysadmin1:[node1, node2,...],ysadmin2:[node1, node2,...]}"
        f = open(self.nodeListFile, 'r')
        md={}
        md=eval(f.readline())
        for tm in md[self.machine]:
            self.nodeDict[tm]=md[self.machine][tm]

        #self.nodeDict={}
        #self.nodeDict['Cheyenne Rack 1']=['r1i0n1']
        #print self.nodeDict

    def buildPCPArchiveDir_yellowstone(self):
        #build yellowstone/ysXXXX-ib in /path/to/pcp-logs/
        if os.path.exists(self.outputDir):
            sys.exit("Before continue, please backup: "+self.outputDir)
        if os.path.exists(self.outputDir+"-empty"):
            print "copy from the empty dir\n"
        else:
            print "create the empty dir\n"
            os.system('mkdir '+self.outputDir+'-empty')
            for serverName in self.nodeDict.keys():
                for nodeName in self.nodeDict[serverName]:
                    os.system('mkdir '+self.outputDir+'-empty/'+nodeName+'-ib')
        os.system('cp -r '+self.outputDir+'-empty '+self.outputDir)
        pass

    def buildPCPArchiveDir_cheyenne(self):
        #build cheyenne/rXiYnZ in /path/to/pcp-logs/
        if os.path.exists(self.outputDir):
            sys.exit("Before continue, please backup: "+self.outputDir)
        if os.path.exists(self.outputDir+"-empty"):
            print "copy from the empty dir\n"
        else:
            print "create the empty dir\n"
            os.system('mkdir '+self.outputDir+'-empty')
            for serverName in self.nodeDict.keys():
                for nodeName in self.nodeDict[serverName]:
                    #print "nodename=",nodeName
                    os.system('mkdir '+self.outputDir+'-empty/'+nodeName)
        os.system('cp -r '+self.outputDir+'-empty '+self.outputDir)
        pass

def runSysCommand(scommand):
    tmpscreenout=subprocess.Popen(scommand,
                 shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                 stderr=subprocess.STDOUT, close_fds=True)
    #tmp_array=tmpscreenout
    tmp_array=tmpscreenout.stdout.readlines()
    return tmp_array 

if __name__ == "__main__":
    if(len(sys.argv) < 2):
        print("usage %s machine_name" % (sys.argv[0]))
        sys.exit(1)

    machineName=sys.argv[1]

    """
    p = composeNodeList(machineName)
    p.setServerList()
    p.buildExpectScript()
    for serverName in p.serverDict[p.machineName+'_'+p.serverStructure]:
        try:
            #p.findYSNode(serverName)
            p.findCHNode(serverName)
            pass
        except:
            print ("Can not find node list at server: %s" % (serverName))
    p.writeout()

    q = composeRrdList(machineName)
    q.buildExpectScript()
    methodToCall = getattr(q, q.findMetric)
    methodToCall()
    q.writeout() 
    """

    r = buildEmptyPcpFolder(machineName)
    r.readNodeDictionary()
    methodToCall = getattr(r, r.buildDir)
    methodToCall()     
