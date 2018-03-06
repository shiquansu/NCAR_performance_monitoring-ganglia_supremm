#!/usr/bin/env python
import argparse #yum install python-argparse.noarch
import yaml #take care of regular expression

import os
import sys
import re
import json
import ast
import subprocess
import bisect
import time
import datetime
import glob


parser = argparse.ArgumentParser(description='Script to refine the pcp archives. It query the rrds again to pick up the pcp missing in the latest run. Example: python refine_nodelist.py -d 20170313 -m cheyenne -r 2 would query the date 2017 March 13, on Cheyenne machine, the second time to run the extract_rrd')
parser.add_argument('-d','--date',  dest='dquery', help='which date to query', required=True, type=str)
parser.add_argument('-m','--machine',  dest='mname', help='which machine to query', required=True, type=str)
parser.add_argument('-r','--round',  dest='rnumber', help='round number of attempt', required=True, type=int)
args = parser.parse_args()

os.system('grep "Cannot collect: " screenoutput'+args.dquery+args.mname+str(args.rnumber-1)+'.txt > ss.txt')
os.system('chmod 777 ss.txt')
serverDict={}
serverDict['cheyenne_central']=['Cheyenne Rack 1','Cheyenne Rack 2','Cheyenne Rack 3','Cheyenne Rack 4','Cheyenne Rack 5','Cheyenne Rack 6','Cheyenne Rack 7','Cheyenne Rack 8','Cheyenne Rack 9','Cheyenne Rack 10','Cheyenne Rack 11','Cheyenne Rack 12','Cheyenne Rack 13','Cheyenne Rack 14']
serverDict['yellowstone_central']=['yellowstone']
nodeDict={}
nodeDict[args.mname]={}
for s in serverDict[args.mname+'_central']:
    nodeDict[args.mname][s]=[]

f = open('ss.txt', 'r')
for line in f:
    #for cheyenne
    if ('cheyenne'==args.mname):
        sn=line.split(',')[1].split(' ')[3]
        nn=line.split(',')[2].split(' ')[1][:-1]
        #print sn, nn
        nodeDict['cheyenne']['Cheyenne Rack '+sn].append(nn)
    
f = open(args.mname+'-NodeList-central-'+args.dquery+'.txt', 'w')
f.write(str(nodeDict))
f.close()
