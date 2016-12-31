#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
import pandas as pd
import numpy as np
import os
import sys
import re

#################data extract from output############
#Tdata = [[],[]]
#File = 'e:/code/JY/20161102term/ini1000'
#FileList = os.listdir(File)
#for line in FileList:
#    FilePath = File + '/' + line
#    print line,FilePath
#    with open(FilePath) as Fil:
#        for li in Fil:
#            li = li.rstrip()
#            chainname = re.findall('\((.*),',li)
#            term = re.findall(',(.*)\)', li)
#            Tdata[0].append(chainname[0])
#            Tdata[1].append(term[0])
#Tdata = pd.DataFrame(Tdata,index=['chain_name','credit_term']).T
#Tdata.to_csv('e:/code/JY/output/20161102term1000.csv',encoding='utf-8',index=False)

Type = []
Dlq = []
File = 'e:/code/JY/20161102dlq/5000'
FileList = os.listdir(File)
for line in FileList:
    FilePath = File + '/' + line
    print line,FilePath
    with open(FilePath) as Fil:
        for li in Fil:
            Type.append(line)
            Dlq.append(float(li))
JYdata = zip(Type,Dlq)
JYdata = pd.DataFrame(JYdata,columns=['Type','Dlq'])

#IGdata = [[],[]]
#N = 10000
#for i in range(0,N):
#    incent = float(i)/float(N)
#    print incent
#    IGdata[0].append(incent)
#    JYdata.Dlq[JYdata['Dlq'] >= incent]
#    IGdata[i].append()

JYdata.to_csv('e:/code/JY/output/20161102dlq-5000.csv',encoding='utf-8',index=False)
#IGdata.to_csv('e:/code/JY/output/IG.csv',encoding='utf-8',index=False)
