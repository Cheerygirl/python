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
INIdata = [[],[]]
File = 'e:/code/JY/20161102term/ini5000'
FileList = os.listdir(File)
for line in FileList:
    FilePath = File + '/' + line
    print line,FilePath
    with open(FilePath) as Fil:
        for li in Fil:
            li = li.rstrip()
            chainname = re.findall('\((.*),',li)
            term = re.findall(',(.*)\)', li)
            INIdata[0].append(chainname[0])
            INIdata[1].append(term[0])
INIdata = pd.DataFrame(INIdata,index=['chain_name','credit_term']).T

ADdata = [[],[]]
File = 'e:/code/JY/20161102term/ad5000'
FileList = os.listdir(File)
for line in FileList:
    FilePath = File + '/' + line
    print line,FilePath
    with open(FilePath) as Fil:
        for li in Fil:
            li = li.rstrip()
            chainname = re.findall('\((.*),',li)
            term = re.findall(',(.*)\)', li)
            ADdata[0].append(chainname[0])
            ADdata[1].append(term[0])
ADdata = pd.DataFrame(ADdata,index=['chain_name','credit_term']).T
INIdata.to_csv('e:/code/JY/output/20161102initerm5000.csv',encoding='utf-8',index=False)
ADdata.to_csv('e:/code/JY/output/20161102adterm5000.csv',encoding='utf-8',index=False)