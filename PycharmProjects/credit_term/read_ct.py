#!/usr/bin/python
# -*- coding: gbk -*

#import library
import pandas as pd
import numpy as np
import os
import sys
import re
import json

#################data extract from output############
def dataext1(Path):
    JYdata = pd.DataFrame()
    FileList = os.listdir(Path)
    i = 0
    for line in FileList:
        FilePath = Path + '/' + line
        print FilePath
        with open(FilePath) as Fil:
            for li in Fil:
                print li
                JYdata = JYdata.append(pd.read_json(li))
                i = i + 1
    print JYdata
    return JYdata.T

def dataext2(Path):
    ChainName = []
    Term = []
    FileList = os.listdir(Path)
    for line in FileList:
        FilePath = Path + '/' + line
        print FilePath
        with open(FilePath) as Fil:
            for li in Fil:
                li = li.rstrip()
                chainname = re.findall('\((.*),', li)
                term = re.findall(',(.*)\)', li)
                ChainName.append(chainname[0])
                Term.append(float(term[0]))
    ChainTerm = pd.DataFrame(Term,index=ChainName,columns=['Term'])
    return ChainTerm

#data File
INIFile = 'e:/code/JY/20161104term/initerm'
ADFile = 'e:/code/JY/20161104term/adterm'
#data extract
INITerm = dataext1(INIFile)
ADTerm = dataext2(INIFile)
#data merge
TermCom = INITerm.T
TermCom['ADTerm'] = ADTerm.Term
TermCom.to_csv('e:/code/JY/output/20161104cluster-1000term.csv',encoding='utf-8')