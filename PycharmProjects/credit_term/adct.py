#!/usr/bin/python
# -*- coding: gbk -*

#import library
import pandas as pd
import numpy as np
import os
import sys
import re

#################data extract from output############
def dataext(Path):
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
INIFile = 'e:/code/JY/20161104term/adterm'
#ADFile  = 'e:/code/JY/20161102term/ad5000'

#data extract
INITerm = dataext(INIFile)
#ADTerm = dataext(ADFile)

#data merge
TermCom = INITerm
#TermCom['ADTerm'] = ADTerm
TermCom.to_csv('e:/code/JY/output/20161104cluster-1000adterm.csv',encoding='gbk')



