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
def dataext(Path):
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

#data File
INIFile = 'e:/code/JY/20161104term/initerm'
#ADFile  = 'e:/code/JY/20161104term/adterm'

#data extract
INITerm = dataext(INIFile)
#ADTerm = dataext(ADFile)

#data merge
TermCom = INITerm.T

TermCom.to_csv('e:/code/JY/output/20161104cluster-1000initerm.csv',encoding='gbk')



