#!/usr/bin/python
# -*- coding: gbk -*

#import library
import pandas as pd
import numpy as np
import os
import sys
import re

#################data extract from output############

ChainName = []
Term = []
FileList = os.listdir(Path)
for line in FileList:
    FilePath = Path + '/' + line
    print FilePath
    if(os.path.isdir(FilePath)):
        with open(FilePath) as Fil:
            for li in Fil:
                li = li.rstrip()
                chainname = re.findall('\((.*),', li)
                term = re.findall(',(.*)\)', li)
                    ChainName.append(chainname[0])
                    Term.append(term[0])
    ChainTerm = pd.DataFrame(Term,index=ChainName)
    return ChainTerm

#data File
INIFile = 'e:/code/JY/20161102term/ini5000'
ADFile  = 'e:/code/JY/20161102term/ad5000'

#data extract
INITerm = dataext(INIFile)
ADFile = dataext(ADFile)

#data merge


