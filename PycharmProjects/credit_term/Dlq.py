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
    Type = []
    Dlq = []
    FileList = os.listdir(Path)
    for line in FileList:
        FilePath = File + '/' + line
        with open(FilePath) as Fil:
            for li in Fil:
                Type.append(line)
                Dlq.append(float(li))
    TypeDlq = pd.DataFrame(Dlq,index=Type,columns=['DlqRio'])
    return TypeDlq

#data File
File = 'e:/code/JY/20161111dlq'

#data extract
DlqRio = dataext(File)

DlqRio.to_csv('e:/code/JY/output/20161111cluster-1000dlq.csv',encoding='utf-8',index=True)

