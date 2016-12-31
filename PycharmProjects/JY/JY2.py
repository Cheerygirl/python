#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np
import random

#################data extract from output############
FilePath = 'e:/code/JY/20161029/overdue.json'
JYdata = pd.DataFrame()
i = 0
with open(FilePath) as Fil:
    for line in Fil:
#        lines = [line] + list(itertools.islice(Fil,0))
#        jfile = json.loads(''.join(lines))
        JYdata[i] = pd.read_json(line,typ='series')
        i = i + 1
#print  jfile.keys()
#print  jfile['bill_amount']
JYdata = JYdata.T

JYdata['Rio'] = JYdata['overdueAmount']/JYdata['amount']
JYdata.to_csv('e:/code/JY/20161029/overdueJY.csv',encoding='utf-8',index=False)






