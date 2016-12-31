#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np
import json
import itertools
import re

################data extract form database###################
RAW=('''select seller_group_name,seller_name,buyer_name,
datediff(match_pay_time,bill_time) pay_day,bill_amount
from modelcentre.m_core_bill_match
where m_version = '20160913184422'
and match_pay_time is not null
and datediff(due_time,bill_time)>0
order by buyer_name;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))

#data processing: credit_order&match_pay_time_is_not_null&no_abnormal_data
avgpayday = np.nanmean(rdata.loc[:,'pay_day'])
stdpayday = np.nanstd(rdata.loc[:,'pay_day'])
cldatacd = (rdata.pay_day >= avgpayday-3*stdpayday) | (rdata.pay_day <= avgpayday-3*stdpayday)
cldata = rdata[cldatacd]

rawdata = rdata[['buyer_name','bill_amount','pay_day']]

#################extract data from json###########################
FilePath = 'e:/code/JY/20161029/sample.json'
JYdata = pd.DataFrame()
comparedata = pd.DataFrame(columns=['buyer_name','bill_amount','pay_day'])
i = 0
Chain = []
with open(FilePath) as Fil:
    for line in Fil:
        line = line.rstrip()
        chainname = re.findall('\((.*),\[',line)
        sample = re.findall('\[(.*)\]',line)
        samplef = sample[0].split(', ')
        for js in samplef:
            JYdata[i] = pd.read_json(js, typ='series')
            Chain.append(chainname[0])
            i = i + 1
    JYdata = JYdata.T
    JYdata['chain_name'] = Chain

JYdata.to_csv('e:/code/JY/json.csv',encoding='gbk',index=False)
rawdata.to_csv('e:/code/JY/raw.csv',encoding='gbk',index=False)

rpayday = []
for i in range(0,len(JYdata['chain_name'])):
    rpayday.append(rawdata.pay_day[(rawdata['buyer_name']==JYdata['chain_name'].iloc[i]) & (rawdata['bill_amount'].round(2)==round(JYdata['bill_amount'].iloc[i],2))])
JYdata['rpay_day'] = rpayday
JYdata.to_csv('e:/code/JY/JY.csv',encoding='utf-8',index=False)


