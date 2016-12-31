#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np
import random

#data_extract
#sql extract
RAW=('''select seller_group_name,seller_name,buyer_name,
datediff(match_pay_time,bill_time) pay_day,bill_amount
from modelcentre.m_core_bill_match
where m_version = %(m_version)s
and match_pay_time is not null
and datediff(due_time,bill_time)>0
order by buyer_name;''')

CHAIN=('''select distinct buyer_name
from modelcentre.m_core_bill_match
where m_version = %(m_version)s
and match_pay_time is not null
and datediff(due_time,bill_time)>0;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

#data vesion extract
m_version = '20160913184422'

#data_prepare
rawdata = pd.read_sql(RAW,con=cnx,params={'m_version':m_version})
chainname = pd.read_sql(CHAIN,con=cnx,params={'m_version':m_version})

ChainNum = rawdata['bill_amount'].groupby(rawdata['buyer_name']).count()
ChainAmt = rawdata['bill_amount'].groupby(rawdata['buyer_name']).sum()
Chainavgpayday = rawdata['pay_day'].groupby(rawdata['buyer_name']).mean()
Chainstdpayday = rawdata['pay_day'].groupby(rawdata['buyer_name']).std()
ChainName = pd.Series(ChainNum.index,index=ChainNum.index)
ChainIndex = pd.DataFrame({'ChainName':ChainName, 'BillNum':ChainNum, 'BillAmt':ChainAmt,'AvgPayday':Chainavgpayday, 'StdPayday':Chainstdpayday})

#
ChainIndex['ChainOutAmt'] = 0
ChainIndex['ChainOutAmtRio'] = 0
k = 0
for i in ChainIndex.ChainName:
    cd = (rawdata['buyer_name'] == i) & \
         (rawdata['pay_day'] >= (ChainIndex.AvgPayday[ChainIndex['ChainName']==i].iloc[0]-2*ChainIndex.StdPayday[ChainIndex['ChainName']==i].iloc[0])) &\
         (rawdata['pay_day'] <= (ChainIndex.AvgPayday[ChainIndex['ChainName']==i].iloc[0]+2*ChainIndex.StdPayday[ChainIndex['ChainName']==i].iloc[0]))
    ChainIndex['ChainOutAmt'].iloc[k] = sum(rawdata.bill_amount[cd])
    ChainIndex['ChainOutAmtRio'].iloc[k] = ChainIndex['ChainOutAmt'].iloc[k] / ChainIndex['BillAmt'].iloc[k]
    k = k + 1

ChainIndex.to_csv('e:/code/python/ChainIndex.csv', encoding='utf-8', index=False)

#
Payday = ChainIndex.AvgPayday.quantile(.9)
StdPayday = ChainIndex.StdPayday.quantile(.9)
ChainThreshold = ChainIndex.ChainName[(ChainIndex['AvgPayday']<Payday) | (ChainIndex['StdPayday']<StdPayday) | (ChainIndex['ChainOutAmtRio']>=0.95)]
ChainThreshold.to_csv('e:/code/python/ChainThreshold.csv', encoding='utf-8', index=False)