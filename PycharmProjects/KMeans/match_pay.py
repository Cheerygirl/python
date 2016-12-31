#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
import wavelets
from time import time
import matplotlib.pyplot as plt
import datetime

#data_prepare
RAW=('''select core_group_name,core_name,chain_name,coreerpchainuid,proof_no,
bill_time,pay_time,ra,pa,order_type
from offlinecentre.z_core_raw_document
where data_version = '20161117151107'
and coreerpchainuid = '2004378';''')

#INIBALANCE=('''select core_group_name,core_name,chain_name,coreerpchainuid,
#calendar_date,inbalance
#from offlinecentre.z_core_datainitial_balance
#where data_version = '20161117151107'
#and coreerpchainuid = '2004378';''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))
#inibalance = pd.DataFrame(pd.read_csv(INIBALANCE,con=cnx))

Bill = rdata[rdata['order_type']==0]
bdata = Bill[['core_group_name','core_name','chain_name','coreerpchainuid','proof_no','bill_time','ra','order_type']]
Pay = rdata[rdata['order_type']==1]
pdata = Pay[['core_group_name','core_name','chain_name','coreerpchainuid','proof_no','pay_time','pa','order_type']]

Chain = np.unique(bdata['chain_name'])
ChainNum = len(Chain)
INIBALANCE = np.zeros(ChainNum)
INIBALANCE = pd.Series(INIBALANCE,index=Chain)

#AR balance
ByDate = pd.date_range('20140101','20160930')
Balance = []
for i in Chain:
    for j in ByDate:
        Balance.append(INIBALANCE.loc[INIBALANCE.index==i].values[0]+\
                  sum(bdata.ra[(bdata['bill_time']<=j) & (bdata['chain_name']==i)])-\
                  sum(pdata.pa[(pdata['pay_time']<=j) & (pdata['chain_name']==i)]))
Balance = pd.Series(Balance,index=ByDate)
Balance.to_csv('e:/code/python/balance.csv',encoding='utf-8')


#match_pay_time
bdata['match_pay_time'] = None
bdata['paid_amount'] = None
MatchData = pd.DataFrame(columns=bdata.columns)
for i in Chain:
    Balance = INIBALANCE.loc[INIBALANCE.index == i].values[0]
    ChainPay = pdata[pdata['chain_name']==i]
    ChainPay = ChainPay.sort_values('pay_time')
    ChainBill = bdata[bdata['chain_name']==i]
    ChainBill = ChainBill.sort_values('bill_time')
    if Balance>=0:
        ChainBillSum = Balance
        ChainPaySum = 0.0
    else:
        ChainBillSum = 0.0
        ChainPaySum = -1*Balance
    pay_point = 0
    for j in range(len(ChainBill)):
        ChainBillSum = ChainBillSum + ChainBill['ra'].iloc[j]
        while(ChainPaySum < ChainBillSum):
            if(pay_point<=(len(ChainPay)-1)):
                ChainPaySum = ChainPaySum + ChainPay['pa'].iloc[pay_point]
                pay_point = pay_point + 1
            else:
                break
        if(ChainPaySum>=ChainBillSum):
            ChainBill['match_pay_time'].iloc[j] = ChainPay['pay_time'].iloc[pay_point]
            ChainBill['paid_amount'].iloc[j] = ChainBill['ra'].iloc[j]
        elif((ChainPaySum<ChainBillSum)&(pay_point==(len(ChainPay)-1))):
            ChainBill['paid_amount'].iloc[j] = ChainPaySum-(ChainBillSum-ChainBill['ra'].iloc[j])
        else:
            break
    MatchData = MatchData.append(ChainBill)

#MatchData['dso'] = None
#for i in range(len(MatchData)):
#    if(MatchData['match_pay_time'].iloc[i]==None):
#        bill_time = pd.to_datetime(MatchData['bill_time'].iloc[i],unit='D')
#        match_pay_time = pd.to_datetime(MatchData['match_pay_time'].iloc[i],unit='D')
#        pay_day = match_pay_time-bill_time
#        MatchData['dso'] = pay_day
#    else:
#        MatchData['dso'] = 'Not Paid'

MatchData.to_csv('e:/code/python/MatchData.csv',encoding='utf-8')

