#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
import wavelets
from time import time
import matplotlib.pyplot as plt
from pandas.tseries.offsets import *

# data_prepare
BALANCE=('''SELECT core_group_name,core_name,chain_name,coreerpchainuid,
bydate,ar_amount
FROM modelcentre.m_ar_match
where m_version = '20161110132130';''')

RAW = ('''select core_group_name,core_name,chain_name,coreerpchainuid,proof_no,
bill_time,pay_time,ra,pa,order_type
from offlinecentre.z_core_raw_document
where data_version = '20161026192200';''')

# connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW, con=cnx))
Balance = pd.DataFrame(pd.read_sql(BALANCE, con=cnx))

Bill = rdata[rdata['order_type'] == 0]
bdata = Bill[
    ['core_group_name', 'core_name', 'chain_name', 'coreerpchainuid', 'proof_no', 'bill_time', 'ra', 'order_type']]
Pay = rdata[rdata['order_type'] == 1]
pdata = Pay[
    ['core_group_name', 'core_name', 'chain_name', 'coreerpchainuid', 'proof_no', 'pay_time', 'pa', 'order_type']]

Chain = np.unique(bdata['coreerpchainuid'])
ChainNum = len(Chain)
print 'Data prepare Successed'
###################################################################################
Balance['cycle'] = None
Balance['bill_amount'] = None
CycleData = pd.DataFrame(columns=Balance.columns)
for i in Chain:
    ChainBill = bdata[(bdata['coreerpchainuid'] == i) & (bdata['bill_time']>='2014-01-01')]
    ChainBill = ChainBill.sort_values('bill_time')
    ChainBalance = Balance[(Balance['coreerpchainuid'] == i) & (Balance['bydate']>='2014-01-01') & (Balance['bydate']<='2016-09-30')]
    ChainBalance = ChainBalance.sort_values('bydate')
    ByDate = ChainBalance.bydate
    for j in range(len(ByDate)-2):
        BillDate = pd.Timestamp(ChainBalance['bydate'].iloc[j])
        EndDate = pd.Timestamp(ChainBalance['bydate'].iloc[-1])
        BalancePoint = ChainBalance.ar_amount[ChainBalance['bydate'] == BillDate].values[0]
        ChainBalance['dpabs'] = (ChainBalance.ar_amount - BalancePoint).abs()
        ChainBalance['dp'] = ChainBalance.ar_amount - BalancePoint
        for date in pd.date_range(str(BillDate), str(EndDate - DateOffset(days=2))):
            dateindex = ChainBalance.index[ChainBalance.bydate == date].values[0]
            balance = ChainBalance.ar_amount[ChainBalance.bydate == date].values[0]
            if ((ChainBalance.dp[dateindex] * ChainBalance.dp[dateindex + 2] > 0)
                    & (ChainBalance.dpabs[dateindex+1]==0)):
 #                   | (ChainBalance['dpabs'].iloc[dateindex+1]<=(balance*0.001))):
                ChainBill.cycle[(ChainBill['bill_time'] == BillDate) & (ChainBill['chain_name'] == i)] = ChainBalance.by_date[dateindex+1]
                break
            elif ((ChainBalance.dpabs[dateindex + 1] > 0)
                    & (ChainBalance.dpabs[dateindex] > ChainBalance.dpabs[dateindex + 1])
                    & (ChainBalance.dpabs[dateindex + 2] > ChainBalance.dpabs[dateindex + 1])
                    & (ChainBalance.dp[dateindex] * ChainBalance.dp[dateindex + 2] < 0)):
                ChainBalance.cycle[(ChainBalance['bydate'] == BillDate) & (ChainBalance['coreerpchainuid'] == i)] = \
                ChainBalance.bydate[dateindex + 1]
                ChainBalance.bill_amount[(ChainBalance['bydate'] == BillDate) & (ChainBalance['coreerpchainuid'] == i)] = \
                    sum(ChainBill.ra[(ChainBill['bill_time']>=str(BillDate))
                                     & (ChainBill['bill_time']<=str(ChainBalance.bydate[dateindex + 1]))])
                break
        print str(i)+' Successed'
    CycleData = CycleData.append(ChainBalance)

CycleData.to_csv('e:/code/python/cycledatamatch_sql.csv', encoding='utf-8')







