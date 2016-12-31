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
where m_version = '20161117151107'
group by chain_name,bydate;''')

RAW = ('''select core_group_name,core_name,chain_name,coreerpchainuid,proof_no,
bill_time,pay_time,ra,pa,order_type
from offlinecentre.z_core_raw_document
where data_version = '20161117151107';''')

# connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW, con=cnx))
Balance = pd.DataFrame(pd.read_sql(BALANCE, con=cnx))
uuid = 'chain_name'

Bill = rdata[rdata['order_type'] == 0]
bdata = Bill[
    ['core_group_name', 'core_name', 'chain_name', 'coreerpchainuid', 'proof_no', 'bill_time', 'ra', 'order_type']]
Pay = rdata[rdata['order_type'] == 1]
pdata = Pay[
    ['core_group_name', 'core_name', 'chain_name', 'coreerpchainuid', 'proof_no', 'pay_time', 'pa', 'order_type']]

Chain = np.unique(bdata[uuid])
ChainNum = len(Chain)

print 'Data prepare Successed'
###################################################################################
Balance['cycle'] = None
Balance['match_date'] = None
Balance['bill_amount'] = None
Balance['avg_ar_amount'] = None
CycleData = pd.DataFrame(columns=Balance.columns)
for i in Chain:
    ChainBill = bdata[(bdata[uuid] == i) & (bdata['bill_time']>='2014-01-01')]
    ChainBill = ChainBill.sort_values('bill_time')
    ChainBalance = Balance[(Balance[uuid] == i) & (Balance['bydate']>='2014-01-01') & (Balance['bydate']<='2016-09-30')]
    ChainBalance = ChainBalance.sort_values('bydate')
    ChainBalanceRaw = ChainBalance
    ChainBalance = ChainBalance.drop_duplicates(subset='ar_amount', keep='first')
    for j in range(len(ChainBalance.bydate)-2):
        BillDate = pd.Timestamp(ChainBalance['bydate'].iloc[j])
        BalancePoint = ChainBalance['ar_amount'].iloc[j]
        ChainBalance['dpabs'] = (ChainBalance.ar_amount - BalancePoint).abs()
        ChainBalance['dp'] = ChainBalance.ar_amount - BalancePoint
        BalanceRange = ChainBalance.iloc[j+1:]
        for k in range(len(BalanceRange)-2):
            if ((BalanceRange['dp'].iloc[k+1]==0)):
                ChainBalance.match_date[(ChainBalance['bydate'] == BillDate) & (ChainBalance[uuid] == i)] = \
                    BalanceRange['bydate'].iloc[k+1]
                ChainBalance.cycle[(ChainBalance['bydate'] == BillDate) & (ChainBalance[uuid] == i)] = \
                    BalanceRange['bydate'].iloc[k+1] - BillDate
                ChainBalance.bill_amount[(ChainBalance['bydate'] == BillDate) & (ChainBalance[uuid] == i)] = \
                    sum(ChainBill.ra[(ChainBill['bill_time'] > str(BillDate))
                                     & (ChainBill['bill_time'] < str(BalanceRange['bydate'].iloc[k + 1]))])
                ChainBalance.avg_ar_amount[(ChainBalance['bydate'] == BillDate) & (ChainBalance[uuid] == i)] = \
                    sum(ChainBalanceRaw.ar_amount[(ChainBalanceRaw['bydate'] > str(BillDate))
                                                   & (ChainBalanceRaw['bydate'] < str(BalanceRange['bydate'].iloc[k + 1]))])
                break
            elif ((BalanceRange['dp'].iloc[k] * BalanceRange['dp'].iloc[k + 2] < 0)):
                ChainBalance.match_date[(ChainBalance['bydate'] == BillDate) & (ChainBalance[uuid] == i)] = \
                    BalanceRange['bydate'].iloc[k + 1]
                ChainBalance.cycle[(ChainBalance['bydate'] == BillDate) & (ChainBalance[uuid] == i)] = \
                    BalanceRange['bydate'].iloc[k + 1] - BillDate
                ChainBalance.bill_amount[(ChainBalance['bydate'] == BillDate) & (ChainBalance[uuid] == i)] = \
                    sum(ChainBill.ra[(ChainBill['bill_time']>str(BillDate))
                                     & (ChainBill['bill_time']<str(BalanceRange['bydate'].iloc[k+1]))])
                ChainBalance.avg_ar_amount[(ChainBalance['bydate'] == BillDate) & (ChainBalance[uuid] == i)] = \
                    sum(ChainBalanceRaw.ar_amount[(ChainBalanceRaw['bydate'] > str(BillDate))
                                                   & (ChainBalanceRaw['bydate'] < str(BalanceRange['bydate'].iloc[k + 1]))])
                break
    print str(i)+' Successed'
    CycleData = CycleData.append(ChainBalance)

CycleData.to_csv('e:/code/python/cyclematchYH.csv', encoding='utf-8')







