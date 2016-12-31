#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
import wavelets
from time import time
import matplotlib.pyplot as plt

# data_prepare
RAW = ('''select core_group_name,core_name,chain_name,coreerpchainuid,proof_no,
bill_time,pay_time,ra,pa,order_type
from offlinecentre.z_core_raw_document
where data_version = '20161117151107'
and coreerpchainuid = '2004378';''')

# connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW, con=cnx))

Bill = rdata[rdata['order_type'] == 0]
bdata = Bill[
    ['core_group_name', 'core_name', 'chain_name', 'coreerpchainuid', 'proof_no', 'bill_time', 'ra', 'order_type']]
Pay = rdata[rdata['order_type'] == 1]
pdata = Pay[
    ['core_group_name', 'core_name', 'chain_name', 'coreerpchainuid', 'proof_no', 'pay_time', 'pa', 'order_type']]

Chain = np.unique(bdata['chain_name'])
ChainNum = len(Chain)
INIBALANCE = np.zeros(ChainNum)
INIBALANCE = pd.Series(INIBALANCE, index=Chain)

# AR balance
ByDate = pd.date_range('20140101', '20160930')
Balance = []
BalanceChain = []
BalanceDate = []
for i in Chain:
    for j in ByDate:
        Balance.append(INIBALANCE.loc[INIBALANCE.index == i].values[0] + \
                       sum(bdata.ra[(bdata['bill_time'] <= j) & (bdata['chain_name'] == i)]) - \
                       sum(pdata.pa[(pdata['pay_time'] <= j) & (pdata['chain_name'] == i)]))
        BalanceChain.append(i)
        BalanceDate.append(j)
Balance = pd.DataFrame({'chain_name': BalanceChain, 'by_date': BalanceDate, 'Balance': Balance})
Balance.to_csv('e:/code/python/balance.csv', encoding='utf-8')

Balance['cycle'] = None
Balance['bill_amount'] = None
CycleData = pd.DataFrame(columns=Balance.columns)
for i in Chain:
    ChainBill = bdata[(bdata['chain_name'] == i) & (bdata['bill_time']>='2014-01-01')]
    ChainBill = ChainBill.sort_values('bill_time')
    ChainBalance = Balance[Balance['chain_name'] == i]
    ChainBalance = ChainBalance.sort_values('by_date')
    for j in range(len(ByDate)):
        BillDate = pd.Timestamp(ChainBalance['by_date'].iloc[j])
        BalancePoint = ChainBalance.Balance[ChainBalance['by_date'] == BillDate].values[0]
        ChainBalance['dpabs'] = (ChainBalance.Balance - BalancePoint).abs()
        ChainBalance['dp'] = ChainBalance.Balance - BalancePoint
        for date in pd.date_range(str(BillDate), '20160928'):
            dateindex = ChainBalance.index[ChainBalance.by_date == date].values[0]
            balance = ChainBalance.Balance[ChainBalance.by_date == date].values[0]
            #            if ((ChainBalance['dpabs'].iloc[dateindex+1]==0) | (ChainBalance['dpabs'].iloc[dateindex+1]<=(balance*0.001))):
            #            if (ChainBalance['dpabs'].iloc[dateindex + 1] == 0):
            #                ChainBill.cycle[(ChainBill['bill_time'] == BillDate) & (ChainBill['chain_name'] == i)] = ChainBalance['by_date'].iloc[dateindex+1]
            #                break
            if ((ChainBalance['dpabs'].iloc[dateindex + 1] > 0)
                    & (ChainBalance['dpabs'].iloc[dateindex] > ChainBalance['dpabs'].iloc[dateindex + 1])
                    & (ChainBalance['dpabs'].iloc[dateindex + 2] > ChainBalance['dpabs'].iloc[dateindex + 1])
                    & (ChainBalance['dp'].iloc[dateindex] * ChainBalance['dp'].iloc[dateindex + 2] < 0)):
                ChainBalance.cycle[(ChainBalance['by_date'] == BillDate) & (ChainBalance['chain_name'] == i)] = \
                ChainBalance['by_date'].iloc[dateindex + 1]
                ChainBalance.bill_amount[(ChainBalance['by_date'] == BillDate) & (ChainBalance['chain_name'] == i)] = \
                    sum(ChainBill.ra[(ChainBill['bill_time']>=str(BillDate))
                                     & (ChainBill['bill_time']<=str(ChainBalance['by_date'].iloc[dateindex + 1]))])
                break
    CycleData = CycleData.append(ChainBalance)

CycleData.to_csv('e:/code/python/cycledatamatch.csv', encoding='utf-8')







