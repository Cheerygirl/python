#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error
from numpy.polynomial import Polynomial as P
from pandas.tseries.offsets import *

#data_prepare
RAW=('''select core_group_name,core_name,chain_name,coreerpchainuid,proof_no,
bill_time,pay_time,ra,pa,order_type
from offlinecentre.z_core_raw_document
where data_version = '20161117151107'
and coreerpchainuid = '2004378';''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))

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
BalanceChain = []
BalanceDate = []
for i in Chain:
    for j in ByDate:
        Balance.append(INIBALANCE.loc[INIBALANCE.index==i].values[0]+\
                  sum(bdata.ra[(bdata['bill_time']<=j) & (bdata['chain_name']==i)])-\
                  sum(pdata.pa[(pdata['pay_time']<=j) & (pdata['chain_name']==i)]))
        BalanceChain.append(i)
        BalanceDate.append(j)
Balance = pd.DataFrame({'chain_name':BalanceChain,'by_date':BalanceDate,'Balance':Balance})
Balance.to_csv('e:/code/python/balance.csv',encoding='utf-8')

#Fit_order
OrderList = range(1,10)
Balance['cycle'] = None
Balance['match_date'] = None
Balance['bill_amount'] = None
BalanceFit = pd.DataFrame(columns=Balance.columns)
Fit = []
Error = []
for i in Chain:
    for order in OrderList:
        timeindex = np.array(range(0,len(Balance[Balance['chain_name']==i])))
        time = Balance.by_date[Balance['chain_name']==i]
        chain = Balance.chain_name[Balance['chain_name']==i]
        balance = Balance.Balance[Balance['chain_name']==i]
        fit = P.fit(timeindex,balance,order)
        balancefit = fit(timeindex)
        error = mean_squared_error(list(balance),list(balancefit))
#        BalanceFit = BalanceFit.append(pd.DataFrame({'chain_name':chain,'by_date':time,'index':timeindex,'balance':balance,'balancefit':balancefit}))
        Fit.append(fit)
        Error.append(error)
    Fit = pd.Series(Fit,index=OrderList)
    Error = pd.Series(Error,index=OrderList)
    Opterr = min(Error)
    for order in OrderList:
        if(Error[order] == Opterr):
            OptFit = Fit[order]
    #plot
    timeindexp = np.linspace(0,len(Balance[Balance['chain_name']==i]),10000)
    _ = plt.plot(timeindex, balance, '.', timeindexp, OptFit(timeindexp))
#    _=plt.plot(timeindex,balance,'.',timeindexp,Fit[0](timeindexp),'-',timeindexp,Fit[1](timeindexp),
#               '-',timeindexp,Fit[2](timeindexp),'-',timeindexp,Fit[3](timeindexp),'-'
#               , timeindexp, Fit[4](timeindexp), '-',timeindexp,Fit[5](timeindexp),'-'
#               , timeindexp, Fit[6](timeindexp), '-',timeindexp,Fit[7](timeindexp),'-')
    plt.show()

    ChainBill = bdata[(bdata['chain_name'] == i) & (bdata['bill_time'] >= '2014-01-01')]
    ChainBill = ChainBill.sort_values('bill_time')
    ChainBalance = Balance[Balance['chain_name'] == i]
    ChainBalance = ChainBalance.sort_values('by_date')
    for dateindex in range(len(ByDate)):
        BillDate = pd.Timestamp(ChainBalance['by_date'].iloc[dateindex])
        BalancePoint = ChainBalance.Balance[ChainBalance['by_date'] == BillDate].values[0]
        DatePoints = (OptFit - BalancePoint).roots()
        DatePoint = min(DatePoints[(DatePoints.imag == 0) & (DatePoints.real > 0)])
        Date = ByDate[0] + DateOffset(days=DatePoint.real)
        ChainBalance.cycle[(ChainBalance['by_date'] == BillDate) & (ChainBalance['chain_name'] == i)] = DatePoint
        ChainBalance.match_date[(ChainBalance['by_date'] == BillDate) & (ChainBalance['chain_name'] == i)] = Date
        ChainBalance.bill_amount[(ChainBalance['by_date'] == BillDate) & (ChainBalance['chain_name'] == i)] = \
            sum(ChainBill.ra[(ChainBill['bill_time']>=str(BillDate)) & (ChainBill['bill_time']<=Date)])
    BalanceFit = BalanceFit.append(ChainBalance)

BalanceFit.to_csv('e:/code/python/fitbalance.csv',encoding='utf-8')

#Fit
#order = 9
#Balance['balance_fit'] = None
#for i in Chain:
#    timeindex = np.array(range(0,len(Balance[Balance['chain_name']==i])))
#    time = Balance.by_date[Balance['chain_name']==i]
#    balance = Balance.Balance[Balance['chain_name']==i]
#    balancefit = np.polyfit(timeindex,balance,order)
#    fit = np.poly1d(balancefit)
#    print fit
#    #plot
#    timeindexp = np.linspace(0,len(Balance[Balance['chain_name']==i]),10000)
#    _=plt.plot(timeindex,balance,'.',timeindexp,fit(timeindexp),'-')
#    plt.show()

#Fit_order
#OrderList = range(1,10)
#Balance['balance_fit'] = None
#Balance['index'] = None
#BalanceFit = pd.DataFrame(columns=Balance)
#Fit = []
#Error = []
#for i in Chain:
#    for order in OrderList:
#        timeindex = np.array(range(0,len(Balance[Balance['chain_name']==i])))
#        time = Balance.by_date[Balance['chain_name']==i]
#        chain = Balance.chain_name[Balance['chain_name']==i]
#        balance = Balance.Balance[Balance['chain_name']==i]
#        balanceparam = np.polyfit(timeindex,balance,order)
#        fit = np.poly1d(balanceparam)
#        balancefit = fit(timeindex)
#        error = mean_squared_error(list(balance),list(balancefit))
#        BalanceFit = BalanceFit.append(pd.DataFrame({'chain_name':chain,'by_date':time,'index':timeindex,'balance':balance,'balancefit':balancefit}))
#        Fit.append(fit)
#        Error.append(error)
#    Fit = pd.Series(Fit,index=OrderList)
#    Error = pd.Series(Error,index=OrderList)
#    Opterr = min(Error)
#    for order in OrderList:
#        if(Error[order] == Opterr):
#            OptFit = Fit[order]
#    #plot
#    timeindexp = np.linspace(0,len(Balance[Balance['chain_name']==i]),10000)
#    _ = plt.plot(timeindex, balance, '.', timeindexp, OptFit(timeindexp))
#    _=plt.plot(timeindex,balance,'.',timeindexp,Fit[0](timeindexp),'-',timeindexp,Fit[1](timeindexp),
#               '-',timeindexp,Fit[2](timeindexp),'-',timeindexp,Fit[3](timeindexp),'-'
#               , timeindexp, Fit[4](timeindexp), '-',timeindexp,Fit[5](timeindexp),'-'
#               , timeindexp, Fit[6](timeindexp), '-',timeindexp,Fit[7](timeindexp),'-')
#    plt.show()

#ChainBill = bdata[(bdata['chain_name'] == i) & (bdata['bill_time'] >= '2014-01-01')]
#ChainBill = ChainBill.sort_values('bill_time')
#ChainBalance = Balance[Balance['chain_name'] == i]
#ChainBalance = ChainBalance.sort_values('by_date')
#for dateindex in range(len(ByDate)):
#    BillDate = pd.Timestamp(ChainBalance['by_date'].iloc[j])
#    BalancePoint = ChainBalance.Balance[ChainBalance['by_date'] == BillDate].values[0]
#    DatePoint = OptFit(BalancePoint)

