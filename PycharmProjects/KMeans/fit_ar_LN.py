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
BALANCE=('''SELECT core_group_name,core_name,chain_name,coreerpchainuid,
bydate,ar_amount
FROM modelcentre.m_ar_match
where m_version = '20161110132130'
and coreerpchainuid in ('10002','10118','10030','10146','40322','30407','40214','10032');''')

RAW = ('''select core_group_name,core_name,chain_name,coreerpchainuid,proof_no,
bill_time,pay_time,ra,pa,order_type
from offlinecentre.z_core_raw_document
where data_version = '20161026192200'
and coreerpchainuid in ('10002','10118','10030','10146','40322','30407','40214','10032');''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW, con=cnx))
Balance = pd.DataFrame(pd.read_sql(BALANCE, con=cnx))

Bill = rdata[rdata['order_type']==0]
bdata = Bill[['core_group_name','core_name','chain_name','coreerpchainuid','proof_no','bill_time','ra','order_type']]
Pay = rdata[rdata['order_type']==1]
pdata = Pay[['core_group_name','core_name','chain_name','coreerpchainuid','proof_no','pay_time','pa','order_type']]

Chain = np.unique(bdata['coreerpchainuid'])
ChainNum = len(Chain)
print 'Data prepare Successed'

########################################################################################
OrderList = range(1,10)
Balance['cycle'] = None
Balance['match_date'] = None
Balance['bill_amount'] = None
BalanceFit = pd.DataFrame(columns=Balance.columns)

for i in Chain:
    Fit = []
    Error = []
    for order in OrderList:
        timeindex = np.array(range(0,len(Balance[Balance['coreerpchainuid']==i])))
        time = Balance.bydate[Balance['coreerpchainuid']==i]
        chain = Balance.coreerpchainuid[Balance['coreerpchainuid']==i]
        balance = Balance.ar_amount[Balance['coreerpchainuid']==i]
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
    print len(timeindex)
    print len(balance)
    print i+" OptFit Successed"

    #plot
    fig = plt.figure(0)
    plotname = 'e:/code/python/LNfit'+i+'.png'
    timeindexp = np.linspace(0,len(Balance[Balance['coreerpchainuid']==i]),10000)
    _=plt.plot(timeindex, balance, '.', timeindexp, OptFit(timeindexp), '-')
#    _=plt.plot(timeindex,balance,'.',timeindexp,Fit[0](timeindexp),'-',timeindexp,Fit[1](timeindexp),
#               '-',timeindexp,Fit[2](timeindexp),'-',timeindexp,Fit[3](timeindexp),'-'
#               , timeindexp, Fit[4](timeindexp), '-',timeindexp,Fit[5](timeindexp),'-'
#               , timeindexp, Fit[6](timeindexp), '-',timeindexp,Fit[7](timeindexp),'-')
    plt.savefig(plotname)
    plt.close(0)

    #fit_root
    ChainBill = bdata[(bdata['coreerpchainuid'] == i) & (bdata['bill_time'] >= '2013-01-01')]
    ChainBill = ChainBill.sort_values('bill_time')
    ChainBalance = Balance[(Balance['coreerpchainuid'] == i) & (Balance['bydate']>='2013-01-01') & (Balance['bydate']<='2016-09-30')]
    ChainBalance = ChainBalance.sort_values('bydate')
    ByDate = Balance.bydate[
        (Balance['coreerpchainuid'] == i) & (Balance['bydate'] >= '2013-01-01') & (Balance['bydate'] <= '2016-09-30')]
    for dateindex in range(len(ByDate)):
        BillDate = pd.Timestamp(ChainBalance['bydate'].iloc[dateindex])
        BalancePoint = ChainBalance.ar_amount[ChainBalance['bydate'] == BillDate].values[0]
        DatePoints = (OptFit - BalancePoint).roots()
        if(len(DatePoints[(DatePoints.imag == 0) & (DatePoints.real > 0)])>0):
            DatePoint = min(DatePoints[(DatePoints.imag == 0) & (DatePoints.real > 0)])
            Date = pd.to_datetime(ByDate.iloc[0]) + DateOffset(days=DatePoint.real)
            ChainBalance.cycle[(ChainBalance['bydate'] == BillDate) & (ChainBalance['coreerpchainuid'] == i)] = DatePoint.real
            ChainBalance.match_date[(ChainBalance['bydate'] == BillDate) & (ChainBalance['coreerpchainuid'] == i)] = Date
            ChainBalance.bill_amount[(ChainBalance['bydate'] == BillDate) & (ChainBalance['coreerpchainuid'] == i)] = \
                sum(ChainBill.ra[(ChainBill['bill_time']>=str(BillDate)) & (ChainBill['bill_time']<=Date)])
    print i + " Cycle Successed"
    Chainoupname = 'e:/code/python/fitbalance_LN'+i+'.csv'
    ChainBalance.to_csv(Chainoupname,encoding='utf-8')
    BalanceFit = BalanceFit.append(ChainBalance)

BalanceFit.to_csv('e:/code/python/fitbalance_LN.csv',encoding='utf-8')

