#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
from time import time
import matplotlib.pyplot as plt
import datetime
import time

from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.datasets import load_digits
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale

import csv as csv

#data_prepare
RAW=('''select seller_group_name,seller_name,buyer_name,buyer_id,order_no,
bill_time,match_pay_time,due_time,bill_amount,paid_amount,
datediff(match_pay_time,bill_time) pay_day,is_all_paid
from modelcentre.m_core_bill_match
where m_version = '20161109000000'
and buyer_name <> ''
order by buyer_name;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))

#read_credit_term&limit
TermLimit = pd.read_csv('e:/code/credit_term_and_limit_for_tianwo.csv',encoding='gbk')

#data_prepare
TermLimit = TermLimit[[0,1,3,5]]
TermLimit.columns = ['chain_name','core_name','credit_term','credit_limit']
TermLimit = TermLimit.dropna()
TermDict = {TermLimit['chain_name'][i]: TermLimit['credit_term'][i] for i in TermLimit.index}
LimitDict = {TermLimit['chain_name'][i]: TermLimit['credit_limit'][i] for i in TermLimit.index}

rdata['bill_time'] = pd.to_datetime(rdata['bill_time'])
rdata['match_pay_time'] = pd.to_datetime(rdata['match_pay_time'])
rdata['due_time'] = pd.to_datetime(rdata['due_time'])

Order = rdata[['buyer_name','seller_name','order_no','bill_time','due_time','match_pay_time','bill_amount','pay_day','paid_amount']]
Order.columns = ['chain_name','core_name','order_no','bill_time','due_time','pay_time','bill_amount','pay_day','paid_amount']
Order['dcf_credit_term'] = Order['chain_name'].map(lambda x:TermDict[x] if x in TermDict.keys() else np.nan)
Order['dcf_credit_limit'] = Order['chain_name'].map(lambda x:LimitDict[x] if x in LimitDict.keys() else np.nan)
Order['credit_term_dcf'] = pd.to_timedelta(Order['dcf_credit_term'],unit='d')
Order['dcf_due_time'] = Order['bill_time'] + Order['credit_term_dcf']
Order = Order.drop(['credit_term_dcf'],axis=1)
Order['loan_time'] = np.nan
Order['is_in_loan'] = np.nan
Order['is_paid_off'] = np.where(Order['bill_amount']>Order['paid_amount'],0,1)
Order['is_overdue'] = np.where((Order['is_paid_off']==1)&(Order['pay_day']>Order['dcf_credit_term']),1,0)
Order['overdue_amount'] = np.where((Order['is_paid_off']==1)&(Order['pay_day']>Order['dcf_credit_term']),Order['bill_amount'],0)
Order['overdue_day'] = np.where((Order['is_paid_off']==1)&(Order['pay_day']>Order['dcf_credit_term']),(Order['pay_day']-Order['dcf_credit_term']),0)
#Order['credit_term'] = None
#Order['due_time'] = None
#for chain in np.unique(TermLimit['chain_name']):
#    for order in range(len(Order.credit_term[Order.chain_name == chain])):
#        Order.credit_term[order] = TermLimit.credit_term[TermLimit['chain_name'] == chain][0]
#        Order.due_time[order] = Order.bill_time[order] + Order.credit_term[order]

#���������Ż�׼������
Order = Order.dropna(axis=0,subset=['dcf_credit_term','dcf_credit_limit'])

#��������ฺ�ͬ��Ч��2014-10-01��2019-12-31�������ʵ��ݱ���Ϊ��������δ����δ����
#����������ʼ��Ϊ2014-11-30��������2014-11-30�������ջ���ȡ���ռ�֮ǰ����δ���ʶ���Ϊ������
#��ʷ�Ѹ��������δ������ʺ�δ���ʵ��ݣ��⼸�����ݼ��漰ָ�����
#�����趨ʱ��ÿ10��ˢ��һ��ָ��
#ѭ���趨��ÿ�գ�ÿ���ᵥ��Ӳָ���жϣ��ᵥһ������ɣ���ÿ10�������ύ

SimDateRange = pd.date_range('2014-11-30','2016-11-30')
for Date in SimDateRange:
    DataStartDate = pd.to_datetime('2014-01-01')
    DataEndDate = pd.to_datetime(Date)
    DataRange = pd.date_range(DataStartDate,DataEndDate)
    #��ԭDate���������Date���켰֮ǰ�Ķ����������Ѹ�����������δ������,�ų���Ч����(��ͬ����)�����ʶ���������δ����δ�ڣ���δ���ʶ���
    OrderSet = Order[(Order['bill_time']>=DataStartDate) & (Order['bill_time']<=DataEndDate)]
    OrderSet['pay_day'] = np.where(OrderSet['pay_time']>DataEndDate,np.nan,OrderSet['pay_day'])
    OrderSet['is_paid_off'] = np.where(OrderSet['pay_time']>DataEndDate,0,1)
    OrderSet['is_overdue'] =  np.where(OrderSet['pay_time']>DataEndDate,0,1)
    OrderSet['overdue_amount'] = np.where(OrderSet['pay_time']>DataEndDate,np.nan,OrderSet['overdue_amount'])
    OrderSet['overdue_day'] = np.where(OrderSet['pay_time']>DataEndDate,np.nan,OrderSet['overdue_day'])
    OrderSet['paid_amount'] = np.where(OrderSet['pay_time']>DataEndDate,np.nan,OrderSet['paid_amount'])
    OrderSet['pay_time'] = OrderSet['pay_time'].map(lambda x: x if x <= DataEndDate else np.nan)
    #�Ѹ�����Ϊ������ɶ���
    PaidSet = OrderSet[OrderSet['is_paid_off']==1]
    #���ʵ��ݣ�����δ����δ�ڣ�Ϊδ�����ں�ͬ���ڵ���
    LoanSet = OrderSet[(OrderSet['is_paid_off']==0) & (OrderSet['bill_time']>='2014-10-01')
                       & (OrderSet['bill_time']<='2019-12-31') & (OrderSet['dcf_due_time']>=DataEndDate)]
    LoanedSet = LoanSet[LoanSet['is_in_loan']==1]
    LoanOrderSet = LoanSet[LoanSet['is_in_loan']==0]


def OrdAmtScore(DataSet):
    AvgAmt = DataSet['bill_amount'].mean()
    StdAmt = DataSet['bill_amount'].std()
    DataSet['OrdAmt_Score'] = DataSet['bill_amount'].map(lambda x:9 if (x>=AvgAmt+3*StdAmt)&(x<=AvgAmt-3+StdAmt) else 1 if (x<AvgAmt+3*StdAmt)&(x>AvgAmt-3+StdAmt)&(x>=AvgAmt+2*StdAmt)&(x<=AvgAmt-2+StdAmt) else 3)
    return DataSet





#Day index prepare
def WindowIndex(window,rawdata,Index):
    Chain = np.unique(rawdata['chain_name'])
    Mindate = rawdata['bill_time'].groupby(rawdata['chain_name']).min()
    for chain in Chain:
        startdate = Mindate[chain]
        DateRange = pd.date_range(startdate, datetime.date.today())
        for date in DateRange:
            WindowStartDate = date
            WindowEndDate = date + datetime.timedelta(days=window)
            WindowData = rawdata[(rawdata['chain_name']==chain)&(rawdata['bill_time']>=WindowStartDate)&(rawdata['bill_time']<=WindowEndDate)]

