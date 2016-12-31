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

#data cleaning: credit_order&match_pay_time_is_not_null&no_abnormal_data
def DataAbnormalDrop(rawdata):
    avgpayday = np.nanmean(rawdata.loc[:,'pay_day'])
    stdpayday = np.nanstd(rawdata.loc[:,'pay_day'])
    cldatacd = (rawdata.pay_day >= avgpayday-3*stdpayday) | (rawdata.pay_day <= avgpayday-3*stdpayday)
    cldata = rawdata[cldatacd]
    return cldata

#data_cleaning
cdata = DataAbnormalDrop(rawdata)

#model prepare
SimCt = range(0, 600, 1)
DlqRio = [0.005]  # Simulating delinqune ratio

#model of simulate credit term and delinquent amount
def SimCtDlqF(DataFra,ChainName):
    SimDlq = []
    SimDlqRio = []
    SimCtDlq = []
    SimChainname = []
    Sumamt = sum(DataFra.loc[:, 'bill_amount'])
    for j in SimCt:
        SimDlq.append(sum(DataFra.bill_amount[DataFra.pay_day >= SimCt[j]]))
        SimDlqRio.append(SimDlq[j] / Sumamt)
        SimChainname.append(ChainName)
    SimCtDlq.append(SimChainname)
    SimCtDlq.append(SimCt)
    SimCtDlq.append(SimDlq)
    SimCtDlq.append(SimDlqRio)
    SimCtDlq = pd.DataFrame(SimCtDlq, index=['Chain_name','Simulate_Term', 'Delinquent_amount', 'Delinquent_Ratio'])
    SimCtDlq = SimCtDlq.T
    return SimCtDlq

# model for sample credit term of simulate delinquent ratio  ## input output of model of simulate credit term and delinquent amount
def DltTermF(DataSimCtDlq,DlqRio,ChainName):
    DataRio = DataSimCtDlq.Delinquent_Ratio
    DlqTerm = [[],[],[]]
    DlqTerm[0].append(ChainName)
    DlqTerm[1].append(DlqRio)
    for m in range(0, len(DataRio)):
        if DataSimCtDlq.Delinquent_Ratio[m] <= DlqRio:
            DlqTerm[2].append(DataSimCtDlq.Simulate_Term[m])
            break
    DlqTerm = pd.DataFrame(DlqTerm, index=['Chain_name','SimDlq', 'Credit_term']).T
    return DlqTerm

# model for sample delinquent ratio of simulate credit term  ## input output of model of simulate credit term and delinquent amount
def TermDlqF(DataSimCtDlq,Term,DlqRio,ChainName):
    DataTerm = DataSimCtDlq.Simulate_term
    TermDlq = [[],[],[],[]]
    TermDlq[0].append(ChainName)
    TermDlq[1].append(DlqRio)
    TermDlq[2].append(Term)
    for m in range(0, len(DataTerm)):
        if DataSimCtDlq.Simulate_term[m] >= Term:
            TermDlq[3].append(DataSimCtDlq.Delinquent_amount[m])
            break
    TermDlq = pd.DataFrame(TermDlq, index=['Chain_name','SimDlq' 'Credit_term','Delinquent_amount']).T
    return TermDlq

#######Chain Credit term of simulate delinquent ratio###############
ChainDlqTerm = pd.DataFrame(columns=['Chain_name','SimDlq', 'Credit_term'])
ChainIndex = [[],[],[]]
DataNum = len(cdata) - 1  # rawdata numbers

for i in range(0,len(DlqRio)):
    for j in range(0,len(chainname['buyer_name'])):
        count = 0
        Bill = pd.DataFrame(columns=['pay_day','bill_amount'])
        for k in range(0,DataNum):
            if  cdata['buyer_name'].iloc[k] == chainname['buyer_name'].iloc[j]:
                count = count + 1
                Bill = Bill.append(cdata.iloc[k])
        SimCtDlq = SimCtDlqF(Bill,chainname['buyer_name'].iloc[j])
#        print SimCtDlq
        ChainDlqTerm = ChainDlqTerm.append(DltTermF(SimCtDlq,DlqRio[i],chainname['buyer_name'].iloc[j]), ignore_index=True)
        ChainIndex[0].append(chainname['buyer_name'].iloc[j])
        ChainIndex[1].append(DlqRio[i])
        ChainIndex[2].append(count)
ChainIndex = pd.DataFrame(ChainIndex,columns=['Chain_name','SimDlq','Bill_number'])
SimCtDlq.to_csv('e:/code/python/credit_term/SimCtDlqF2.csv',encoding='utf-8',index=False)
ChainIndex.to_csv('e:/code/python/credit_term/ChainIndex.csv',encoding='utf-8',index=False)
ChainDlqTerm.to_csv('e:/code/python/credit_term/ChainDlqTerm.csv',encoding='utf-8',index=False)


############montercarlo simulation###################
SimTim = 10
RioTim = len(DlqRio)
TermDlq = pd.DataFrame(columns=['Chain_name','SimDlq', 'Credit_term'])
SimDis = pd.DataFrame(index=range(0,SimTim*RioTim),columns=['SimDlq','SimTim','Ratio'])

m = 0
for i in range(0,len(DlqRio)):
    for j in range(0,SimTim):
        SimDis['SimDlq'].iloc[m] = DlqRio[i]
        SimDis['SimTim'].iloc[m] = j
        for k in range(0, len(chainname['buyer_name'])):
            sampleid = np.random.choice(DataNum, chainname['Bill_number'], replace=False)
            sample = cdata.iloc[sampleid, :]
            Sampleamount = Sampleamount+sum(sample.bill_amount)
            SimCtDlqM = SimCtDlqF(sample, chainname['buyer_name'].iloc[j])
            TermDlq = TermDlq.append(DltTermF(SimCtDlqM, DlqRio[i], ChainDlqTerm.Credit_term[ChainDlqTerm['Chain_name']==chainname['buyer_name'].iloc[j] | ChainDlqTerm['SimDlq']==DlqRio[i]], chainname['buyer_name'].iloc[k]), ignore_index=True)
            Overdueamount = Overdueamount+sum(TermDlq.Delinquent_amount)
        OverdueRatio = Overdueamount/Sampleamount
        SimDis['Ratio'].iloc[m] = Overdueamount
        m = m + 1

SimCtDlqM.to_csv('e:/code/python/credit_term/SimCtDlqMF2.csv',encoding='utf-8',index=False)
TermDlq.to_csv('e:/code/python/credit_term/TermDlqMF2.csv',encoding='utf-8',index=False)
SimDis.to_csv('e:/code/python/credit_term/SimDisMF2.csv',encoding='utf-8',index=False)






