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

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

#data vesion extract
m_version = '20160913184422'

#data_prepare
def DataPrepare(version,con):
    try:
        rawdata = pd.read_sql(RAW,con=cnx,params={'m_version':m_version})
        print 'SUCCESS DATA EXTRACT!'
        cnx.close()
    except mysql.connector.Error as e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    return rawdata

# read_data
rdata = pd.DataFrame(DataPrepare(m_version,cnx))

#data cleaning: credit_order&match_pay_time_is_not_null&no_abnormal_data
def DataAbnormalDrop(rawdata):
    avgpayday = np.nanmean(rawdata.loc[:,'pay_day'])
    stdpayday = np.nanstd(rawdata.loc[:,'pay_day'])
    cldatacd = (rawdata.pay_day >= avgpayday-3*stdpayday) | (rawdata.pay_day <= avgpayday-3*stdpayday)
    cldata = rawdata[cldatacd]
    return cldata

#data_cleaning
cdata = DataAbnormalDrop(rdata)

#model prepare
SimCt = range(0, 801, 1)
DlqRio = [0.005, 0.01, 0.02, 0.03, 0.04, 0.05]  # Simulating delinqune ratio

#model of simulate credit term and delinquent amount
def SimCtDlqF(DataFra):
    SimDlq = []
    SimDlqRio = []
    SimCtDlq = []
    Sumamt = sum(DataFra.loc[:, 'bill_amount'])
    for j in SimCt:
        SimDlq.append(sum(DataFra.bill_amount[DataFra.pay_day >= SimCt[j]]))
        SimDlqRio.append(SimDlq[j] / Sumamt)
    SimCtDlq.append(SimCt)
    SimCtDlq.append(SimDlq)
    SimCtDlq.append(SimDlqRio)
    SimCtDlq = pd.DataFrame(SimCtDlq, index=['Simulate_Term', 'Delinquent_amount', 'Delinquent_Ratio'])
    SimCtDlq = SimCtDlq.T
    return SimCtDlq

# model for sample credit term of simulate delinquent ratio  ## input output of model of simulate credit term and delinquent amount
def DltTermF(DataSimCtDlq):
    DataRio = DataSimCtDlq.Delinquent_Ratio
    DlqTerm = [[],[]]
    for k in range(0, len(DlqRio)):
        DlqTerm[0].append(DlqRio[k])
        for m in range(0, len(DataRio)):
            if DataSimCtDlq.Delinquent_Ratio[m] <= DlqRio[k]:
                DlqTerm[1].append(DataSimCtDlq.Simulate_Term[m])
                break
    DlqTerm = pd.DataFrame(DlqTerm, index=['SimDlq', 'Credit_term']).T
    return DlqTerm

#model for credit term of simulate delinquent ratio  ##input ALLoutput of sample credit term of simulate delinquent ratio
def CreditTermF(DataDlqTerm):
    CreditTerm = pd.DataFrame(index=DlqRio, columns=['SimDlq', 'Credit_term', 'Rio'])
    for k in range(0, len(DlqRio)):
        Term = []
        CreditTerm['SimDlq'].iloc[k] = DlqRio[k]
        for m in range(0, len(DataDlqTerm['SimDlq'])):
            if DataDlqTerm['SimDlq'].iloc[m] == DlqRio[k]:
                Term.append(DataDlqTerm['Credit_term'].iloc[m])
        Term = pd.DataFrame(Term)
        CreditTerm['Credit_term'].iloc[k] = Term.mode().iloc[0, 0]
    for k in range(0, len(DlqRio) - 1):
        CreditTerm['Rio'].iloc[k + 1] = (
        float(CreditTerm['Credit_term'].iloc[k] - CreditTerm['Credit_term'].iloc[k + 1]) / float(
            CreditTerm['SimDlq'].iloc[k] - CreditTerm['SimDlq'].iloc[k + 1]))
    return CreditTerm

#model for opt Credit_term & delinquent ratio  ##input output of credit term of simulate delinquent ratio
def OptCreditTermF(DataCreditTerm):
    MaxRit = DataCreditTerm['Rio'].min()
    for k in range(0, len(DlqRio)-1):
        if DataCreditTerm.iloc[k, 1] >= MaxRit:
            OptCreditTerm = DataCreditTerm['Credit_term'].iloc[k]
    return OptCreditTerm

##########montecarlo simulate#################
DlqTerm = pd.DataFrame(columns=['SimDlq', 'Credit_term'])
DataNum = len(cdata) - 1  # rawdata numbers
# Ramdon Simulate Times
SimulateTimeRange = range(100,101,1)
SimulateTimes = len(SimulateTimeRange)
SimulateTime= np.random.choice(SimulateTimeRange,SimulateTimes,replace=False)
#Ramdom Sample Number
SampleNumberRange = range(100,101,1)
SampleNumbers = len(SampleNumberRange)
SampleNumber = np.random.choice(SampleNumberRange,SampleNumbers,replace=False)
for i in range(0,len(SimulateTime)):
    for j in range(0,len(SampleNumber)):
        for k in range(1,SimulateTime[i]):
            # sampling
            sampleid = np.random.choice(DataNum, SampleNumber[j], replace=False)
            sample = cdata.iloc[sampleid,:]
            SimCtDlq = SimCtDlqF(sample)
            DlqTerm = DlqTerm.append(DltTermF(SimCtDlq),ignore_index=True)
CreditTerm = CreditTermF(DlqTerm)
OptCreditTerm = OptCreditTermF(CreditTerm)
DlqTerm.to_csv('e:/code/python/credit_term/DlqTermF.csv',encoding='utf-8',index=False)
CreditTerm.to_csv('e:/code/python/credit_term/CreditTermF.csv',encoding='utf-8',index=False)
print OptCreditTerm





