#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo pay_pct simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np
import random

#data_extract
#sql extract
RAW=('''select core_group_name,core_name,chain_name,
pa/ra pay_pct,ra bill_amount
from offlinecentre.z_core_raw_document
where data_version = %(data_version)s
and ra > 0
and pay_time is not null
and datediff(due_time,bill_time)>0
order by chain_name;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='offlinecentre',
                              use_unicode=True)

#data vesion extract
data_version = '20160614'

#data_prepare
def DataPrepare(version,con):
    try:
        rawdata = pd.read_sql(RAW,con=cnx,params={'data_version':data_version})
        print 'SUCCESS DATA EXTRACT!'
        cnx.close()
    except mysql.connector.Error as e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    return rawdata

# read_data
rdata = pd.DataFrame(DataPrepare(data_version,cnx))

#data cleaning: credit_order&match_pay_time_is_not_null&no_abnormal_data
def DataAbnormalDrop(rawdata):
    avgamount = np.nanmean(rawdata.loc[:,'bill_amount'])
    stdamount = np.nanstd(rawdata.loc[:,'bill_amount'])
    avgpaypct = np.nanmean(rawdata.loc[:,'pay_pct'])
    stdapaypct = np.nanstd(rawdata.loc[:,'pay_pct'])
    cldatacd = (rawdata.bill_amount >= avgamount-3*stdamount) | (rawdata.bill_amount <= avgamount-3*stdamount) | (rawdata.pay_pct >= avgpaypct-3*stdapaypct) |(rawdata.pay_pct <= avgpaypct+3*stdapaypct)
    cldata = rawdata[cldatacd]
    return cldata

#data_cleaning
cdata = DataAbnormalDrop(rdata)

#model prepare
SimPct = [i/100.0 for i in range(101)] #Simulateing Pay pct
AmtRio = [i/10.0 for i in range(6,11,1)]  # Simulating Credit_amount ratio

#model of simulate Paypct and Credit_amount
def SimPctAmtF(DataFra):
    SimAmt = []
    SimAmtRio = []
    SimPctAmt = []
    Sumamt = sum(DataFra.loc[:, 'bill_amount'])
    for j in range(0,len(SimPct)):
        cd = (DataFra.pay_pct*100).astype(int) == int(SimPct[j]*100)
        SimAmt.append(sum(DataFra.bill_amount[cd]))
        SimAmtRio.append(SimAmt[j] / Sumamt)
    SimPctAmt.append(SimPct)
    SimPctAmt.append(SimAmt)
    SimPctAmt.append(SimAmtRio)
    SimPctAmt = pd.DataFrame(SimPctAmt, index=['Simulate_Paypct', 'Credit_amount', 'Amount_Ratio'])
    SimPctAmt = SimPctAmt.T
    return SimPctAmt

# model for sample Pay_pct of simulate Credit_amount_Ratio  ## input output of model of simulate Paypct and Credit_amount
def AmtPaypctF(DataSimPctAmt):
    DataRio = DataSimPctAmt.Simulate_Paypct
    AmtPpct = [[],[]]
    for k in range(0, len(AmtRio)):
        AmtPpct[0].append(AmtRio[k])
        AccuAmtRio = 0
        for m in range(0, len(DataRio)):
            AccuAmtRio = AccuAmtRio + DataSimPctAmt.Amount_Ratio[m]
            if AccuAmtRio >= AmtRio[k]:
                AmtPpct[1].append(DataSimPctAmt.Simulate_Paypct[m])
                break
    AmtPpct = pd.DataFrame(AmtPpct, index=['SimAmtRio', 'PayPct']).T
    return AmtPpct

#model for Pay_pct of simulate Credit_amount_Ratio  ##input ALLoutput of sample Pay_pct of simulate Credit_amount_Ratio
def PaypctF(DataAmtPaypct):
    Paypct = pd.DataFrame(index=AmtRio, columns=['SimAmtRio', 'PayPct', 'Rio'])
    for k in range(0, len(AmtRio)):
        Pct = []
        Paypct['SimAmtRio'].iloc[k] = (AmtRio[k])
        for m in range(0, len(DataAmtPaypct['SimAmtRio'])):
            if DataAmtPaypct['SimAmtRio'].iloc[m] == AmtRio[k]:
                Pct.append(DataAmtPaypct['PayPct'].iloc[m])
        Pct = pd.DataFrame(Pct)
        Paypct['PayPct'].iloc[k] = Pct.mode().iloc[0, 0]
    for k in range(0, len(AmtRio) - 1):
        Paypct['Rio'].iloc[k + 1] = (
        float(Paypct['PayPct'].iloc[k] - Paypct['PayPct'].iloc[k + 1]) / float(
            Paypct['SimAmtRio'].iloc[k] - Paypct['SimAmtRio'].iloc[k + 1]))
    return Paypct

#model for opt Credit_term & delinquent ratio  ##input output of credit term of simulate delinquent ratio
def OptPaypctF(DataPaypct):
    MaxRit = DataPaypct['Rio'].min()
    for k in range(0, len(AmtRio)-1):
        if DataPaypct.iloc[k, 1] >= MaxRit:
            OptPaypct = DataPaypct.iloc[k, 0]
    return OptPaypct

##########montecarlo simulate#################
AmtPaypct = pd.DataFrame(columns=['SimAmtRio', 'PayPct'])
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
            SimPctAmt = SimPctAmtF(sample)
            AmtPaypct = AmtPaypct.append(AmtPaypctF(SimPctAmt))
Paypct = PaypctF(AmtPaypct)
OptPaypct = OptPaypctF(Paypct)
AmtPaypct.to_csv('e:/code/python/credit_term/AmtPaypctF.csv',encoding='utf-8',index=False)
Paypct.to_csv('e:/code/python/credit_term/PaypctF.csv',encoding='utf-8',index=False)
print OptPaypct





