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

ChainNum = rawdata['bill_amount'].groupby(rawdata['buyer_name']).count()
ChainAmt = rawdata['bill_amount'].groupby(rawdata['buyer_name']).sum()
Chainavgpayday = rawdata['pay_day'].groupby(rawdata['buyer_name']).mean()
Chainstdpayday = rawdata['pay_day'].groupby(rawdata['buyer_name']).std()
ChainName = pd.Series(ChainNum.index,index=ChainNum.index)
ChainIndex = pd.DataFrame({'ChainName':ChainName, 'BillNum':ChainNum, 'BillAmt':ChainAmt,'AvgPayday':Chainavgpayday, 'StdPayday':Chainstdpayday})
Payday = ChainIndex.AvgPayday.mean()
StdPayday = ChainIndex.StdPayday.mean()
Chainup30 = ChainIndex.ChainName[ChainIndex['BillNum']>=30]
#Chainunder = ChainIndex.ChainName[(ChainIndex['']>=30)]

cdata = pd.DataFrame(columns=rawdata.columns)
for i in Chainup30:
    cdata = cdata.append(rawdata[rawdata['buyer_name']==i])

#model prepare
SimCt = range(0, 850, 1)
DlqRio = [0.005]  # Simulating delinqune ratio

############model of simulate credit term and delinquent amount###########
def SimCtDlqF(DataFra,ChainName=None):
    SimDlq = []
    SimDlqRio = []
    SimCtDlq = []
    SimChainname = []
    Sumamt = sum(DataFra.loc[:, 'bill_amount'])
    if ChainName == None:
        for j in SimCt:
            SimDlq.append(sum(DataFra.bill_amount[DataFra.pay_day > SimCt[j]]))
            SimDlqRio.append(SimDlq[j] / Sumamt)
        SimCtDlq = pd.DataFrame({'Simulate_Term': SimCt, 'Delinquent_amount': SimDlq,'Delinquent_Ratio': SimDlqRio})
    else:
        for j in SimCt:
            SimDlq.append(sum(DataFra.bill_amount[DataFra.pay_day > SimCt[j]]))
            SimDlqRio.append(SimDlq[j] / Sumamt)
            SimChainname.append(ChainName)
        SimCtDlq = pd.DataFrame({'Chain_name':SimChainname, 'Simulate_Term':SimCt, 'Delinquent_amount':SimDlq, 'Delinquent_Ratio':SimDlqRio})
    return SimCtDlq

############# model for sample credit term of simulate delinquent ratio  ## input output of model of simulate credit term and delinquent amount
def DltTermF(DataSimCtDlq,DlqRio,ChainName=None):
    DataRio = DataSimCtDlq.Delinquent_Ratio
    chainname = []
    dlqrio = []
    term = []
    if ChainName == None:
        dlqrio.append(DlqRio)
        for m in range(0, len(DataRio)):
            if DataSimCtDlq.Delinquent_Ratio[m] <= DlqRio:
                term.append(DataSimCtDlq.Simulate_Term[m])
                break
        DlqTerm = pd.DataFrame({'SimDlq': dlqrio, 'Credit_term': term})
    else:
        chainname.append(ChainName)
        dlqrio.append(DlqRio)
        for m in range(0, len(DataRio)):
            if DataSimCtDlq.Delinquent_Ratio[m] <= DlqRio:
                term.append(DataSimCtDlq.Simulate_Term[m])
                break
        DlqTerm = pd.DataFrame({'Chain_name':chainname, 'SimDlq':dlqrio,  'Credit_term':term})
    return DlqTerm

# model for sample delinquent ratio of simulate credit term  ## input output of model of simulate credit term and delinquent amount
def TermDlqF(DataSimCtDlq,Term,DlqRio,ChainName=None):
    DataTerm = DataSimCtDlq.Simulate_term
    chainname = []
    dlqrio = []
    term = []
    Tdlqrio = []
    if ChainName == None:
        dlqrio.append(DlqRio)
        term.append(Term)
        for m in range(0, len(DataTerm)):
            if DataSimCtDlq.Simulate_term[m] >= Term:
                Tdlqrio.append(DataSimCtDlq.Delinquent_amount[m])
                break
        TermDlq = pd.DataFrame({'SimDlq': dlqrio, 'Credit_term': term, 'Delinquent_amount': Tdlqrio})
    else:
        chainname.append(ChainName)
        dlqrio.append(DlqRio)
        term.append(Term)
        for m in range(0, len(DataTerm)):
            if DataSimCtDlq.Simulate_term[m] >= Term:
                Tdlqrio.append(DataSimCtDlq.Delinquent_amount[m])
                break
        TermDlq = pd.DataFrame({'Chain_name':chainname, 'SimDlq':dlqrio, 'Credit_term':term, 'Delinquent_amount':Tdlqrio})
    return TermDlq

#######Chain Credit term of simulate delinquent ratio###############

ChainDlqTerm = pd.DataFrame(columns=['Chain_name','SimDlq', 'Credit_term'])
for i in range(0,len(DlqRio)):
    for j in Chainup30.values:
        Bill = cdata[cdata['buyer_name']==j]
        SimCtDlq = SimCtDlqF(Bill,j)
        ChainDlqTerm = ChainDlqTerm.append(DltTermF(SimCtDlq,DlqRio[i],j),ignore_index=True)
SimCtDlq.to_csv('e:/code/python/credit_term/ChainSimCtDlq.csv',encoding='utf-8',index=False)
ChainDlqTerm.to_csv('e:/code/python/credit_term/ChainDlqTerm.csv',encoding='utf-8',index=False)

###########Chain credit term Adjust###########

ChainCt = ChainDlqTerm
ChainCt['Adterm'] = np.zeros(len(ChainCt['Chain_name']))
for i in range(0,len(DlqRio)):
    for j in Chainup30.values:
        cd = (ChainCt.SimDlq == DlqRio[i]) & (ChainCt.Chain_name == j)
        if (ChainCt.Credit_term[cd]<=31):
            ChainCt.Adterm[cd] = 30
        elif (ChainCt.Credit_term[cd]<=61):
            ChainCt.Adterm[cd] = 60
        elif (ChainCt.Credit_term[cd]<=91):
            ChainCt.Adterm[cd] = 90
        elif (ChainCt.Credit_term[cd]<=121):
            ChainCt.Adterm[cd] = 120
        elif (ChainCt.Credit_term[cd]<=151):
            ChainCt.Adterm[cd] = 150
        elif (ChainCt.Credit_term[cd]<=181):
            ChainCt.Adterm[cd] = 180
        elif (ChainCt.Credit_term[cd]<=271):
            ChainCt.Adterm[cd] = 270
        elif (ChainCt.Credit_term[cd]<=361):
            ChainCt.Adterm[cd] = 360
        else: ChainCt.Adterm[cd] = 540
ChainCt.to_csv('e:/code/python/credit_term/ChainCt.csv', encoding='utf-8', index=False)

############stastic pool###################

def StasticSample(DlqRio,Cato):
    Bill = pd.DataFrame(columns=cdata.columns)
    cd = ChainCt['Adterm']==Cato & ChainCt['SimDlq']==DlqRio
    for i in ChainCt.Chain_name[cd]:
            Bill = Bill.append(cdata[cdata['buyer_name']==i])
    return Bill

##########simulation#########
SimTim = 1
RioTim = len(DlqRio)
Cato = [30,60,90,120,150,180,270,360,540]
SampleNumber = [500,1000,1500]
SimDlq = []
SimTim = []
SimCato = []
SamNum = []
SimRatio = []
for i in range(0,len(DlqRio)):
    for j in range(0,SimTim):
        for k in Cato:
            Bill = StasticSample(DlqRio[i],j)
            BillNum = len(Bill['bill_amount'])
            for m in SampleNumber:
                SimDlq.apppend(DlqRio[i])
                SimTim.append(j)
                SimCato.append(k)
                SamNum.append(m)
                sampleid = np.random.choice(BillNum, m, replace=False)
                sample = Bill.iloc[sampleid,:]
                SampleAmount = sum(sample.bill_amount)
                SimCtDlqM = SimCtDlqF(sample)
                SampleTermDlq = DltTermF(SimCtDlqM, Cato, DlqRio[i])
                OverdueAmount = sum(SampleTermDlq.Delinquent_amount)
                OverdueRatio = OverdueAmount/SampleAmount
                SimRatio.append(OverdueRatio)
SampleRatio = pd.DataFrame({'SimDlq':SimDlq, 'SimTim':SimTim, 'SimCato':SimCato, 'SamNum':SamNum, 'SimRatio':SimRatio})

SimCtDlqM.to_csv('e:/code/python/credit_term/SimCtDlqMF2.csv',encoding='utf-8',index=False)
SampleTermDlq.to_csv('e:/code/python/credit_term/TermDlqMF2.csv',encoding='utf-8',index=False)
SampleRatio.to_csv('e:/code/python/credit_term/SimDisMF2.csv',encoding='utf-8',index=False)



