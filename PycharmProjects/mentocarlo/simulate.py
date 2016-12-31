#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np
import random

#data_prepare
RAW=('''select seller_group_name,seller_name,buyer_name,
datediff(match_pay_time,bill_time) pay_day,bill_amount
from modelcentre.m_core_bill_match
where m_version = '20160913184422'
and match_pay_time is not null
and datediff(due_time,bill_time)>0
order by buyer_name;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))

#data processing: credit_order&match_pay_time_is_not_null&no_abnormal_data
#avgpayday = np.nanmean(rdata.loc[:,'pay_day'])
#stdpayday = np.nanstd(rdata.loc[:,'pay_day'])
#cldatacd = (rdata.pay_day >= avgpayday-3*stdpayday) & (rdata.pay_day <= avgpayday+3*stdpayday)
#cldata = rdata[cldatacd]
ChainNum = rdata['bill_amount'].groupby(rdata['buyer_name']).count()
ChainAmt = rdata['bill_amount'].groupby(rdata['buyer_name']).sum()
Chainpayday = rdata['pay_day'].groupby(rdata['buyer_name']).mean()
Chainmaxpayday = rdata['pay_day'].groupby(rdata['buyer_name']).max()
ChainName = pd.Series(ChainNum.index,index=ChainNum.index)
ChainIndex = pd.DataFrame([ChainName,ChainNum,ChainAmt,Chainpayday,Chainmaxpayday]).T
ChainIndex.to_csv('e:\ChainIndex.csv',encoding='utf-8',index=False)



#montecarlo simulate for just sampling from rawdata
#model setting data
SamTim = 10000 #simulating times
SamNum = 1000 #sample numbers
DlqRio = [0.005] #Simulating delinqune ratio
DataNum = len(cldata)-1 #rawdata numbers
DlqTerm = [[],[]]
CreditTerm = pd.DataFrame(index=DlqRio,columns=['SimDlq','Credit_term','Rio'])

for i in range(0,SamTim):

    # sampling
    sampleid = np.random.choice(DataNum, SamNum, replace=False)
    sample = cldata.iloc[sampleid,:]

    #model of simulate credit term and delinquent amount
    SimCt = range(0, 801, 1)
    SimDlq = []
    SimDlqRio = []
    SimCtDlq = []
    SimCtDlqData = pd.DataFrame(columns = ['Simulate_Term', 'Delinquent_amount','Delinquent_Ratio'])
    Sumamt = sum(sample.loc[:, 'bill_amount'])
    for j in SimCt:
        SimDlq.append(sum(sample.bill_amount[sample.pay_day >= SimCt[j]]))
        SimDlqRio.append(SimDlq[j] / Sumamt)
    SimCtDlq.append(SimCt)
    SimCtDlq.append(SimDlq)
    SimCtDlq.append(SimDlqRio)
    SimCtDlq = pd.DataFrame(SimCtDlq, index = ['Simulate_Term', 'Delinquent_amount','Delinquent_Ratio'])
    SimCtDlq = SimCtDlq.T
    SimCtDlqData.append(SimCtDlq)

    #model for sample credit term of simulate delinquent ratio
    for k in range(0,len(DlqRio)):
        DlqTerm[0].append(DlqRio[k])
        for m in range(0,len(SimDlqRio)):
            if SimCtDlq.Delinquent_Ratio[m] <= DlqRio[k]:
                DlqTerm[1].append(SimCtDlq.Simulate_Term[m])
                break

#data save
DlqTerm = pd.DataFrame(DlqTerm,index = ['SimDlq','Credit_term']).T
DlqTerm.to_csv('e:/code/python/credit_term/DlqTerm.csv',encoding='utf-8',index=False)
SimCtDlqData.to_csv('e:/code/python/credit_term/SimCtDlq.csv',encoding='utf-8',index=False)

#mode
def get_mode(arr):
    mode = []
    arr_appear = dict((a, arr.count(a)) for a in arr)
    if max(arr_appear.values()) == 1:
        return
    else:
        for k, v in arr_appear.items():
            if v == max(arr_appear.values()):
                mode.append(k)
    return mode

#model for credit term of simulate delinquent ratio
for k in range(0,len(DlqRio)):
    Term = []
    CreditTerm['SimDlq'].iloc[k] = (DlqRio[k])
    for m in range(0,len(DlqTerm['SimDlq'])):
        if DlqTerm['SimDlq'].iloc[m] == DlqRio[k]:
            Term.append(DlqTerm['Credit_term'].iloc[m])
    Term = pd.DataFrame(Term)
    CreditTerm['Credit_term'].iloc[k] = Term.mode().iloc[0,0]
for k in range(0,len(DlqRio)-1):
    CreditTerm['Rio'].iloc[k+1] =(float(CreditTerm['Credit_term'].iloc[k]-CreditTerm['Credit_term'].iloc[k+1])/float(CreditTerm['SimDlq'].iloc[k]-CreditTerm['SimDlq'].iloc[k+1]))
CreditTerm.to_csv('e:/code/python/credit_term/CreditTerm.csv',encoding='utf-8',index=False)


# change max the optCreditTerm
MaxRit = CreditTerm['Rio'].min()
for k in range(0,len(DlqRio)-1):
    if CreditTerm.iloc[k,1] >= MaxRit:
        OptCreditTerm = CreditTerm['Credit_term'].iloc[k]
print OptCreditTerm

