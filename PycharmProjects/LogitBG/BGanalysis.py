#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

LPdata = pd.read_csv('e:/code/python/Logit/LoanPay.csv',encoding='utf-8')
LUdata = pd.read_csv('e:/code/python/Logit/LineUsage.csv',encoding='utf-8')

#chain_uuid,core_name,institution_name,
#amount,loan_amount,write_off_amount,payment_amount,overdue_amount,unpaid_principle,
#ar_start_date,ar_end_date,apply_date,lending_date,loan_due_date,write_off_date,factoring_due_date,
#loan_days,overdue_days,remain_day,
#loan_document_state,is_write_off,is_overdue

#chain_uuid,core_name,institution_name,calendar_date,credit_line,loan_amount,
#write_off_amount,loan_balance,credit_line_usage_rate

BadDebt = sum(LPdata.loan_amount[LPdata.is_write_off==0])
Sumamount = sum(LPdata.loan_amount)
SimOverdue = range(0,201,1)
SimODday = pd.DataFrame(index=range(0,len(SimOverdue)),columns=['SimODday','DlqAmt','BadDebtRio','DlqRio'])
k = 0
for i in SimOverdue:
    SimODday['SimODday'].iloc[i] = i
    SimODday['DlqAmt'].iloc[i] = sum(LPdata.overdue_amount[LPdata.overdue_days>SimOverdue[i]])
    SimODday['BadDebtRio'].iloc[i] = BadDebt/(SimODday['DlqAmt'].iloc[i]+BadDebt)
    SimODday['DlqRio'].iloc[i] = (SimODday['DlqAmt'].iloc[i]+BadDebt)/Sumamount
SimODday.to_csv('e:/code/python/Logit/SimODay.csv',encoding='utf-8',index=False)
plt.plot(SimODday['SimODday'],SimODday['BadDebtRio'],color='blue',linewidth=2.5,linestyle='-')
plt.plot(SimODday['SimODday'],SimODday['DlqRio'],color='red',linewidth=2.5,linestyle='-')
plt.show()

##################################################

BadDebtBill = LPdata[LPdata.is_write_off==0]
BadDebtChain = []
for i in range(0,len(BadDebtBill.index)):
    if BadDebtBill['chain_uuid'].iloc[i] not in BadDebtChain:
        BadDebtChain.append(BadDebtBill['chain_uuid'].iloc[i])

def ChainNumF(Bill):
    Chain = []
    for i in range(0, len(Bill.index)):
        if Bill['chain_uuid'].iloc[i] not in Chain:
            Chain.append(Bill['chain_uuid'].iloc[i])
    return Chain

SimOverdue = range(0,201,1)
SimODNum = pd.DataFrame(index=range(0,len(SimOverdue)),columns=['SimODday','DlqCNum','BadDebtCRio','DlqCNumRio'])
k = 0
for i in SimOverdue:
    SimODNum['SimODday'].iloc[i] = i
    SimODNum['DlqCNum'].iloc[i] = len(ChainNumF(LPdata[LPdata.overdue_days>SimOverdue[i]]))
    SimODNum['BadDebtCRio'].iloc[i] = float(len(BadDebtChain))/float((SimODNum['DlqCNum'].iloc[i]+len(BadDebtChain)))
    SimODNum['DlqCNumRio'].iloc[i] = float((SimODNum['DlqCNum'].iloc[i]+len(BadDebtChain)))/float(len(ChainNumF(LPdata)))

SimODNum.to_csv('e:/code/python/Logit/SimODNum.csv',encoding='utf-8',index=False)
plt.plot(SimODNum['SimODday'],SimODNum['BadDebtCRio'],color='blue',linewidth=2.5,linestyle='-')
plt.plot(SimODNum['SimODday'],SimODNum['DlqCNumRio'],color='red',linewidth=2.5,linestyle='-')
plt.show()