#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

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

#Data prepare
LPdata['ar_start_date'] = pd.to_datetime(LPdata['ar_start_date'])
LPdata['ar_end_date'] = pd.to_datetime(LPdata['ar_end_date'])
LPdata['apply_date'] = pd.to_datetime(LPdata['apply_date'])
LPdata['lending_date'] = pd.to_datetime(LPdata['lending_date'])
LPdata['loan_due_date'] = pd.to_datetime(LPdata['loan_due_date'])
LPdata['write_off_date'] = pd.to_datetime(LPdata['write_off_date'])
LPdata['factoring_due_date'] = pd.to_datetime(LPdata['factoring_due_date'])

LPdata.info()

#order index
LPdata['pay_day'] = LPdata['write_off_date'] - LPdata['lending_date']
LPdata['credit_term'] = LPdata['loan_due_date'] - LPdata['lending_date']
LPdata['overdue_day'] = LPdata['write_off_date'] -LPdata['loan_due_date']
#LPdata.overdue_day[LPdata['overdue_day']<=0] = None

#chain_index
ChainGp = LPdata.groupby(LPdata['chain_uuid'])
Nowadays = pd.Timestamp('2016-10-01')
#-------------------------------------
ChainNum = ChainGp['loan_amount'].count()
ChainNumPr = ChainNum/LPdata['loan_amount'].count
ChainAmt = ChainGp['loan_amount'].sum()
ChainAmtPr = ChainAmt/LPdata['loan_amount'].sum
#-------------------------------------
ChainBillAvgamt = ChainGp['loan_amount'].mean()
ChainBillMaxamt = ChainGp['loan_amount'].max()
ChainBillMaxamt = ChainGp['loan_amount'].min()
ChainBillStdamt = ChainGp['loan_amount'].std()
ChainBillByamt = ChainBillStdamt/ChainBillAvgamt
#--------------------------------------
ChainPaynum = ChainGp['pay_day'].count()
ChainAvgpayday = ChainGp['pay_day'].mean()
ChainMaxpayday = ChainGp['pay_day'].max()
ChainMaxpayday = ChainGp['pay_day'].min()
ChainStdpayday = ChainGp['pay_day'].std()
ChainBypayday = ChainStdpayday/ChainAvgpayday
#---------------------------------------


#--------------------------------------
ChainBillFirstDate = ChainGp['lending_date'].min()
ChainBillLastDate = ChainGp['lending_date'].max()
ChainBillSpan = ChainGp['lending_date'].max() - ChainGp['lending_date'].min()
ChainBillDay = ChainGp['lending_date'].apply(lambda x: len(x.unique()))
ChainAvgBillGap = ChainBillSpan/ChainBillDay
ChainBillWotSpan = Nowadays - ChainBillLastDate
#--------------------------------------
ChainPayFirstDate = ChainGp['write_off_date'].min()
ChainPayLastDate = ChainGp['write_off_date'].max()
ChainPaySpan = ChainGp['write_off_date'].max() - ChainGp['write_off_date'].min()
ChainPayDay = ChainGp['write_off_date'].apply(lambda x: len(x.unique()))
ChainAvgPayGap = ChainBillSpan/ChainBillDay
ChainPayWotSpan = Nowadays - ChainPayLastDate
#--------------------------------------
ChainDate = pd.DataFrame({'BillFirstDate':ChainBillFirstDate,'BillLastDate':ChainBillLastDate,
                          'PayFirstDate':ChainPayFirstDate,'PayLastDate':ChainPayLastDate})
write_off_date = dict(list(ChainGp['write_off_date']))
lending_date = dict(list(ChainGp['lending_date']))
def DictMerge(dict1,dict2):
    newdict = {}
    newdict.update(dict1)
    newdict.update(dict2)
    for key in dict1.keys():
        if key in dict2:
            newdict[key] = tuple(list(dict2[key]) + list(dict2[key]))
    return newdict
TradeDate = DictMerge(lending_date,write_off_date)

ChainTradeFirstDate = ChainDate.min(axis=1)
ChainTradeLastDate = ChainDate.max(axis=1)
ChainTradeSpan = ChainTradeLastDate - ChainTradeLastDate
ChainTradeWotSpan = Nowadays - ChainTradeLastDate
#--------------------------------------

#chain window index
windows = range(1,100)
