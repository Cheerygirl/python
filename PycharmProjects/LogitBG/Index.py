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
LPdata['pay_days'] = LPdata['write_off_date'] - LPdata['lending_date']
LPdata['credit_term'] = LPdata['loan_due_date'] - LPdata['lending_date']
LPdata['overdue_days'] = LPdata['write_off_date'] -LPdata['loan_due_date']
LPdata.overdue_day[LPdata['overdue_days']<=0] = None