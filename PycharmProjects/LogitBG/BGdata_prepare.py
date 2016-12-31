#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np

#data_prepare
LoanPay = ('''SELECT chain_uuid,core_name,institution_name,
amount,loan_amount,write_off_amount,payment_amount,overdue_amount,unpaid_principle,
ar_start_date,ar_end_date,apply_date,lending_date,loan_due_date,write_off_date,factoring_due_date,
loan_days,overdue_days,remain_day,
loan_document_state,is_write_off,is_overdue
FROM analysis.s_loan_accounting;''')

LineUsage = ('''SELECT chain_uuid,core_name,institution_name,calendar_date,credit_line,loan_amount,
write_off_amount,loan_balance,credit_line_usage_rate
FROM analysis.s_credit_line_usage;''')

#connect dataset
cnx = mysql.connector.connect(user='analysis', password='adhoc@DCFTest#9837%',
                              host='application2.datacube.dcf.net',
                              database='analysis',
                              use_unicode=True)

# read_data
LPdata = pd.DataFrame(pd.read_sql(LoanPay,con=cnx))
LUdata = pd.DataFrame(pd.read_sql(LineUsage,con=cnx))

LPdata.to_csv('e:/code/python/Logit/LoanPay.csv',encoding='utf-8',index=False)
LUdata.to_csv('e:/code/python/Logit/LineUsage.csv',encoding='utf-8',index=False)