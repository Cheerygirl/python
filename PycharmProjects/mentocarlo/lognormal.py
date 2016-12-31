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

billNum = rawdata['bill_amount'].groupby([rawdata['buyer_name']]).count()
Chain = pd.Series(billNum.index,index=billNum.index)
BillNum = pd.DataFrame([Chain,billNum],index=['buyer_name','bill_number']).T
AnaChain = BillNum.buyer_name[BillNum['bill_number']>=30]

#get var and mean
def DataAbnormalDrop(DataF):
    avgpayday = np.nanmean(DataF.loc[:,'pay_day'])
    stdpayday = np.nanvar(DataF.loc[:,'pay_day'])

for i in range(0,len(chainname)):






#data cleaning: credit_order&match_pay_time_is_not_null&no_abnormal_data
#def DataAbnormalDrop(rawdata):
#    avgpayday = np.nanmean(rawdata.loc[:,'pay_day'])
#    stdpayday = np.nanstd(rawdata.loc[:,'pay_day'])
#    cldatacd = (rawdata.pay_day >= avgpayday-3*stdpayday) & (rawdata.pay_day <= avgpayday+3*stdpayday)
#    cldata = rawdata[cldatacd]
#    return cldata

#data_cleaning
#cdata = DataAbnormalDrop(rawdata)