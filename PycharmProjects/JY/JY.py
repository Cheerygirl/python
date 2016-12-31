#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np
import json
import itertools

################data extract form database###################
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
avgpayday = np.nanmean(rdata.loc[:,'pay_day'])
stdpayday = np.nanstd(rdata.loc[:,'pay_day'])
cldatacd = (rdata.pay_day >= avgpayday-3*stdpayday) | (rdata.pay_day <= avgpayday-3*stdpayday)
cldata = rdata[cldatacd]

#################data extract from output############
FilePath = 'e:/code/JY/20161029/jCoreBillMatch.json'
JYdata = pd.DataFrame()
i = 0
with open(FilePath) as Fil:
    for line in Fil:
#        lines = [line] + list(itertools.islice(Fil,0))
#        jfile = json.loads(''.join(lines))
#        print line
        JYdata[i] = pd.read_json(line,typ='series')
        i = i + 1
#print  jfile.keys()
#print  jfile['bill_amount']
JYdata = JYdata.T

#################JY#####################
rcount = rdata['bill_amount'].groupby([rdata['buyer_name']]).count()
ramount = rdata['bill_amount'].groupby([rdata['buyer_name']]).sum()
JYcount = JYdata['bill_amount'].groupby([JYdata['buyer_name']]).count()
JYamount = JYdata['bill_amount'].groupby([JYdata['buyer_name']]).sum()
compare = pd.DataFrame([rcount,ramount,JYcount,JYamount]).T
compare.to_csv('e:/code/JY/compare.csv',encoding='utf-8',index=False)

#with open('part.json') as fin:
#    data = pd.DataFrame(json.loads(line) for line in fin)

