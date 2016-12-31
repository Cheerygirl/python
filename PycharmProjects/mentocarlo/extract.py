#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
import random
import matplotlib.pyplot as plt

#data_prepare
RAW=('''select seller_group_name,seller_name,buyer_name,
datediff(match_pay_time,bill_time) pay_day,bill_amount
from modelcentre.m_core_bill_match
where m_version = '20160913184422'
and buyer_name = %(buyer_name)s
and match_pay_time is not null
and datediff(due_time,bill_time)>0
order by buyer_name;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
buyer_name = u'郑州丹尼斯'
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx,params={'buyer_name':buyer_name}))
rdata.to_csv('e:/code/python/credit_term/rdata.csv',encoding='utf-8',index=False)
def LogNor(DataFra):
    avgpayday = np.nanmean(DataFra.loc[:,'pay_day'])
    varpayday = np.nanvar(DataFra.loc[:,'pay_day'])
    Avgpayday = np.log(avgpayday) - np.log(varpayday/pow(avgpayday,2)+1)/2
    Stdpayday = pow(np.log(varpayday/pow(avgpayday,2)+1),0.5)
    return Avgpayday,Stdpayday,avgpayday,pow(varpayday,0.5)

[LogNorAvg,LogNorStd,AvgP,StdP] = LogNor(rdata)

s = np.random.lognormal(LogNorAvg, LogNorStd, 1000000)
count, bins, ignored = plt.hist(s, 100, normed=True, align='mid')
x = np.linspace(min(bins), max(bins), 10000)
pdf = (np.exp(-(np.log(x) - LogNorAvg)**2 / (2 * LogNorStd**2)) / (x * LogNorStd * np.sqrt(2 * np.pi)))
plt.plot(x,pdf, linewidth=2, color='m')
plt.axis('tight')


count, bins, ignored = plt.hist(rdata['pay_day'], 100, normed=True, align='mid')
x1 = np.linspace(min(bins), 500, 10000)
pdf1 = (np.exp(-(np.log(x1) - LogNorAvg)**2 / (2 * LogNorStd**2)) / (x * LogNorStd * np.sqrt(2 * np.pi)))
plt.plot(x1,pdf1, linewidth=2, color='r')
plt.axis('tight')
plt.show()