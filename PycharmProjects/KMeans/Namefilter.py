#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

#montecarlo Credit_term simulation
#import MySQLdba
import mysql.connector
import pandas as pd
import numpy as np
import re
import csv as csv

#data_prepare
RAW=('''select distinct chain_name
from offlinecentre.z_core_raw_document
where data_version = '20161117151107'
order by chain_name;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))
Chain = []
for i in rdata.chain_name:
    Chain.append(i)

#FilterRule
def ChineseFilter(chain):
    ChinesePattern = re.compile(u'[\u4e00-\u9fa5]')
    MNCchain = []
    CNChain = []
    for i in chain:
        if ChinesePattern.search(i):
            CNChain.append(i)
        else:
            MNCchain.append(i)
    return CNChain,MNCchain

def KeepRule(keepchar,chain):
    Filter = []
    Keep = []
    for i in keepchar:
        for j in chain:
            if re.search(i,j):
                Keep.append(j)
    Filter = [k for k in chain if k not in Keep]
    return Filter,Keep

def FilterRule(filterchar,chain):
    Filter = []
    Keep = []
    for i in filterchar:
        for j in chain:
            if re.search(i,j):
                Filter.append(j)
    Keep = [k for k in chain if k not in Filter]
    return Filter,Keep

def LenFilter(Len,chain):
    Filter = []
    Keep = []
    for i in chain:
        if len(i) <= Len:
            Filter.append(i)
    Keep = [k for k in chain if k not in Filter]
    return Filter,Keep

CNChain,MNCChain = ChineseFilter(Chain)

FilterChar = [u'零售',u'零星',u'废弃',u'待用',u'冻结',u'个体',u'一次性客户',u'旧',u'停用',u'虚拟',
              u'维修项目客户',u'甘肃洞库',u'成品赔偿']
InvalidChain,ValidChain = FilterRule(FilterChar,CNChain)

KeepChar = [u'公司',u'部',u'处',u'店',u'厂',u'所',u'医院',u'院',u'商行',u'中心',u'局',u'委员会',u'队',
            u'仓库',u'幼儿园',u'检察院',u'中学',u'库',u'大学',u'会',u'门市',u'报社',u'大学',u'集团'
            u'企业']
Individual,Company = KeepRule(KeepChar,ValidChain)

CompanyShort,CompanyLong = LenFilter(3,Company)

MNCChain = pd.Series(MNCChain)
InvalidChain = pd.Series(InvalidChain)
Individual = pd.Series(Individual)
Company = pd.Series(Company)
CompanyShort = pd.Series(CompanyShort)
CompanyLong = pd.Series(CompanyLong)

MNCChain.to_csv('e:/code/python/MNCChain.csv',encoding='gbk')
InvalidChain.to_csv('e:/code/python/InvalidChain.csv',encoding='gbk')
Individual.to_csv('e:/code/python/Individual.csv',encoding='gbk')
Company.to_csv('e:/code/python/Company.csv',encoding='gbk')
CompanyLong.to_csv('e:/code/python/CompanyLong.csv',encoding='gbk')
CompanyShort.to_csv('e:/code/python/CompanyShort.csv',encoding='gbk')