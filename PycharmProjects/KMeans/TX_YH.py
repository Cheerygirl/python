#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import pandas as pd
import numpy as np
from time import time
import matplotlib.pyplot as plt

FileName = u'东方雨虹结果'
FilePath = 'e:/code/'+FileName+'.csv'

result = pd.read_csv(FilePath,encoding='gbk')
result.columns = ['core_name','chain_id','ar_date','ar_amount','match_day','bill_amount','avg_aramount','match_date']
result.ar_date = pd.to_datetime(result.ar_date)
result.match_date = pd.to_datetime(result.match_date)
Chain = np.unique(result['chain_id'])


for i in Chain:
    ChainResult = result[result.chain_id==i]
    ChainResult = ChainResult.dropna(axis=0)
    ChainResult = ChainResult.sort_values('ar_date')
    for j in range(len(ChainResult)):
        ChainResult.ar_date.iloc[i]

def DateTree()







