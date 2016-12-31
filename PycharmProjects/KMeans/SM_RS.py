#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import pandas as pd
import numpy as np
from time import time
import matplotlib.pyplot as plt

FileName = u'神州数码（中国）有限公司结果'
FilePath = 'e:/code/'+FileName+'.csv'

result = pd.read_csv(FilePath,encoding='gbk')
result = result.drop(result.columns[-1],axis=1)
result.columns = ['core_name','chain_id','ar_date','ar_amount','match_day','bill_amount','match_date']
result.ar_date = pd.to_datetime(result.ar_date)
result.match_date = pd.to_datetime(result.match_date)
result['YM'] = result.ar_date.map(lambda x: x.strftime('%Y-%m'))
Chain = np.unique(result['chain_id'])

#
Cycle = result
tmpCycle = pd.DataFrame(columns=Cycle.columns)
for i in Chain:
    ChainCycle = Cycle[Cycle.chain_id == i]
    ChainCycle = ChainCycle.sort_values('ar_date')
    temcycle = ChainCycle
    print i
    j = 0
    for k in ChainCycle.index[0:-2]:
        if((ChainCycle.match_date.iloc[j] == ChainCycle.match_date.iloc[j+1]) & (ChainCycle.YM.iloc[j] == ChainCycle.YM.iloc[j+1])):
            temcycle = temcycle.drop(k)
        j = j + 1
    tmpCycle = tmpCycle.append(temcycle)

tmpCycle.to_csv('e:/code/python/CycleSM.csv', encoding='gbk')