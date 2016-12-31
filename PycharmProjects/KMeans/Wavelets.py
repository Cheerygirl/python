#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
import wavelets
import scipy
from scipy import signal
from time import time
import matplotlib.pyplot as plt

#data_prepare
RAW=('''SELECT core_group_name,core_name,chain_name,coreerpchainuid,
bydate,ar_amount
FROM modelcentre.m_ar_match
where m_version = '20161117151107'
and coreerpchainuid = '2004378';''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))

ArTime = rdata['bydate'].values
ArDt = 1
ArAmount = scipy.signal.detrend(rdata['ar_amount'].values,axis=-1,type='linear',bp=0)

#Wavelet
#wa = wavelets.WaveletAnalysis(rdata['ar_amount'].values,time=rdata['bydate'].values,dt=1)
wa = wavelets.WaveletAnalysis(ArAmount,time=ArTime,dt=1)
#wavelet power spectrum
power = wa.wavelet_power
#wavelet scales
scales = wa.scales
#associated time vector
t = wa.time
#reconstruction of the original data
rx = wa.reconstruction()

#plot
fig,ax = plt.subplots()
ax.contourf(t,scales,power,100)
ax.set_yscale('log')
fig.savefig('e:/2004378.png')
