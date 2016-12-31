#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
from time import time
import matplotlib.pyplot as plt
import xlrd

from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.datasets import load_digits
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale

import csv as csv

#data_prepare
RAW=('''SELECT chain_name,circle_start_date,circle_end_date,
bill_amount,bill_number,circle_start_ar,circle_end_ar,
datediff(circle_end_date,circle_start_date) cycle
FROM modelcentre.o_ar_circle
where m_version = 'sparku-20161105103251'
and circle_end_ar is not null
and bill_amount > 0;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))
Chain = np.unique(rdata['chain_name'])


