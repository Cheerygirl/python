#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import mysql.connector
import pandas as pd
import numpy as np
from time import time
import matplotlib.pyplot as plt

from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.datasets import load_digits
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale

import csv as csv

#data_prepare
RAW=('''select seller_group_name,seller_name,buyer_name,buyer_id,proof_no,proj_name,
bill_time,match_pay_time,bill_amount,paid_amount,
datediff(match_pay_time,bill_time) pay_day,is_all_paid
from modelcentre.m_core_bill_match
where m_version = '20161117151107'
and buyer_name <> ''
order by buyer_name;''')

#connect dataset
cnx = mysql.connector.connect(user='adhoc', password='adhoc@DCFOffLine#9837%',
                              host='offlinecentre.cigru3mivzpd.rds.cn-north-1.amazonaws.com.cn',
                              database='modelcentre',
                              use_unicode=True)

# read_data
rdata = pd.DataFrame(pd.read_sql(RAW,con=cnx))

#index prepare
ChainNum = rdata['bill_amount'].groupby(rdata['buyer_name']).count()
ChainAmt = rdata['bill_amount'].groupby(rdata['buyer_name']).sum()
ChainBillAvgamt = rdata['bill_amount'].groupby(rdata['buyer_name']).mean()
ChainBillMaxamt = rdata['bill_amount'].groupby(rdata['buyer_name']).max()
ChainBillStdamt = rdata['bill_amount'].groupby(rdata['buyer_name']).std()
ChainBillByamt = ChainBillStdamt/ChainBillAvgamt
ChainPaynum = rdata['pay_day'].groupby(rdata['buyer_name']).count()
ChainAvgpayday = rdata['pay_day'].groupby(rdata['buyer_name']).mean()
ChainMaxpayday = rdata['pay_day'].groupby(rdata['buyer_name']).max()
ChainStdpayday = rdata['pay_day'].groupby(rdata['buyer_name']).std()
ChainBypayday = ChainStdpayday/ChainAvgpayday
ChainName = pd.Series(ChainNum.index,index=ChainNum.index)
ChainIndex = pd.DataFrame({'ChainName':ChainName, 'Num':ChainNum, 'Amt':ChainAmt,
                           'BillAvgamt':ChainBillAvgamt,'BillMaxamt':ChainBillMaxamt,
                           'BillStdamt':ChainBillStdamt,'BillByamt':ChainBillByamt,
                           'PayNum':ChainPaynum,
                           'AvgPayday':ChainAvgpayday, 'StdPayday':ChainStdpayday,
                           'MaxPayday':ChainMaxpayday,'ByPayday':ChainBypayday})

ChainNumLess1 = ChainIndex[ChainIndex['PayNum']<=1]
ChainIndex = ChainIndex[ChainIndex['PayNum']>1]

###############KMeans###############
ModelIndex = ChainIndex[['BillAvgamt','BillStdamt','AvgPayday','StdPayday','MaxPayday']]
ScaleIndex = scale(ModelIndex)
SampleNum, FeatureNum = ScaleIndex.shape
labels = ChainIndex.index
ClusterNum = 20

print(79 * '_')
print('% 9s' % 'init'
      '    time  inertia    homo   compl  v-meas     ARI AMI  silhouette')

def bench_k_means(estimator, name, data):
    t0 = time()
    estimator.fit(data)
    print('% 9s   %.2fs    %i   %.3f   %.3f   %.3f   %.3f   %.3f    %.3f'
          % (name, (time() - t0), estimator.inertia_,
          metrics.homogeneity_score(labels, estimator.labels_),
          metrics.completeness_score(labels, estimator.labels_),
          metrics.v_measure_score(labels, estimator.labels_),
          metrics.adjusted_rand_score(labels, estimator.labels_),
          metrics.adjusted_mutual_info_score(labels,  estimator.labels_),
          metrics.silhouette_score(data, estimator.labels_,
                                   metric='euclidean',
                                   sample_size=SampleNum)))

    return estimator.fit_predict(data).astype(int)


clusterF = bench_k_means(KMeans(init='k-means++', n_clusters=ClusterNum, n_init=10),
              name="k-means++", data=ScaleIndex)
clusterF = pd.Series(clusterF,index=ModelIndex.index)
ClusterOutputF = ModelIndex
ClusterOutputF['Cluster'] = clusterF

clusterS = bench_k_means(KMeans(init='random', n_clusters=ClusterNum, n_init=10),
              name="random", data=ScaleIndex)
clusterS = pd.Series(clusterS,index=ModelIndex.index)
ClusterOutputS = ModelIndex
ClusterOutputS['Cluster'] = clusterS

ClusterOutputF.to_csv('e:/code/clusterF.csv',encoding='utf-8',index=True)
ClusterOutputS.to_csv('e:/code/clusterS.csv',encoding='utf-8',index=True)

#######KMeans with PCA##############
ClusterNum = 5 #ClusterNumber must > Features
# in this case the seeding of the centers is deterministic, hence we run the
# kmeans algorithm only once with n_init=1
pca = PCA(n_components=ClusterNum).fit(ScaleIndex)
print(pca.explained_variance_ratio_)
clusterT = bench_k_means(KMeans(init=pca.components_, n_clusters=ClusterNum, n_init=1),
              name="PCA-based",
              data=ScaleIndex)
clusterT = pd.Series(clusterT,index=ModelIndex.index)
ClusterOutputT = ModelIndex
ClusterOutputT['Cluster'] = clusterT

ReducedScaleIndex = PCA(n_components=2).fit_transform(ScaleIndex)
clusterFr = bench_k_means(KMeans(init='k-means++', n_clusters=ClusterNum, n_init=10),
              name="PCA-Reduced",
              data=ReducedScaleIndex)
clusterFr = pd.Series(clusterFr,index=ModelIndex.index)
ClusterOutputFr = ModelIndex
ClusterOutputFr['Cluster'] = clusterFr

ClusterOutputFr.to_csv('e:/code/clusterFr.csv',encoding='utf-8',index=True)
ClusterOutputT.to_csv('e:/code/clusterT.csv',encoding='utf-8',index=True)

print(79 * '_')
