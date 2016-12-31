#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import numpy as np

def reression(testPoint,trainSet,targetSet):
    xMat = np.mat(trainSet);yMat = np.mat(targetSet).T;
    weight = lineRegression(xMat,yMat);
    yHat = xMat*weight;
    corrMat = np.corrcoef(yHat.T,yMat);
    '''
        |correlationCoefficient| > 0.8时称为高度相关，
        当0.5< |correlationCoefficient|<0.8时称为显著相关，
        当 0.3<|correlationCoefficient|<0.5时，成为低度相关，
        当 |correlationCoefficient| < 0.3时，称为无相关
    '''
    if corrMat[0,1] < 0.5:
        return LWLRegresssion(testPoint,xMat,yMat);
    else:
        return testPoint*weight;

def lineRegression(xMat,yMat):
    '''note:
        weight = (X.T*X).I*(X.T*y)
        mat.I: 求矩阵的逆
        mat.T: 求矩阵的转置
    '''
    xTx = xMat.T * xMat;
    if 0.0 == np.linalg.det(xTx):
        return ridgeRegression(xMat,yMat);
    else:
        return xTx.I * (xMat.T * yMat);

def LWLRegresssion (testPoint,xMat,yMat,k=1.0):
    '''
        k: 带宽
        weight = (X.T*W*X).I *X.T*W*y
        shape(mat): 返回矩阵的行与列，是一个元组结构
        eye(m,m) or eye ((m)): 产生m个不同的m维单位向量
    '''
    r = np.shape(xMat)[0];
    weight = np.mat(np.eye((r)));
    for j in range(r):
        diffMat = testPoint - xMat[j:,];
        weight[j,j] = np.exp(diffMat.T*diffMat/(-2.0*k**2));
    xTx = xMat.T*(weight*xMat);
    if 0.0 == np.linalg.det(xTx):
        return ridgeRegression(xMat,yMat,True);
    else:
        return testPoint*(xTx.I*(xMat*(weight*yMat)));

def ridgeRegression(xMat,yMat,q = False,lam=0.2):
    xTx = xMat.T;
    if q:
         w = np.mat(np.mat(np.eye((np.shape(xMat)[0]))))
         xTx *= w*xMat;
    else:
        xTx *= xMat;
    denom = xTx + lam*np.eye(np.shape(xMat)[1]);
    if 0.0 == np.linalg.det (np.demon):
        print("matrix is singular, cannot do inverse")
        return;
    else:
        return np.demon.I*(xMat.T*yMat)
    return