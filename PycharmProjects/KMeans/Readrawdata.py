#!/usr/bin/python
# -*- coding: gbk -*
__author__ = 'Cheery'

import pandas as pd
import numpy as np
import xlrd
import os
import sys

File = 'e:/code/python/excel1'
FileList = os.listdir(File)
for line  in FileList:
    FilePath = File + '/' + line
    FilePath = unicode(FilePath)
    with xlrd.open_workbook(FilePath) as Fil:
        for table in Fil.sheet_names():
            col
            for rownum in range(0,table.nrows):
                row = table.row_values(rownum)
                if row:
                    app = {}
                    for i in range(0,len(table.row_values()))




Fil = xlrd.open_workbook(u'e:/code/python/excel1/百货供应商信息表.xls')
