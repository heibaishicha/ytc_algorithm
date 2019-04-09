#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/3/30 19:50
# @Author  : LuoJie
# @Email   : 2715053558@qq.com
# @File    : SparkTest4.py
# @Software: PyCharm

from pyspark import SparkContext

logFile = "C:\\work\\myItem\\ytc-ai\\mnist_testdemo\\mnist_testdemo\\README.md"

sc = SparkContext("local","Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i,lines with b: %i"%(numAs, numBs))
