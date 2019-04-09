#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/3/31 23:50
# @Author  : LuoJie
# @Email   : 2715053558@qq.com
# @File    : PysparkTest.py
# @Software: PyCharm


from __future__ import print_function
import sys
from operator import add
import math
from pyspark import SparkConf, SparkContext


def cosine(t):
    print (t)
    x = t[0][1]
    y = t[1]
    convxy = 0
    sumx=0
    sumy=0
    for i in range(len(x)):
        convxy += x[i] * y[i]
        sumx += x[i] ** 2
        sumy += y[i] ** 2
    return (t[0][0],convxy /(math.sqrt(sumx) * math.sqrt(sumy)))


if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf=conf)
data1 = sc.parallelize([(1, [1, 2, 4, 6, 8]),
                        (2, [2, 1, 4, 6, 8]),
                        (3, [1, 1, 1, 1, 1]),
                        (4, [2, 3, 4, 5, 6]),
                        (5, [3, 2, 5, 6, 4])])
data2 = sc.parallelize([[2, 3, 5, 6, 7]])
data_rdd = data1.cartesian(data2)
output = data_rdd.map(cosine).collect()
print(output)
