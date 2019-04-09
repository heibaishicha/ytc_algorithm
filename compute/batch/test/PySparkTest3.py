#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/1 0:18
# @Author  : LuoJie
# @Email   : 2715053558@qq.com
# @File    : PySparkTest3.py
# @Software: PyCharm

__author__ = 'LuoJie'
import os
import sys

# os.environ['SPARK_HOME']="D:\spark\spark-1.6.2-bin-hadoop2.6\spark-1.6.2-bin-hadoop2.6"
# sys.path.append("D:\spark\spark-1.6.2-bin-hadoop2.6\spark-1.6.2-bin-hadoop2.6\python")

from pyspark import SparkContext

sc = SparkContext('local')
doc = sc.parallelize([['a','b','c'],['b','d','d']])
words = doc.flatMap(lambda d:d).distinct().collect()
word_dict = {w:i for w,i in zip(words,range(len(words)))}
word_dict_b = sc.broadcast(word_dict)


def wordCountPerDoc(d):
    dict={}
    wd = word_dict_b.value
    for w in d:
        if dict.has_key(wd[w]):
            dict[wd[w]] +=1
        else:
            dict[wd[w]] = 1
    return dict


print(doc.map(wordCountPerDoc).collect())
print("successful!")
