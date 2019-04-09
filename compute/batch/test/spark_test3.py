#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/3/30 19:31
# @Author  : LuoJie
# @Email   : 2715053558@qq.com
# @File    : spark_test3.py
# @Software: PyCharm

from pyspark.mllib.feature import StandardScaler
from pyspark import SparkContext
from pyspark.ml.linalg import Vectors

sc = SparkContext("local[2]", "SparkTest")

vec = Vectors.dense([[-1, 5, 1], [2, 0, 1]])
print(vec)

dataset = sc.parallelize(vec)
scaler = StandardScaler(withMean=True, withStd=True)
model = scaler.fit(dataset)
result = model.transform(dataset).collect()
print(result)
