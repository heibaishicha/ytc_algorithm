#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/3/30 17:46
# @Author  : LuoJie
# @Email   : 2715053558@qq.com
# @File    : spark_test.py
# @Software: PyCharm

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "SparkTest")
ssc = StreamingContext(sc, 1)

# Create Checkpoint fro local StreamingContext
ssc.checkpoint("checkpoint")


# Define updateFunc: sum of the (key, value) pairs
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


# Create DStream that connect to 192.168.68.160:9999
lines = ssc.socketTextStream("localhost", 9999)

# Calculate running counts
running_counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).updateStateByKey(updateFunc)

running_counts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()
