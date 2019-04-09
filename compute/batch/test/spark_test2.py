#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/3/30 19:27
# @Author  : LuoJie
# @Email   : 2715053558@qq.com
# @File    : spark_test2.py
# @Software: PyCharm

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark import SparkContext

spamFp = "file:///home/hadoop/code/spark/files/spam.txt"
normalFp = "file:///home/hadoop/code/spark/files/normal.txt"

sc = SparkContext("local[2]", "SparkTest")

spam= sc.textFile(spamFp)
normal= sc.textFile(normalFp)

# 将每个单词作为一个单独的特征
tf = HashingTF(numFeatures=10000)
spamFeatures = spam.map( lambda email : tf.transform(email.split(" ")) )
normalFeatures = normal.map( lambda email : tf.transform(email.split(" ")) )

# 构建LabelPoint，即每个向量都有它的label，之后联合构成整个训练集
positiveExamples = spamFeatures.map( lambda features : LabeledPoint(1, features) )
negativeExamples = normalFeatures.map( lambda features : LabeledPoint(0, features) )
trainingData = positiveExamples.union(negativeExamples  )
# SGD是迭代算法，因此在这里缓存数据集，加快运行速度
trainingData.cache()

# 训练
model = LogisticRegressionWithSGD.train( trainingData )

# 预测
posTest = tf.transform( "O M G Get cheap stuff by sending money to ...".split(" ") )
negTest = tf.transform( "I just want to play tennis now".split(" ") )

print( "Prediction for positive test example : %g" % model.predict(posTest) )
print( "Prediction for negative test example : %g" % model.predict(negTest) )

