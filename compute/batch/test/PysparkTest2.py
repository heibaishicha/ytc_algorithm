#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/1 0:00
# @Author  : LuoJie
# @Email   : 2715053558@qq.com
# @File    : PysparkTest2.py
# @Software: PyCharm

from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf = conf)
    print('***************************** %s' % sc.appName)
    sc.stop()
