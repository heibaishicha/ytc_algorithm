from pyspark import SparkContext
logFile = "/Users/chenxl/Documents/soft/spark-2.0.1-bin-hadoop2.6/README.md"
sc = SparkContext("local","Simple App")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i,lines with b: %i"%(numAs,numBs))
--------------------- 
作者：佛空如水 
来源：CSDN 
原文：https://blog.csdn.net/ydc321/article/details/78903240 
版权声明：本文为博主原创文章，转载请附上博文链接！