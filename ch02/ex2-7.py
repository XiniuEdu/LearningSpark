# Evn: Standalone Applications
# Name: Initializing Spark in Python

## 参考例2-1，统计文件行数的Spark独立应用，基于SparkContext。
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
file = sc.textFile("../data/README.md")
words = file.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)

print(words.collect())

## 参考例2-1，统计文件行数的Spark独立应用，基于Spark Dataframe。
#from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName("My App").getOrCreate()
#file = spark.read.text("../data/README.md")
#words = file.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
print(words.collect())
