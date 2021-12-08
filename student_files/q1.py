import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df.printSchema()

out = df.filter(df.Reviews==None or df.Rating < 1.0)
out.show()