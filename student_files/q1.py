import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", True) \
    .csv("hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn))

# remove rating < 1.0
out = df.filter(df.Rating >= 1.0)
# remove no reviews
out = out.where(col("Number of Reviews").isNotNull())

#Write the output as CSV into HDFS path /assignment2/output/question1/
out.write.csv("hdfs://{}:9000/assignment2/output/question1".format(hdfs_nn))

spark.stop()