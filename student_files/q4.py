import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", True) \
    .csv("hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn))

df = df.withColumn("Cuisine Style", from_json(col("Cuisine Style"), "array<string>"))
out = df.select(col("City"), explode("Cuisine Style").alias("Cuisine")).groupBy("City", "Cuisine").count()
out.show()
out.write.csv("hdfs://{}:9000/assignment2/output/question4".format(hdfs_nn))

spark.stop()