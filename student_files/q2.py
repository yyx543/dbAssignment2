import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", True) \
    .csv("hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn))

# drop null prices
df = df.filter(col("Price Range").isNotNull())

# change Rating data type
df = df.withColumn("Rating",col("Rating").cast("float"))
# drop duplicates
df = df.dropDuplicates(["Price Range", "Rating", "City"])
# find min and max and combine them
min_df = df.groupBy("City", "Price Range").min("Rating").withColumnRenamed("min(Rating)", "Rating")
max_df = df.groupBy("City", "Price Range").max("Rating").withColumnRenamed("max(Rating)", "Rating")
out = min_df.union(max_df)

# retrieve the rest of the columns
out = out.withColumnRenamed("Rating", "newRating") \
         .withColumnRenamed("Price Range", "newPrice Range") \
         .withColumnRenamed("City", "newCity")
out = out.join(df, (col("City") == col("newCity")) & (col("Rating") == col("newRating")) & ((col("Price Range") == col("newPrice Range")))) \
         .sort("Price Range", "City", "Rating")
out = out.drop("newCity", "newRating", "newPrice Range")

# change back Rating data type
out = out.withColumn("Rating",col("Rating").cast("string"))

out.write.csv("hdfs://{}:9000/assignment2/output/question2".format(hdfs_nn))

spark.stop()