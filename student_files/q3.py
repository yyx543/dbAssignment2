import sys, ast
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

# retrieve review and date
def foreach(record):
    id = record[0]
    review_lst = ast.literal_eval(record[1])
    reviews = review_lst[0]
    dates = review_lst[1]
    review_detials = []
    for review, date in zip(reviews, dates):
        review_detials.append((id, review, date))
    return review_detials

df = spark.read.option("header", True) \
    .csv("hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn))

# retrieve review and date in a nested list
rev_df = df.select("ID_TA", "Reviews")
id_rev = rev_df.rdd.map(lambda data: [data[0], data[1]])
id_rev_date = id_rev.map(foreach)
output_lst = list(id_rev_date.collect())

# creating new dataframe
new_lst = []
for line in output_lst:
    for el in line:
        new_lst.append(el)
out = spark.createDataFrame(new_lst, ["ID_TA", "review", "date"])
out.write.csv("hdfs://{}:9000/assignment2/output/question3".format(hdfs_nn))

spark.stop()