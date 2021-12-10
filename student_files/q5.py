import sys, ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

# cast_id
# character
# credit_id
# gender (1-female, 2-male)
# id
# name
# order

# retrieve movie_id, title, actor1, actor2
def foreach(record):
    movie_id = record[0]
    title = record[1]
    cast_dict = ast.literal_eval(record[2])

    cast_lst = []
    cast_id = []
    for el in cast_dict:
        cast_lst.append(el["name"])
        cast_id.append(el["cast_id"])

    func_output = []
    for idx1 in range(len(cast_lst)):
        for idx2 in range(idx1+1, len(cast_lst)):
            if cast_id[idx1] > cast_id[idx2]:
                func_output.append((movie_id, title, cast_lst[idx1], cast_lst[idx2]))
            else:
                func_output.append((movie_id, title, cast_lst[idx2], cast_lst[idx1]))
    return func_output

df = spark.read.option("header",True).parquet("hdfs://{}:9000/assignment2/part2/input/".format(hdfs_nn))

new_df = df.select("movie_id", "title", "cast")
id_title_cast = new_df.rdd.map(lambda data: [data[0], data[1], data[2]])
id_title_dict = id_title_cast.map(foreach).flatMap(lambda x:x)

preprocess = id_title_dict.map(lambda x: (x[0], x[1], x[2], x[3])).toDF(("cast_id", "movie_id", "title", "actor1", "actor2"))
count_df = preprocess.groupBy("actor1", "actor2").count().filter(col("count") > 1)

output_df = preprocess.join(count_df, ["actor1", "actor2"]).drop(col("count"))
output_df = output_df.select("movie_id", "title", "actor1", "actor2").dropDuplicates()
output_df.out.option("header",True).mode('overwrite').parquet("hdfs://%s:9000/assignment2/output/question5" % (hdfs_nn))

spark.stop()