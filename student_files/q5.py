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
    for el in cast_dict:
        cast_lst.append(el["name"])

    cast_id = []
    for el in cast_dict:
        cast_id.append(el["cast_id"])

    func_output = []
    for idx1 in range(len(cast_lst)):
        for idx2 in range(idx1+1, len(cast_lst)):
            if cast_id[idx1] > cast_id[idx2]:
                func_output.append((str(cast_id[idx1]) + ":" + str(cast_id[idx2]), [movie_id, title, cast_lst[idx1], cast_lst[idx2]]))
            else:
                func_output.append((str(cast_id[idx2]) + ":" + str(cast_id[idx1]), [movie_id, title, cast_lst[idx2], cast_lst[idx1]]))
    return func_output

def find_char(x):
    if len(x[1])>1:
        return x[1]
    return []

df = spark.read.option("header",True).parquet("hdfs://{}:9000/assignment2/part2/input/".format(hdfs_nn))
print("spooderman df")
data = df.rdd
print(data.take(1))
print("sppoderman")
df.printSchema()
#df.show()

new_df = df.select("movie_id", "title", "cast")
id_title_cast = new_df.rdd.map(lambda data: [data[0], data[1], data[2]])

print("hello world")
id_title_dict = id_title_cast.map(foreach).flatMap(lambda x: x)
print(id_title_dict.take(1))
data = id_title_dict.reduceByKey(lambda x  :x).flatMap(find_char)
print(data.take(1))

# new_df = df.select("movie_id", "title", "cast")
# id_title_cast = new_df.rdd.map(lambda data: [data[0], data[1], data[2]])
# id_title_dict = id_title_cast.map(foreach).flatMap(lambda x:x)
# temp = id_title_dict.toDF()
# temp.show()
#preprocess = id_title_dict.map(lambda x: (x[0], x[1], x[2], x[3], x[4])).toDF(("cast_id", "movie_id", "title", "actor1", "actor2"))
#preprocess.show()
# preprocess.select(col()).groupBy("actor1", "actor2").count().filter(col("count") > 1).show()

# count_df = preprocess.groupBy("actor1", "actor2").count()
# count_df.show()


# out.show()

spark.stop()

df = spark.read.option("header",True)\
                .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))

df = df.withColumn("cast", from_json(col("cast"), "array<map<string,string>>"))

# extract actors from cast map
df = df.select(col("movie_id"), col("title"), explode("cast").alias("cast_person"))
df = df.withColumn("actor", col("cast_person").getItem("name"))

# self join to get actor pairs
pairdf = df.alias("df1").join(df.alias("df2"), col("df1.movie_id")==col("df2.movie_id"), "inner") \
                .select(col("df1.movie_id").alias("movie_id"), col("df1.title"), \
                        col("df1.actor").alias("actor1"), col("df2.actor").alias("actor2")) 
                
# pairs worked tgt for at least 2 movies --> sort all (a,b) and (b,a) to (a,b) then drop duplicates
condf = pairdf.groupBy("actor1", "actor2").agg(count("movie_id").alias("workcount"))
condf = condf.filter(condf.actor1 != condf.actor2).filter(condf.workcount>=2).withColumn("pair_sorted", array_sort(array(col("actor1"),col("actor2"))))
condf = condf.dropDuplicates(["pair_sorted"])

# join on worked tgt pairs
pairdf = pairdf.join(condf.alias("c"), (pairdf.actor1==condf.pair_sorted[0]) & (pairdf.actor2==condf.pair_sorted[1]), "right") \
        .select(col("movie_id"), col("title"), \
                pairdf.actor1.alias("actor1"), pairdf.actor2.alias("actor2"))

pairdf.write.option("header",True) \
        .csv("hdfs://{}:9000//assignment2/output/question5/".format(hdfs_nn))

spark.stop()