from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

conf= SparkConf().setMaster("local").setAppName("q1")
sc= SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()


item_user_mat = sc.textFile("itemusermat").map(lambda x: x.split(" "))
item_user_mat = item_user_mat.map(lambda x: (int(x[0]), x[1:]))
ratings = item_user_mat.map(lambda x: x[1])
model = KMeans.train(ratings, 10)

predictions = item_user_mat.map(lambda row: (row[0], model.predict(row[1])))

movies = sc.textFile("movies.dat").map(lambda x: x.split("::")).map(lambda x: (int(x[0]), x[1:]))

joined = predictions.join(movies).map(lambda x: (x[1][0], (x[0],x[1][1])))

result = joined.groupByKey().map(lambda x: (x[0],list(x[1])[0:5])).sortByKey()
result = result.flatMap(lambda x: x).collect()
# result = result.map(lambda )
#print(result)


sc.parallelize(result).saveAsTextFile("q1")
