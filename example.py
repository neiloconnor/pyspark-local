# Based on examples in PySpark cheatsheet
# https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_Cheat_Sheet_Python.pdf

# Initalizing Spark
# SparkContext
from pyspark import SparkContext
sc = SparkContext()

# Loading data
# Parallelized Collections
rdd = sc.parallelize([('a',7),('a',2),('b',2)])
rdd2 = sc.parallelize([('a',2),('d',1),('b',1)])
rdd3 = sc.parallelize(range(100))
rdd4 = sc.parallelize([("a",["x","y","z"]),  ("b",["p", "r"])])

# External Data
textfile_rdd = sc.textFile('example_data.txt')

rdd.map(lambda x: x[0]).filter(lambda x: x=='b').collect()

sc.stop()