# import findspark
# findspark.init()
from pyspark import SparkContext, SparkConf
# from pyspark.python.pyspark.shell import sqlContext
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
# import match
import os
import pyspark
from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StructType, StringType, DoubleType, BooleanType

# sc = SparkContext("local", "count app")
# words = sc.parallelize(
#     ["scala",
#      "java",
#      "hadoop",
#      "spark",
#      "akka",
#      "spark vs hadoop",
#      "pyspark",
#      "pyspark and spark"
#      ])
# counts = words.count()
# print("number of element in Rdd -> %i" % counts)

# spark = SparkSession.builder.enableHiveSupport().getOrCreate()
# df = spark.read.options(header=True, inferSchema=True, delimiter=',').csv("E339312_ExportedData_Account.1-1.csv")
# df.printSchema()
#
# df = spark.read.csv("path1,path2,path3")

# schema = StructType().add("RecordNumber", IntegerType(), True).add("Zipcode",IntegerType(),True) \
#       .add("ZipCodeType", StringType(), True) \
#       .add("City",StringType(),True) \
#       .add("State",StringType(),True) \
#       .add("LocationType",StringType(),True) \
#       .add("Lat",DoubleType(),True) \
#       .add("Long",DoubleType(),True) \
#       .add("Xaxis",IntegerType(),True) \
#       .add("Yaxis",DoubleType(),True) \
#       .add("Zaxis",DoubleType(),True) \
#       .add("WorldRegion",StringType(),True) \
#       .add("Country",StringType(),True) \
#       .add("LocationText",StringType(),True) \
#       .add("Location",StringType(),True) \
#       .add("Decommisioned",BooleanType(),True) \
#       .add("TaxReturnsFiled",StringType(),True) \
#       .add("EstimatedPopulation",IntegerType(),True) \
#       .add("TotalWages",IntegerType(),True) \
#       .add("Notes",StringType(),True)
#
# df_with_schema = spark.read.format("csv").option("header", True).schema(schema).load("E339312_ExportedData_Account.1-1.csv")
# print(df_with_schema)

# df = sqlContext.create


# sc = SparkContext("local", "Hello World App")
# sc.setLogLevel("Info")
# print(sc.version)
# print(sc)




if __name__ == '__main__':
    print("======================")

    # 配置环境
    spark = SparkSession.builder\
        .master("local")\
        .appName("Word Count")\
        .config("spark.some.config.option", "some-value")\
        .getOrCreate()

    print("ok")
    print(spark)
    filepath = "E339312_ExportedData_Account.1-1.csv"
    data = spark.read.format('csv').load(filepath, sep=',', header=True, inferSchema=True)
    print(data)
    writepath="test/ok"
    data.repartition(1).write.csv(writepath, mode="overwrite")
    print(data.repartition(1).write.csv(writepath, mode="overwrite"))



# rdd = spark.sparkContext.parallelize([
#     (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
#     (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
#     (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
# ])
# df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])
#
# print(df)

from pyspark import SparkContext, SparkConf
# from pyspark.shell import sqlContext
# SparkConf()
# sc = SparkContext("local", "hello")

# print(sc)
# data2 = [("Alice", 5), ("Bob", 5), ("Tom", 5)]
# print(data2)
# df = sqlContext.createDataFrame(data2, ["name", "age"])
# df.show()
