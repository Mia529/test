from pyspark import SparkContext
from pyspark.shell import sqlContext

from pyspark.sql.functions import col, pandas_udf, mean, max
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row, functions as F
from pyspark.sql.window import Window
from pyspark.sql import SQLContext
import pandas as pd
spark = SparkSession.builder.getOrCreate()
from datetime import datetime, date
from pyspark.sql import Row

import numpy as np
# import pyspark.pandas as ps
# df = spark.createDataFrame([
#     Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#     Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
#     Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
# ])
# print(df)
# pandas_df = pd.DataFrame({
#     'a': [1, 2, 3],
#     'b': [2., 3., 4.],
#     'c': ['string1', 'string2', 'string3'],
#     'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
#     'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
# })
# df = spark.createDataFrame(pandas_df)
# print(df)
# sc.stop()
#
# sc = SparkContext("local", "hello")
# print(sc)
# rdd = spark.sparkContext.parallelize([
#     (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
#     (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
#     (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
# ])
# df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])
# print(df)


if __name__ == '__main__':
    print("======================")

    # 配置环境
    spark = SparkSession.builder\
        .master("local")\
        .appName("Word Count")\
        .config("spark.some.config.option", "some-value")\
        .getOrCreate()

    print("ok")
    # print(spark)
    # rdd = spark.sparkContext.parallelize([
    #     (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    #     (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    #     (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
    # ])
    # df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])
    # print(df.show())
    # print(df.printSchema())
    # words = spark.sparkContext.parallelize(
    #     [
    #         "scala",
    #         "java",
    #         "hadoop",
    #         "spark",
    #         "akka",
    #         "spark vs hadoop",
    #         "pyspark",
    #         "pyspark and spark"
    #     ]
    # )
    # print(words)

    # # count()返回RDD中的元素个数
    # counts = words.count()
    # print("Number of elements in RDD -> % i" % counts)
    #
    # # collect()返回RDD中的所有元素，即转换为python数据类型
    # coll = words.collect()
    # print("Elements in RDD -> %s" % coll)
    #
    # # foreach(func)仅返回满足foreach内函数条件的元素
    # def f(x): print(x)
    # fore = words.foreach(f)
    #
    # # filter(f)返回一个包含元素的新RDD，它满足过滤器内部的功能
    # words_filter = words.filter(lambda x: 'spark' in x)
    # filtered = words_filter.collect()
    # print("Fitered RDD -> %s" % (filtered))
    #
    # # map(f,preservesPartitioning = False)通过将该函数应用于RDD中的每个元素来返回新的RDD
    # words_map = words.map(lambda x: (x, 1))
    # mapping = words_map.collect()
    # print("Key value pair -> %s" % (mapping))

    # reduce(f)执行指定的可交换和关联二元操作后，将返回RDD中的元素
    # 下面例子结果是1+2+3+4+5 = 15
    # from operator import add
    # nums = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    # adding = nums.reduce(add)
    # print("Adding all the eements -> %i" % (adding))

    # join(other,numPartitions = None)它返回RDD，其中包含一对带有匹配键的元素以及该特定键的所有值
    # x = spark.sparkContext.parallelize([("spark", 1), ("hadoop", 4)])
    # y = spark.sparkContext.parallelize([("spark", 2), ("hadoop", 5)])
    # joined = x.join(y)
    # final = joined.collect()
    # print("Join RDD -> %s" % (final))

    # 下面换一组数据进行操作
    # intRDD = spark.sparkContext.parallelize([3, 1, 2, 5, 5])
    # stringRDD = spark.sparkContext.parallelize(['Apple', 'Orange', 'Grape', 'Banana', 'Apple'])
    # print(intRDD.distinct().collect())
    # print(stringRDD.distinct().collect())

    # randomSplit()运算将整个集合以随机数的方式按照比例分为多个RDD，比如按照0.4和0.6的比例将intRDD分为两个RDD，并输出
    # sRDD = intRDD.randomSplit([0.4, 0.6])
    # print(len(sRDD))
    # print(sRDD[0].collect())
    # print(sRDD[1].collect())

    # groupBy()预算可以按照传入匿名函数的规则，将数据分为多个Array。
    # 比如下面的例子将intRDD分为偶数和奇数
    # result = intRDD.groupBy(lambda x: x % 2).collect()
    # print(sorted([(x, sorted(y)) for (x, y) in result]))

    # union()进行并集运算
    # intRDD1 = spark.sparkContext.parallelize([3, 1, 2, 5, 5])
    # intRDD2 = spark.sparkContext.parallelize([5, 6])
    # intRDD3 = spark.sparkContext.parallelize([2, 7])
    # print(intRDD1.union(intRDD2).union(intRDD3).collect())

    # intersection()进行交集运算
    # print(intRDD1.intersection(intRDD2).collect())

    # RDD基本动作运算
    # 读取元素，可以使用下列命令读取RDD内的元素，这是Actions运算，所以会马上执行
    # # 取第一条数据
    # print(intRDD.first())
    # # 取前两条数据
    # print(intRDD.take(2))
    # # 升序排列，并取前3条数据
    # print(intRDD.takeOrdered(3))
    # # 降序排列，并取前3条数据
    # print(intRDD.takeOrdered(3, lambda x: -x))

    # # 统计 会返回一些常见的统计指标的值
    # print(intRDD.stats())
    # # 最小值
    # print(intRDD.min())
    # # 最大值
    # print(intRDD.max())
    # # 标准差
    # print(intRDD.stdev())
    # # 计数
    # print(intRDD.count())
    # # 求和
    # print(intRDD.sum())
    # # 平均
    # print(intRDD.mean())

    # RDD key-value基本转换运算
    # kvRDD1 = spark.sparkContext.parallelize([(3, 4), (3, 6), (5, 6), (1, 2)])
    # kvRDD2 = spark.sparkContext.parallelize([(3, 8)])
    # print(kvRDD1.keys().collect())
    # print(kvRDD1.values().collect())
    # print(kvRDD1.filter(lambda x: x[0] < 5).collect())
    # print(kvRDD1.mapValues(lambda x: x**2).collect())
    # print(kvRDD1.sortByKey().collect())
    # print(kvRDD1.sortByKey(True). collect())
    # print(kvRDD1.sortByKey(False).collect())
    # print(kvRDD1.reduceByKey(lambda x, y: x + y).collect())

    # print(kvRDD1.leftOuterJoin(kvRDD2).collect())
    # print(kvRDD1.rightOuterJoin(kvRDD2).collect())

    # # 读取第一条数据
    # print(kvRDD1.first())
    # # 读取前两条数据
    # print(kvRDD1.take(2))
    # # 读取第一条数据的key值
    # print(kvRDD1.first()[0])
    # # 读取第一条数据的value值
    # print(kvRDD1.first()[1])
    # print(kvRDD1.countByKey())
    # print(kvRDD1.lookup(3))

    # sqlContext = SQLContext(spark)
    # print(sqlContext)
    # df = sqlContext.read.format('E339312_ExportedData_Account.1-1.csv').options(header='true', inferschema='true')
    # print(df.show(30))

    # pdf = pd.read_excel('test.xlsx', sheet_name='Sheet1')
    # df = spark.createDataFrame(pdf)
    # df.show()

    # filepath = "E339312_ExportedData_Account.1-1.csv"
    # data = spark.read.format('csv').load(filepath, sep=',', header=True, inferSchema=True)
    # print(data)
    # data.show(30)
    # data.printSchema()
    # list = data.head(3)
    # list = data.take(5)
    # print(type(list))
    # print(list)

    # data_count = data.count()  # 查询总行数
    # print(data_count)
    # print(data.columns)
    # print(data.select("Point-of-View").orderBy("Point-of-View").show(10))


    # sentenceFrame = spark.createDataFrame((
    #     (1, "asf"),
    #     (2, "2143"),
    #     (3, "rfds")
    # )).toDF("label", "sentence")
    # sentenceFrame.show()
    #
    # sentenceFrame1 = spark.createDataFrame((
    #     (1, "asf"),
    #     (2, "2143"),
    #     (4, "f8934y")
    # )).toDF("label", "sentence")
    # sentenceFrame1.show()
    #
    # # subtract() 求差集，可指定对某一列求差集，如不指定，将对所有列求差集
    # newDF = \
    # sentenceFrame1.select("sentence").subtract((sentenceFrame.select("sentence")))
    # newDF.show()
    #
    # # intersect() 求交集，可以指定对某一列求交集，如不指定，将对所有列求交集
    # newDF2 = \
    # sentenceFrame1.select("sentence").intersect(sentenceFrame.select("sentence"))
    # newDF2.show()
    #
    # # union() 求并集，与上面的表连接到语句相同
    # newDF3 = \
    # sentenceFrame1.select("sentence").union(sentenceFrame.select("sentence"))
    # newDF3.show()
    #
    # # union+distinct 求并集并进行去重，结果与上对比
    # newDF4 = sentenceFrame1.select("sentence").union(sentenceFrame.select("sentence"))
    # newDF4.show()

    # 窗口函数的使用
    # rddData = spark.sparkContext.parallelize(
    #     (Row(c="class1", s=50), Row(c="class2", s=40),\
    #      Row(c="class3", s=70), Row(c="class2", s=49),\
    #      Row(c="class3", s=29), Row(c="class1", s=78))
    # )
    # sqlContext = SQLContext(spark)
    # testDF = rddData.toDF()
    # result = (testDF.select("c", "s", F.row_number().over(Window.partitionBy("c").orderBy("s")).alias("rowNum")))
    # finalResult = result.where(result.rowNum <= 1).show()

    # 格式化字符串
    from pyspark.sql.functions import format_string
    df = spark.createDataFrame([(5, "hello")], ['a', 'b'])
    df.select(format_string('%d %s', df.a, df.b).alias('v')).withColumnRenamed("v", "vv").show()

    # 查找字符串的位置
    from pyspark.sql.functions import instr
    df = spark.createDataFrame([('abcd',)], ['s'])
    df.select(instr(df.s, 'b').alias('s')).show()

    # 字符串截取
    from pyspark.sql.functions import substring
    df = spark.createDataFrame([('abcd',)], ['s'])
    df.select(substring(df.s, 1, 2).alias('s')).show()

    # 正则表达式替换
    from pyspark.sql.functions import regexp_replace
    df = spark.createDataFrame([('100sss200',)], ['str'])
    df.select(regexp_replace('str', '(\d)', '-').alias('d')).collect()

    # 将时间格式进行更改
    # 使用pyspark.sql.funcions.date_format方法
    from pyspark.sql import functions as F
    df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    df.select(F.date_format('dt', 'yyyyMMdd').alias('date')).collect()

    # 获取当前日期
    from pyspark.sql.functions import current_date
    spark.range(3).withColumn('date', current_date()).show()

    # 获取当前日期时间
    from pyspark.sql.functions import current_timestamp
    spark.range(3).withColumn('date', current_timestamp()).show()

    # 将字符串日期改为时间日期格式
    from pyspark.sql.functions import to_date, to_timestamp
    df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    df.select(to_date(df.t).alias('date')).show()  # 转日期
    df.select(to_timestamp(df.t).alias('dt')).show()  # 带时间的日期
    df.select(to_timestamp(df.t, 'yyyy-MM-dd HH:mm:ss').alias('dt')).show()  # 可以指定日期格式

    # 获取日期中的年月日
    from pyspark.sql.functions import year, month, dayofmonth
    df = spark.createDataFrame([('2015-04-08',)], ['a'])
    df.select(year('a').alias('year'),
              month('a').alias('month'),
              dayofmonth('a').alias('day')).show()

