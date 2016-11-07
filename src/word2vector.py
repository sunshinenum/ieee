#!/usr/bin/env python
# coding = utf-8
from pyspark import SparkConf
from pyspark import SparkContext
from pprint import pprint
import sys

sys.path.append("..")
from conf.conf import *
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("use ieee")
big_table = spark.sql("select label, u.*, q.* from "
                      "train left join users u on "
                      "train.u_id = u.u_id "
                      "left join questions q on "
                      "train.q_id = q.q_id"
                      )
big_table.createOrReplaceTempView("big_table")
big_table_rdd = big_table.rdd
print big_table_rdd.take(5)
# create word2vector train rdd
w2v_train = big_table_rdd.map(lambda row: row.u_profile_words.split("/"))\
            + big_table_rdd.map(lambda row: row.q_words.split("/"))
# pprint(w2v_train.take(10))

from pyspark.mllib.feature import Word2Vec
word2vector = Word2Vec()
word2vector.setSeed(42)
word2vector_model = word2vector.fit(w2v_train)
word2vector_model.findSynonyms('1492', 20)
word2vector_model.transform('1492').show()
