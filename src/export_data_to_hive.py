#!/usr/bin/env python
# coding = utf-8
from pyspark import SparkConf
from pyspark import SparkContext
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

spark.sql("show databases").show()
spark.sql("use ieee")

sc = spark.sparkContext

# user info.,
user_info_raw = sc.textFile(path_user_info)
user_info = user_info_raw.map(lambda line: line.split())
user_info_rows = user_info.map(lambda line: Row(u_id=line[0]
                                                , u_labels=line[1]
                                                , u_profile_words=line[2]
                                                , u_profile_chars=line[3]
                                                ))
# create user info schema
schema_user = spark.createDataFrame(user_info_rows)
schema_user.createOrReplaceTempView("user_info")

# test select
result = spark \
    .sql("select * from user_info where length(u_profile_words) < 5 limit 10").show()
spark.sql("create table if not exists users as select * from user_info")

user_label = user_info.map(lambda line: {line[0]: line[1].split("/")})
pprint(user_label.take(5))
user_profile_words = user_info.map(lambda line: line[2].split("/"))
# question info
question_info_raw = sc.textFile(path_question_info)
question_info = question_info_raw.map(lambda line: line.split())
question_info_rows = question_info.map(lambda line: Row(q_id=line[0]
                                                        , q_label=line[1]
                                                        , q_words=line[2]
                                                        , q_chars=line[3]
                                                        , q_thumbs=line[4]
                                                        , q_answers=line[5]
                                                        , q_g_answers=line[6]))
schema_question = spark.createDataFrame(question_info_rows)
schema_question.createOrReplaceTempView("question_info")
spark.sql("create table if not exists questions as select * from question_info")

question_label = question_info.map(lambda line: {line[0]: line[1]})
question_words = question_info.map(lambda line: {line[0]: line[2].split("/")})
# pprint(question_label.take(5))
# train data
train_data_raw = sc.textFile(path_train_data)
train_data = train_data_raw.map(lambda line: line.split())
# pprint(train_data.take(5))
train_data_rows = train_data.map(lambda line: Row(q_id=line[0]
                                                  , u_id=line[1]
                                                  , label=line[2]))
schema_train_data = spark.createDataFrame(train_data_rows)
schema_train_data.createOrReplaceTempView("train_data")
spark.sql("create table if not exists train as select * from train_data")
