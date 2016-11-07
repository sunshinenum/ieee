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
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


sc = SparkContext("local[2]", "First Spark App")
# user info
user_info_raw = sc.textFile(path_user_info)
user_info = user_info_raw.map(lambda line: line.split())
user_info_rows = user_info.map(lambda line: Row(uid=line[0]
                                                , u_labels=line[1]
                                                , u_profile_words=line[2]
                                                , u_profile_chars=line[3]
                                                ))
# create user info schema
schema_user = spark.createDataFrame(user_info_rows)
schema_user.createOrReplaceTempView("user_info")
# test select
result = schema_user\
    .sql("select * from user_info where length(u_profile_words) < 5 limit 10")
result.rdd.take(5)

user_label = user_info.map(lambda line: {line[0]:line[1].split("/")})
pprint(user_label.take(5))
user_profile_words = user_info.map(lambda line: line[2].split("/"))
# question info
question_info_raw = sc.textFile(path_question_info)
question_info = question_info_raw.map(lambda line: line.split())
question_label = question_info.map(lambda line: {line[0]: line[1]})
question_words = question_info.map(lambda line: {line[0]: line[2].split("/")})
pprint(question_label.take(5))
# train data
train_data_raw = sc.textFile(path_train_data)
train_data = train_data_raw.map(lambda line: line.split())
train_data_with_q_label = train_data.map(lambda line: line.append(question_label[line[0]]))
pprint(train_data_with_q_label.take(5))
