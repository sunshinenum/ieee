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
    .master("local[*]") \
    .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true")\
    .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")\
    .config("javax.jdo.option.ConnectionUserName", "chen")\
    .config("javax.jdo.option.ConnectionPassword", "1993")\
    .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.sql.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")\
    .enableHiveSupport()\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("use ieee")

sc = spark.sparkContext


def import_user_info():
      # user info.
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
    spark.sql("create table if not exists users as select * from user_info")


def import_question_info():
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


def import_train_data():
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


def import_test_data():
    test_data_rows = sc.textFile(path_test_data)\
        .map(lambda line: line.split(","))\
        .map(lambda line: Row(q_id=line[0],
                              u_id=line[1]))
    schema_test_data = spark.createDataFrame(test_data_rows)
    schema_test_data.createOrReplaceTempView("test_data")
    spark.sql("create table if not exists test as select * from test_data")
    print "test data imported!"

if __name__ == "__main__":
    import_test_data()
