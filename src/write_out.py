#! /usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Liguo Chen'
import sys
sys.path.append("..")
from conf.conf import *
from chen.ml.similarity import *
import math

"""
user_info:          uid_string, u_label_list, words_list, char_list
question_info:      qid_string, q_label_list, words_list, char_list,
                                thumb_up_int, answers_int, good_answers_int
invited_info_train: qid_string, uid_string, label_bool
temp.csv:           qid_string, uid_string, prob_label_float
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from gensim import corpora, models, similarities


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
spark.sql("use ieee")


def write_out_result(predict_result, test_data):
    """
    Write out predict result (temp.csv) when using local features.
    """
    results = []
    for index, line in enumerate(test_data):
        result = []
        result.append(line[0])
        result.append(line[1])
        result.append(predict_result[index][1])
        results.append(result)
    f = open(path_results, "w")
    f.write("qid,uid,label\n")
    for line in results:
        f.write("%s,%s,%f\n" % tuple(line))
    f.close()


def write_out_result_hive(predict_result):
    """
    Write out predict_result (temp.csv) when using selected features.
    """
    q_u_info = spark.sql("select * from all_features_predict") \
            .rdd.map(lambda row: [row.q_id, row.u_id]).collect()
    f = open(path_results, "w")
    f.write("qid,uid,label\n")
    for index, line in enumerate(q_u_info):
        f.write("%s,%s,%f\n" % (line[0], line[1], predict_result[index][1]))
    f.close()


def write_out_result_give_low_score_zero(predict_result):
    """
    Write out predict_result (temp.csv) when using selected features.
    Score low score items to be zero.(u_answers_rate < 0.05 or q_answers_rate < 0.015
    or log_q_answers == 0.0)
    """
    q_u_info = spark.sql("select * from all_features_predict") \
            .rdd.map(lambda row: [row.q_id, row.u_id
                , row.u_answers_rate
                , row.q_answers_rate
                , row.log_q_answers]).collect()
    f = open(path_results, "w")
    f.write("qid,uid,label\n")
    for index, line in enumerate(q_u_info):
        if line[2] < 0.05 or line[3] < 0.015 or line[4] == 0.0:
            f.write("%s,%s,%f\n" % (line[0], line[1], 0.0))
        else:
            f.write("%s,%s,%f\n" % (line[0], line[1], predict_result[index][1]))
    f.close()


def write_out_in_libsvm(features, path, has_label=True):
    f = open(path, "w")
    n = len(features[0])
    if has_label:
        n -= 1
    for line in features:
        for i in range(n + 1):
            if i == 0:
                if has_label:
                    f.write("%f " % float(line[n]))
                else:
                    f.write("0.0 ")
            else:
                if line[i - 1] == None:
                    f.write("%d:%f " % (i - 1, 0.0))
                else:
                    f.write("%d:%f " % (i - 1, line[i - 1]))
        f.write("\n")
    f.close()


def write_out_train_result(result):
    print "[+] Writing out result."
    f_train = open(path_vectors_train, "r")
    train_list = []
    for line in f_train:
        train_list.append(line)
    f = open(path_train_predict_watch, "w")
    f.write("predict_result, label, u_answers_rate, u_weighted_answers_rate, "
            "q_answers_rate, q_weighted_answers_rate, log_q_answers\n")
    for index, line in enumerate(result):
        f.write("%.6f %s" % (line[1], train_list[index]))
    f.close()
    f_train.close()
