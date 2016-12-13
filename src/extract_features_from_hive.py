#! /usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Liguo Chen'
import sys
sys.path.append("..")
from conf.conf import *
from chen.ml.similarity import *
import math
from conf.conf import *
from pprint import pprint
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
spark.sparkContext.setLogLevel("ERROR")
spark.sql("use ieee")


"""
user_info:          uid_string, u_label_list, words_list, char_list
question_info:      qid_string, q_label_string, words_list, char_list,
                                thumb_up_int, answers_int, good_answers_int
invited_info_train: qid_string, uid_string, label_bool
temp.csv:           qid_string, uid_string, prob_label_float
"""


def get_features_from_hive(is_test=False):
    """
    Get features from hive table ieee.features_select_predict, ieee.features_select_train.
    :param is_test: Test data or train data.
    :return: features
    """
    if is_test:
        sql_query = "select * from ieee.features_select_predict"
        features = spark.sql(sql_query) \
            .rdd.map(lambda row: [row.asDict()[key] for key in list(row.asDict())]).collect()
    else:
        sql_query = "select * from ieee.features_select_train"
        features = spark.sql(sql_query) \
            .rdd.map(lambda row: [row.asDict()[key] for key in list(row.asDict())
                                  if not key == 'label'] + [row.label]).collect()
    print "Features like:"
    print features[:15]
    return features


def get_w2v_train_data():
    query_u_profiles = "select u_profile_words from ieee.full_table"
    query_q_words = "select q_words from ieee.full_table"
    u_profiles = spark.sql(query_u_profiles)\
        .rdd\
        .map(lambda row: row.u_profile_words.split("/"))\
        .collect()
    q_words = spark.sql(query_q_words) \
        .rdd \
        .map(lambda row: row.q_words.split("/")) \
        .collect()
    return u_profiles, q_words


def get_w2v_predict_data():
    query_u_profiles = "select u_profile_words from ieee.invited_without_label_full"
    query_q_words = "select q_words from ieee.invited_without_label_full"
    u_profiles = spark.sql(query_u_profiles)\
        .rdd\
        .map(lambda row: row.u_profile_words.split("/"))\
        .collect()
    q_words = spark.sql(query_q_words) \
        .rdd \
        .map(lambda row: row.q_words.split("/")) \
        .collect()
    return u_profiles, q_words


def sentence_sim(model, s1, s2):
    """
    Get sentence similarity.
    :param model: w2v model
    :param s1: sentence 1
    :param s2: sentence 2
    :return: similarity
    """
    sim_list = []
    sim_l = 0.1
    for w1 in s1:
        for w2 in s2:
            try:
                if model.similarity(w1, w2) > sim_l:
                    sim_list.append(float(model.similarity(w1, w2)))
            except KeyError:
                sim_list.append(0.0)
    return sum(sim_list)


def w2v_train_predict():
    """
    Train w2v model and write similarity to hive.
    :return:
    """
    print "Fetching data from hive..."
    u_words, q_words = get_w2v_train_data()
    u_words_p, q_words_p = get_w2v_predict_data()
    print "Training w2v model with data fetched..."
    model = models.Word2Vec(u_words+q_words+u_words_p+q_words_p
                            , size=100, window=5, min_count=5, workers=4)
    print "Writing similarity to hive"
    write_sim_to_hive(model, True)
    write_sim_to_hive(model, False)


def write_sim_to_hive(model, is_train=True):
    """
    Write data to hive and create full_features table.
    :param model: w2v model
    :param is_train: Train or test similarity.
    :return:
    """
    if is_train:
        sql_q = "select * from ieee.full_table"
    else:
        sql_q = "select * from ieee.invited_without_label_full"
    matrix = spark.sql(sql_q).rdd\
            .map(lambda row: [row.u_id, row.q_id, row.u_profile_words, row.q_words])\
            .collect()
    # print matrix[:5]
    for index, line in enumerate(matrix):
        matrix[index].append(sentence_sim(model,
                                          line[2].split("/"), line[3].split("/")))
    matrix_rdd = spark.sparkContext.parallelize(matrix)
    matrix_df = matrix_rdd.map(lambda line: Row(u_id=line[0],
                                                q_id=line[1],
                                                u_q_sentences_words_sim_w2v=line[4]))
    schema_sim = spark.createDataFrame(matrix_df)
    schema_sim.createOrReplaceTempView("train_data_sim_tmp")
    if is_train:
        spark.sql("use ieee")
        spark.sql("drop table all_features")
        spark.sql("create table if not exists all_features as "
                  "select a.*, b.u_q_sentences_words_sim_w2v from "
                  "ieee.features a left outer join train_data_sim_tmp "
                  "b on a.u_id = b.u_id and a.q_id = b.q_id")
    else:
        spark.sql("use ieee")
        spark.sql("drop table all_features_predict")
        spark.sql("create table if not exists all_features_predict "
                  "as select a.*, b.u_q_sentences_words_sim_w2v from "
                  "ieee.features_predict a left outer join "
                  "train_data_sim_tmp b on a.u_id = b.u_id and a.q_id "
                  "= b.q_id")

if __name__ == '__main__':
    # Write data to hive and create full_features table.
    w2v_train_predict()

