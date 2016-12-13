#!/usr/bin/env python
# coding=utf8
import os
# data path
root = os.getcwd().replace(os.getcwd().split("/")[-1], "")[:-1]
path_train_data = "%s/data/invited_info_train.txt" % root
path_question_info = "%s/data/question_info.txt" % root
path_user_info = "%s/data/user_info.txt" % root
path_validate_nolabel = "%s/data/validate_nolabel.txt" % root
path_test_data = "%s/data/test_nolabel.txt" % root
path_vectors_train = "%s/data/vectors_train.csv" % root
path_vectors_test = "%s/data/vectors_test.csv" % root
path_results = "%s/data/temp.csv" % root
path_table_watch = "%s/data/watch.csv" % root
path_train_predict_watch = "%s/data/watch_train.csv" % root
# parameters
# sigmoid x => x / hot_adjustment
hot_adjustment = 1.0
answers_count_adjustment = 2.0
question_count_adjustment = 1.0
user_answer_history_top_rate = 0.5
# features_count
g_features_count = 4
