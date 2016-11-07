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


def write_out_result(predict_result, test_data):
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


def write_out_features(features, path):
    f = open(path, "w")
    for line in features:
        for index, value in enumerate(line):
            f.write("%f" % value)
            if index != len(line) - 1:
                f.write(",")
        f.write("\n")
    f.close()


def write_out_in_libsvm(features, n, path, has_label=True):
    f = open(path, "w")
    for line in features:
        for i in range(n):
            if i == 0:
                if has_label:
                    f.write("%f " % line[n - 1])
                else:
                    f.write("0.0 ")
            elif i != n - 1:
                f.write("%d:%f " % (i - 1, line[i - 1]))
            else:
                f.write("%d:%f" % (i - 1, line[i - 1]))
        f.write("\n")
    f.close()
