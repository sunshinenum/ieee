#! /usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Liguo Chen'
import sys
sys.path.append("..")
from conf.conf import *
from chen.ml.similarity import *
import math
from sklearn.datasets import *
from sklearn.cross_validation import cross_val_score
from sklearn.tree import DecisionTreeClassifier
from sklearn import linear_model
from sklearn.naive_bayes import MultinomialNB
from sklearn.svm import SVC
from write_out import *
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier

"""
user_info:          uid_string, u_label_list, words_list, char_list
question_info:      qid_string, q_label_list, words_list, char_list,
                                thumb_up_int, answers_int, good_answers_int
invited_info_train: qid_string, uid_string, label_bool
temp.csv:           qid_string, uid_string, prob_label_float
"""


def train_predict():
    # load train data
    x, y = load_svmlight_file(path_vectors_train, g_features_count + 1)
    print "[+] Training ..."

    # decision tree [ 0.89345079  0.89400012  0.89574983  0.89621567  0.89523693]
    # clf = DecisionTreeClassifier(random_state=0)
    # clf.fit(x, y)

    # lr
    clf = linear_model.SGDClassifier(loss="log",
                                     alpha=0.001,
                                     n_iter=2000,
                                     fit_intercept=True,
                                     penalty="l2",
                                     n_jobs=4)
    clf.fit(x, y)

    # # KNN
    # clf = KNeighborsClassifier(n_neighbors=10)
    # clf.fit(x, y)

    # naive bayes
    # clf = MultinomialNB()
    # clf.fit(x, y)

    # rfc
    # clf = RandomForestClassifier(n_estimators=10, max_depth=None,
    #                              min_samples_split=2, random_state=0)
    # clf.fit(x, y)

    # svm
    # clf = SVC()
    # clf.fit(x, y)

    # cs test
    cs_result = cross_val_score(clf, x, y, cv=5)
    print cs_result
    clf.fit(x, y)

    # load predict data and predict
    x_test, y_test = load_svmlight_file(path_vectors_test, g_features_count + 1)
    print "[+] Predicting ..."
    result = clf.predict_proba(x_test)
    # write_out_result(result, data_without_label)
    write_out_result_hive(result)
    # write_out_result_give_low_score_zero(result)

    # write out x train result
    result = clf.predict_proba(x)
    write_out_train_result(result)
