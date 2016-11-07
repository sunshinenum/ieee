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

"""
user_info:          uid_string, u_label_list, words_list, char_list
question_info:      qid_string, q_label_list, words_list, char_list,
                                thumb_up_int, answers_int, good_answers_int
invited_info_train: qid_string, uid_string, label_bool
temp.csv:           qid_string, uid_string, prob_label_float
"""


def train_predict(data_without_label):
    # load train data
    x, y = load_svmlight_file(path_vectors_train, 5)

    # decision tree [ 0.89345079  0.89400012  0.89574983  0.89621567  0.89523693]
    # clf = DecisionTreeClassifier(random_state=0)
    # clf.fit(x, y)

    # lr
    clf = linear_model.SGDClassifier(loss="log", alpha=0.01, n_iter=200, fit_intercept=True)
    clf.fit(x, y)

    # naive bayes
    # clf = MultinomialNB()
    # clf.fit(x, y)

    # svm
    # clf = SVC()
    # clf.fit(x, y)

    # cs test
    print "[+] Training ..."
    cs_result = cross_val_score(clf, x, y, cv=5)
    print cs_result

    # load predict data and predict
    x_test, y_test = load_svmlight_file(path_vectors_test, 5)
    print "[+] Predicting ..."
    result = clf.predict_proba(x_test)
    write_out_result(result, data_without_label)
    print "[+] Writing out result."
