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

"""
user_info:          uid_string, u_label_list, words_list, char_list
question_info:      qid_string, q_label_list, words_list, char_list,
                                thumb_up_int, answers_int, good_answers_int
invited_info_train: qid_string, uid_string, label_bool
temp.csv:           qid_string, uid_string, prob_label_float
"""
from gensim import corpora, models, similarities
# model = models.Word2Vec(sentences, size=100, window=5, min_count=5, workers=4)


def train_w2v(train_data):

    pass


if __name__ == '__main__':
    train_w2v()
