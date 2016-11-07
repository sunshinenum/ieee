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
question_info:      qid_string, q_label_string, words_list, char_list,
                                thumb_up_int, answers_int, good_answers_int
invited_info_train: qid_string, uid_string, label_bool
temp.csv:           qid_string, uid_string, prob_label_float
"""


def sigmod(x, adjustment=1.0):
    return 1.0 / (1.0 + math.pow(math.e, 0.0 - x / adjustment))


def get_hot_score(thumb_up, answers_count, good_answers_count):
    score = sigmod(math.log(thumb_up)) \
            + sigmod(math.log(answers_count)) \
            + sigmod(math.log(good_answers_count))
    return score


def get_answers_score(labeled_data):
    # uid : score
    answer_count_scores = dict()
    for line in labeled_data:
        if line[1] not in answer_count_scores and line[2] == '1':
            answer_count_scores[line[1]] = 1
        elif line[1] in answer_count_scores and line[2] == '1':
            answer_count_scores[line[1]] += 1
    for k, v in answer_count_scores.items():
        if answer_count_scores[k] == 0:
            answer_count_scores[k] = 0.0
        else:
            answer_count_scores[k] = sigmod(math.log(answer_count_scores[k]))
    return answer_count_scores


def get_questions_score(labeled_data):
    """
    Score the question answered times.
    :param labeled_data: invited train.
    :return: dict question_score
    """
    question_score = dict()
    for line in labeled_data:
        if line[0] not in question_score and line[2] == '1':
            question_score[line[0]] = 1
        elif line[0] in question_score and line[2] == '1':
            question_score[line[0]] += 1
    for k, v in question_score .items():
        question_score[k] = sigmod(question_score[k]
                                        , question_count_adjustment)
    return question_score


def get_all_history_labels(user_info, question_info, labeled_data):
    """
    Add user answered questions labels to user_info[4]
    :param user_info: user_info
    :param question_info: question_info
    :param labeled_data: invited_train_data
    :return: user_info
    """
    for key, user in user_info.items():
        labels = get_history_labels(user[0], labeled_data, question_info)
        user_info[key].append(labels)
    return user_info


def get_history_labels(uid, labeled_data, question_info):
    """
    Given uid, get user answer history question labels.
    :param uid: string user id
    :param labeled_data: invited_info_train
    :param question_info: question_info
    :return: set user_labels
    """
    history_labels = []
    for line in labeled_data:
        if line[1] == uid:
            history_labels.append(question_info[line[0]][1])
    # labels counts dict
    labels_count = dict()
    for label in history_labels:
        if label not in labels_count:
            labels_count[label] = history_labels.count(label)
    # sort counts
    sorted_counts_tuples = sorted(labels_count.iteritems()
                           , key=lambda d:d[1], reverse=True)
    high_counts_tuples = sorted_counts_tuples\
        [:int(len(sorted_counts_tuples) * user_answer_history_top_rate)]
    # top $user_answer_history_top_rate counts
    frequent_labels_count = dict(high_counts_tuples)
    return frequent_labels_count


def write_out_watch_table(data, user_info, question_info):
    f = open(path_table_watch, "w")
    for line in data:
        uid = line[1]
        qid = line[0]
        # label, q_label, u_label, q_words, u_words, q_chars, u_chars,
        # q_thumb_up_int, q_answers_int, q_good_answers_int
        f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"
                % (line[2]
                   , question_info[qid][1]
                   , user_info[uid][1]
                   , question_info[qid][2]
                   , user_info[uid][2]
                   , question_info[qid][3]
                   , user_info[uid][3]
                   , question_info[qid][4]
                   , question_info[qid][5]
                   , question_info[qid][6]))
    f.close()


def get_features(data, labeled_data, user_info, question_info, has_label=True):
    """
    Calculate features into a labeled matrix.
    """
    # get answers score
    answers_scores = get_answers_score(labeled_data)
    # Get question score
    question_scores = get_questions_score(labeled_data)
    # features qid, uid, feature_0, feature_1 ... feature_n, label
    features = []
    for line in data:
        feature = []
        qid = line[0]
        uid = line[1]
        # feature.append(line[0])
        # feature.append(line[1])
        # feature 0: Similarity of user_profile_word and question_word.
        # feature.append(sem_similarity(user_info[uid][2]
        #                        , question_info[qid][2]))
        if question_info[qid][1] in user_info[uid][1]:
            feature.append(1.0)
        else:
            feature.append(0.0)
        # feature 1: Does user and question have common labels
        # if question_info[qid][1] in user_info[uid][4]:
        #     feature.append(sigmod(user_info[uid][4][question_info[qid][1]]))
        # else:
        #     feature.append(0.0)
        feature.append(rouge_1(user_info[uid][1], question_info[qid][1]))
        # # feature 2: Is this question hot?
        # feature.append(get_hot_score(question_info[qid][4]
        #                              , question_info[qid][5]
        #                              , question_info[qid][6]))
        feature.append(0.0)
        # feature 3: Does this user like answer question?
        # Have this user ever answer question?
        if uid in answers_scores:
            feature.append(answers_scores[uid])
        else:
            feature.append(0.0)
        # Is this question easy to answer?
        if qid in question_scores:
            feature.append(question_scores[qid])
        else:
            feature.append(0.0)
        # label
        if has_label:
            feature.append(float(line[2]))
        features.append(feature)
    return features


if __name__ == "__main__":
    print get_hot_score(23.0, 40.0, 23.0)
