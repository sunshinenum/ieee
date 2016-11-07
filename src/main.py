#!/usr/bin/env python
# coding=utf8
from preprocessing import *
from extract_features import *
from train_test import *
from write_out import *
import numpy as np
import cPickle as pickle


def get_ranks(labeled_data):
    """
    Get rank of each question.
    """
    pass


def train_model_with_features(features):
    pass


def train_model_with_ranks(ranks):
    pass


def test(model):
    pass


def predict(model, data_without_label):
    pass


def write_out(result):
    pass


def main():
    # read train_data, test_data.
    print "[+] Loading train data and test data ..."
    data_with_label, data_without_label = read_data()
    # read question info and user info.
    print "[+] Loading user_info and question info ..."
    user_info, question_info = read_dict()
    # Append a column of user answered question label count dict to user_info.
    # user_info = get_all_history_labels(user_info, question_info, data_with_label)
    # dump to file
    # with open("user_info", "wb") as fw:
    #     pickle.dump(user_info, fw, -1)
    # # write out watch table
    # print "Writing out watch table ..."
    # write_out_watch_table(data_with_label, user_info, question_info)
    # # extract train features
    print "[+] Extracting features ..."
    features = get_features(data_with_label
                            , data_with_label
                            , user_info
                            , question_info)
    write_out_in_libsvm(features, 6, path_vectors_train)
    # extract test features
    features_test = get_features(data_without_label
                                 , data_with_label
                                 , user_info
                                 , question_info
                                 , has_label=False)
    write_out_in_libsvm(features_test, 6, path_vectors_test, has_label=False)
    # train model
    train_predict(data_without_label)
    # ranks = get_ranks(features)
    # feature_based_model = train_model_with_features(features)
    # rank_based_model = train_model_with_ranks(ranks)
    # test(feature_based_model)
    # test(rank_based_model)
    # feature_based_result = predict(feature_based_model, data_without_label)
    # rank_based_result = predict(rank_based_model, data_without_label)
    # write_out(feature_based_result)
    # write_out(rank_based_result)

if __name__ == "__main__":
    main()
