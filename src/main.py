#!/usr/bin/env python


def read_dict():
    """
    Read user_info.txt, question_info.txt into two dictionaries.
    key: id
    value: list
    """
    pass


def read_data():
    """
    Read invited_info_train.txt into a matrix.
    """
    pass


def get_features(labeled_data):
    """
    Caculate features into a labeled matrix.
    """
    pass


def get_ranks(labeled_data):
    """
    Get rank of each question.
    """
    pass


def rouge1(sentence0, sentence1):
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
    
    data_with_label, data_without_label = read_data()
    features = get_features(data_with_label)
    ranks = get_ranks(features)
    feature_based_model = train_model_with_features(features)
    rank_based_model = train_model_with_ranks(ranks)
    test(feature_based_model)
    test(rank_based_model)
    feature_based_result = predict(feature_based_model, data_without_label)
    rank_based_result = predict(rank_based_model, data_without_label)
    write_out(feature_based_result)
    write_out(rank_based_result)
