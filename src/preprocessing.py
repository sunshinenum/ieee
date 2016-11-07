#!/usr/bin/env python
# coding=utf8
import sys
sys.path.append("..")
from conf.conf import *


def read_dict():
    """
    Read user_info.txt, question_info.txt into two dictionaries.
    key: id
    value: list
    """
    user_info = dict()
    fu = open(path_user_info, "r")
    for line in fu:
        values_list = []
        values_raw = line.split()
        values_list.append(values_raw[0])
        values_list.append(values_raw[1].split("/"))
        values_list.append(values_raw[2].split("/"))
        values_list.append(values_raw[3].split("/"))
        user_info[values_raw[0]] = values_list
    question_info = dict()
    fq = open(path_question_info, "r")
    for line in fq:
        values_list = []
        values_raw = line.split()
        values_list.append(values_raw[0])
        values_list.append(values_raw[1])
        values_list.append(values_raw[2].split("/"))
        values_list.append(values_raw[3].split("/"))
        values_list.append(values_raw[4])
        values_list.append(values_raw[5])
        values_list.append(values_raw[6])
        question_info[values_raw[0]] = values_list
    return user_info, question_info


def read_data():
    """
    Read invited_info_train.txt and validate_nolabel.txt into a matrix.
    """
    f_train = open(path_train_data, "r")
    train_data = []
    for line in f_train:
        train_data.append(line.split())
    f_train.close()
    f_test = open(path_validate_nolabel, "r")
    test_data = []
    for line in f_test:
        test_data.append(line.strip().split(","))
    f_test.close()
    return train_data, test_data


if __name__ == '__main__':
    user_info, question_info = read_dict()
    train_data, test_data = read_data()
    print "[+] Read data complete."
