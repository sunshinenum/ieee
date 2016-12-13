#!/usr/bin/env python
# coding=utf8
from import_data_locally_ import *
from extract_features_locally import *
from train_test import *
from write_out import *
from extract_features_from_hive import *


def main():
    print "[+] Extracting features from hive ..."
    features = get_features_from_hive(is_test=False)
    write_out_in_libsvm(features, path_vectors_train, has_label=True)
    features_test = get_features_from_hive(is_test=True)
    write_out_in_libsvm(features_test, path_vectors_test, has_label=False)
    train_predict()


if __name__ == "__main__":
    main()
