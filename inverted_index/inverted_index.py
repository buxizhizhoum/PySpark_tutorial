#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
inverted index
"""
from __future__ import print_function
import os

from pyspark import SparkConf
from pyspark import SparkContext

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

conf = SparkConf().setAppName("inverted_index").setMaster("local[2]")
sc = SparkContext(conf=conf)


def invert_one_line(row):
    res = []
    for i in range(1, len(row)):
        tmp = (row[i], row[0])
        res.append(tmp)
    return res


def main():
    # preprocess, split line to id  word1 word2 ...
    id_words_pairs = raw_rdd.map(lambda line: line.strip("!,.")) \
        .map(lambda line: line.split(" "))
    print(id_words_pairs.collect())
    # convert to word1 id, word2 id ...
    # remove duplicated item
    inverted_rdd = id_words_pairs.flatMap(invert_one_line).distinct()
    print(inverted_rdd.collect())
    # group by words
    inverted_index = inverted_rdd.reduceByKey(
        lambda x, y: "{} {}".format(x, y))
    print(inverted_index.collect())


if __name__ == "__main__":
    raw_rdd = sc.textFile("../data/inverted_index.txt", 3)

    main()











