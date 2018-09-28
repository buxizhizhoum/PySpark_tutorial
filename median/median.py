#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
get median
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


conf = SparkConf().setAppName("Media").setMaster("local[2]")
sc = SparkContext(conf=conf)


def median(rdd, q):
    sort_rdd = rdd.sortBy(lambda x: float(x))
    # print("sort_data:", sort_data.take(10))
    print("sort_data:", sort_rdd.collect())

    # zipWithIndex add index after data has been sorted
    # the following map swap index and data which will easy the lookup function
    index_data = sort_rdd\
        .zipWithIndex() \
        .map(lambda pair: (pair[1], pair[0])) \
        .cache()

    count = index_data.count()
    index = int((count - 1) * q)  # not all conditions.

    print("count: {}, index: {}".format(count, index))
    res = index_data.lookup(index)
    print(res)
    return res


if __name__ == "__main__":
    text_rdd = sc.textFile("../data/numbers_to_calculate_media.txt")
    print("partitions:", text_rdd.getNumPartitions())
    print("text_rdd:", text_rdd.take(10))

    # pre process
    data_rdd = text_rdd \
        .map(lambda line: line.strip(", ")) \
        .flatMap(lambda line: line.split(","))
    # print("data_rdd:", data_rdd.take(10))
    print("data_rdd:", data_rdd.collect())

    res = median(data_rdd, 0.5)














