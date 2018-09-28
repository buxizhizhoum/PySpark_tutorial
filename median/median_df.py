#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
get median
"""
from __future__ import print_function
import os

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'


conf = SparkConf().setAppName("Media").setMaster("local[2]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()


def median(rdd, q):
    # df = spark.createDataFrame(rdd, FloatType)
    row = Row("data")
    df = rdd.map(row).toDF()
    df.printSchema()
    res = df.approxQuantile("data", [q], 0)
    print(res)
    return res


def preprocess(rdd):
    # pre process
    res_rdd = rdd \
        .map(lambda line: line.strip(", ")) \
        .flatMap(lambda line: line.split(",")) \
        .map(lambda x: float(x) if x.isdigit() else None) \
        .filter(lambda x: x) \
        .sortBy(lambda x: x)
    return res_rdd


if __name__ == "__main__":
    text_rdd = sc.textFile("../data/numbers_to_calculate_media.txt")
    print("partitions:", text_rdd.getNumPartitions())
    print("text_rdd:", text_rdd.take(10))

    data_rdd = preprocess(text_rdd)
    # print("data_rdd:", data_rdd.take(10))
    print("data_rdd:", data_rdd.collect())

    res = median(data_rdd, 0.5)














