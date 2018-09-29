#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
spark structured streaming word count
come from Learning PySpark by Tomasz Drabas.
"""
from __future__ import print_function
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'


spark = SparkSession\
    .builder\
    .appName("structed_streaming_word_count")\
    .master("local[2]")\
    .getOrCreate()


def initialize_stream():
    """
    initialize read stream, read stream from localhost:9999
    :return:
    """
    lines = spark\
        .readStream\
        .format("socket")\
        .option("host", "localhost")\
        .option("port", 9999)\
        .load()
    return lines


def preprocess(lines):
    """
    split lines to words, process in dataframe
    :param lines:
    :return:
    """
    words = lines.select(
        f.explode(
            f.split(lines.value, " ")  # split with space
        ).alias("words")
        )
    return words


def count(words):
    """
    group by words in dataframe
    :param words:
    :return:
    """
    word_count = words.groupBy("words").count()
    return word_count


def output(word_count):
    """
    output to console
    :param word_count:
    :return:
    """
    task = word_count\
        .writeStream\
        .outputMode("complete")\
        .format("console")
    query = task.start()
    query.awaitTermination()


if __name__ == "__main__":
    lines = initialize_stream()
    words = preprocess(lines)
    counts = count(words)
    output(counts)



