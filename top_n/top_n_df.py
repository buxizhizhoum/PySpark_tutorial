#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
get the top n words with dataframe

refer: https://stackoverflow.com/a/34293270/8025086
"""
from __future__ import print_function
import os

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 ' \
    'pyspark-shell'

TOP_N = 3

spark = SparkSession.builder.appName("TopN").master("local[2]").getOrCreate()
sc = spark.sparkContext


if __name__ == "__main__":
    text_rdd = sc.textFile("../data/word_count.txt")
    print(text_rdd.take(10))
    words_count = text_rdd.map(lambda line: line.rstrip("!,.")).flatMap(lambda line: line.split()).map(lambda word: (word, 1))
    print(words_count.take(10))

    # convert to dataframe after flatMap
    text_df = spark.createDataFrame(words_count, ["words", "count"])
    text_df.printSchema()
    print(text_df.show(10))
    group_by = text_df.groupBy("words").sum().orderBy("sum(count)", ascending=False).collect()
    print(group_by)
