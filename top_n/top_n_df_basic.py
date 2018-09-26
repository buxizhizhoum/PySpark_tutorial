#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
get the top n words with dataframe

refer: https://stackoverflow.com/a/34293270/8025086
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

TOP_N = 3

spark = SparkSession.builder.appName("TopN").master("local[2]").getOrCreate()
sc = spark.sparkContext


if __name__ == "__main__":
    text_df = spark.read.text("../data/word_count.txt").toDF("text")
    text_df.printSchema()
    print(text_df.take(10))

    # f.regexp_replace is used to remove !,.
    res = text_df.withColumn("words", f.explode(f.split(f.regexp_replace(f.col("text"), r"[!,.]$", ""), " ")))\
                 .groupBy("words")\
                 .count()\
                 .sort("count", ascending=False)

    print(res.show())
