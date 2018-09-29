#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
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

# initialize read stream, read stream from localhost:9999
lines = spark\
    .readStream\
    .format("socket")\
    .option("host", "localhost")\
    .option("port", 9999)\
    .load()

# words = lines.select(
#     f.explode(
#         f.split(lines.value, " ")  # split with space
#     ).alias("words")
#     )

words = lines.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)

# word_count = words.groupBy("words").count()

# output
# task = word_count\
#     .writeStream\
#     .outputMode("complete")\
#     .format("console")
# query = task.start()
# query.awaitTermination()


