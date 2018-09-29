#!/usr/bin/python
# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

# this is different with the DStream.
# pay attention to the packages version, spark version should be 2.3.0
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 ' \
    '--master local[2] ' \
    'pyspark-shell'

spark = SparkSession \
    .builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

# create a kafka source for streaming queries
# todo: batch duration
df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")\
    .option("subscribe", "test")\
    .load()

df.printSchema()
processed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# output to console
output_df = processed_df.writeStream\
    .format("console")\
    .option("truncate", "false")\
    .start()

output_df.awaitTermination()

