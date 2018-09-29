#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
example from spark official document
"""
import os

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 ' \
    '--master local[2] ' \
    'pyspark-shell'

spark = SparkSession\
    .builder\
    .appName("basic_exercise")\
    .getOrCreate()

df = spark.read.json("/usr/local/spark/examples/src/main/resources/people.json")
df.show()

df.select("name").show()
df.select(df["name"]).show()

df.select(df["name"], df["age"] + 2).show()

df.filter(df["age"] > 21).show()

df.groupBy(df["age"]).count().show()

print("sql operation", "-"*60)
df.createOrReplaceTempView("people")
sql_df = spark.sql("select * from people")
sql_df.show()





