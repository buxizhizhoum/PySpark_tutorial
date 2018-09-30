#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
example from spark official document
"""
import os

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 ' \
    '--master local[2] ' \
    'pyspark-shell'

conf = SparkConf().setAppName("basic_exercise")
sc = SparkContext(conf=conf)
spark = SparkSession\
    .builder\
    .config(conf=conf)\
    .getOrCreate()

# lines = sc.textFile("examples/src/main/resources/people.txt")
lines = sc.textFile("/usr/local/spark/examples/src/main/resources/people.txt")
parts = lines.map(lambda line: line.split(", "))
# define column name
people = parts.map(lambda line: Row(name=line[0], age=line[1]))
print(people.take(10))

schema_people = spark.createDataFrame(people)
schema_people.createOrReplaceTempView("people")

teenagers = spark.sql("select * from people where age >= 13 and age <= 28")
teenagers.show()

rdd = teenagers.rdd.map(lambda x: (x.name, x.age))
print(rdd.collect())


