#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
rdd to df in spark 2.0
https://stackoverflow.com/a/47341054/8025086
"""
from __future__ import print_function
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
# pay attention here, jars could be added at here
os.environ['PYSPARK_SUBMIT_ARGS'] = 'pyspark-shell'


spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()

a = spark.createDataFrame([[1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "e"]], ['ind', "state"])
a.show()
