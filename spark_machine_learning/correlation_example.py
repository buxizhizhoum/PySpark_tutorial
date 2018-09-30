#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
from official spark machine learning example.
examples/src/main/python/ml/correlation_example.py
"""
from __future__ import print_function
import os

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 ' \
    '--master local[2] ' \
    'pyspark-shell'


spark = SparkSession\
    .builder\
    .appName("correlation_example")\
    .master("local[2]")\
    .getOrCreate()


def main():
    data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
            (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
            (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
            (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]

    df = spark.createDataFrame(data, ["features"])
    df.show()

    r1 = Correlation.corr(df, "features").head()
    print("Pearson Correlation matrix: ", str(r1[0]))

    r2 = Correlation.corr(df, "features", "spearman").head()
    print("Spearman Correlation matrix:", str(r2[0]))


if __name__ == "__main__":
    main()

