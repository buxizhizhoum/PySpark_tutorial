#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
from official spark machine learning example.
examples/src/main/python/ml/correlation_example.py
"""
from __future__ import print_function
import os

import pyspark.ml.feature as ft
import pyspark.ml.classification as cl
import pyspark.ml.evaluation as ev
from pyspark.ml import Pipeline

from pyspark.sql import SparkSession
from pyspark.sql import types as typ

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 ' \
    '--master local[2] ' \
    'pyspark-shell'


spark = SparkSession\
    .builder\
    .appName("survival_chance_prediction")\
    .master("local[2]")\
    .getOrCreate()

labels = [('INFANT_ALIVE_AT_REPORT', typ.IntegerType()),
          ('BIRTH_PLACE', typ.StringType()),
          ('MOTHER_AGE_YEARS', typ.IntegerType()),
          ('FATHER_COMBINED_AGE', typ.IntegerType()),
          ('CIG_BEFORE', typ.IntegerType()),
          ('CIG_1_TRI', typ.IntegerType()),
          ('CIG_2_TRI', typ.IntegerType()),
          ('CIG_3_TRI', typ.IntegerType()),
          ('MOTHER_HEIGHT_IN', typ.IntegerType()),
          ('MOTHER_PRE_WEIGHT', typ.IntegerType()),
          ('MOTHER_DELIVERY_WEIGHT', typ.IntegerType()),
          ('MOTHER_WEIGHT_GAIN', typ.IntegerType()),
          ('DIABETES_PRE', typ.IntegerType()),
          ('DIABETES_GEST', typ.IntegerType()),
          ('HYP_TENS_PRE', typ.IntegerType()),
          ('HYP_TENS_GEST', typ.IntegerType()),
          ('PREV_BIRTH_PRETERM', typ.IntegerType())]


# def main():


if __name__ == "__main__":
    # specify schema structure of the df
    schema = typ.StructType(
        [typ.StructField(e[0], e[1], False) for e in labels])
    births = spark.read.csv(
        "../data/births_transformed.csv.gz", header=True, schema=schema)

    births = births.withColumn("BIRTH_PLACE_INT",
                               births["BIRTH_PLACE"].cast(typ.IntegerType()))

    encoder = ft.OneHotEncoder(inputCol="BIRTH_PLACE_INT",
                               outputCol="BIRTH_PLACE_VEC")
    # column with all features collected together
    features_creator = ft.VectorAssembler(
        inputCols=[col[0] for col in labels[2:]]
                  + [encoder.getOutputCol()],
        outputCol="features")

    logistic = cl.LogisticRegression(
        maxIter=10, regParam=0.01, labelCol="INFANT_ALIVE_AT_REPORT")

    pipe = Pipeline(stages=[encoder, features_creator, logistic])
    # train and test
    births_train, births_test = births.randomSplit([0.7, 0.3], seed=666)
    model = pipe.fit(births_train)
    model_test = model.transform(births_test)
    print(model_test.take(1))

    # evaluation
    evaluator = ev.BinaryClassificationEvaluator(
        rawPredictionCol="probability", labelCol="INFANT_ALIVE_AT_REPORT")

    print(evaluator.evaluate(
        model_test, {evaluator.metricName: "areaUnderROC"}))
    print(evaluator.evaluate(
        model_test, {evaluator.metricName: "areaUnderPR"}))

