import logging

import pyspark.sql.session
from pyspark.sql.session import SparkSession
import os
import sys
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def get_spark_session():
    # os.environ['PYSPARK_PYTHON'] = sys.executable
    # os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.master("local[2]") \
        .appName("sparkdemo").getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    return spark


def get_flight_df(spark: pyspark.sql.SparkSession = None):
    if not spark:
        spark = get_spark_session()
    file_path = "C:\\Users\\sujee\\Desktop\\spark_input\\fligt_data\\2015*.csv"
    flight_schema = StructType([StructField("target", StringType(), False),
                                StructField("source", StringType(), False),
                                StructField("totalflight", IntegerType(), False, metadata={"hello": "world"})]
                               )
    flight_df = spark.read.option("header", "true").schema(flight_schema).format("csv").load(file_path) \
        .sortWithinPartitions(col('totalflight'))

    return flight_df


def get_retail_data(spark: pyspark.sql.session.SparkSession = None):
    if not spark:
        spark = get_spark_session()
    file_path = "C:\\Users\\sujee\\Desktop\\spark_input\\retail-data\\*.csv"

    retail_df = spark.read.format('csv').option('header', True).option('inferSchema', True) \
        .load(file_path)

    return retail_df
