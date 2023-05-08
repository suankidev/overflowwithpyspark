import logging

from pyspark.sql.session import SparkSession


def get_spark_session():
    # os.environ['PYSPARK_PYTHON'] = sys.executable
    # os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.master("local[2]") \
        .appName("sparkdemo").getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    return spark





