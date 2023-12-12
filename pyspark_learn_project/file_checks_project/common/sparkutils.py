from pyspark.sql.session import SparkSession
import os


def get_spark_session():
    user_db = os.getenv('USER')
    spark = SparkSession.builder.master('local[2]').enableHiveSupport().getOrCreate()
    # spark.catalog.setCurrentDatabase(user_db)
    return spark


