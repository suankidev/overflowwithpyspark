import logging

from pyspark.sql.session import SparkSession


def get_spark_session():
    # os.environ['PYSPARK_PYTHON'] = sys.executable
    # os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.master("local[2]") \
        .appName("sparkdemo").getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    return spark


def get_jdbc_df(metadata, spark):
    jdbc_df = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin:suanki/testpass@//localhost:1521/PDBORCL") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("user", 'suanki') \
        .option('password', 'testpass') \
        .option('dbtable', metadata.get('table')).load()
    return jdbc_df


class SparkUtils:
    pass
