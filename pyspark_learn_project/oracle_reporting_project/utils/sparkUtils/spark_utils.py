import pyspark.sql.dataframe
from pyspark.sql.session import SparkSession


def get_spark_session(logger):
    try:
        spark = (SparkSession.builder.master('local[2]')
                 .appName("oracle_and_file_project")
                 .getOrCreate()
                 )
        logger.info("spark session created")
        return spark
    except Exception as msg:
        logger.Exception(msg)


def get_jdbc_df(logger, metadata, spark):
    jdbc_df = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin:suanki/testpass@//localhost:1521/PDBORCL") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("user", 'suanki') \
        .option('password', 'testpass') \
        .option('dbtable', metadata.get('table')).load()
    return jdbc_df


def write_jdbc_df(table_name, table_df:pyspark.sql.dataframe.DataFrame ):



