from pyspark.sql.session import SparkSession

def get_spark_session():
    spark = SparkSession.builder.appName("spark-optimization")\
    .master('local[2]').getOrCreate()


