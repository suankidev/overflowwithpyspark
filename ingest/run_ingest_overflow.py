import pyspark.sql
from pyspark.sql.functions import col


def ingest_data(spark: pyspark.sql.SparkSession):
    print("running ingest workflow!")
    df = spark.range(50).toDF("num").where(col("num") % 2 == 0)
    df.show(5,truncate=False)
