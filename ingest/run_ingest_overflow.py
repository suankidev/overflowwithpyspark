from flowutils.SparkUtils import get_spark_session


def ingest_data():
    print("running ingest workflow!")
    spark = get_spark_session()

    print(spark)
