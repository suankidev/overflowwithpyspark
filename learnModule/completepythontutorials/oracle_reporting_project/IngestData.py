import pyspark.sql.session
from oracle_reporting_project.utils.sparkUtils.spark_utils import get_jdbc_df,write_jdbc_df


class IngestData:

    def __init__(self, logger, metadata, spark):
        self.logger = logger
        self.metadata = metadata
        self.spark: pyspark.sql.session.SparkSession = spark

    def save_data(self):
        table_df: pyspark.sql.dataframe.DataFrame = get_jdbc_df(self.logger, self.metadata, self.spark)
        write_jdbc_df(self.metadata.get('table'), table_df)


def ingest_data(logger, metadata, spark):
    ingest = IngestData(logger, metadata, spark)
    ingest.save_data()
