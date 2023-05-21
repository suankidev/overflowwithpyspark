import os
import re
import sys

import pyspark.sql.session

from flowutils.SparkUtils import get_spark_session
import ingest.run_ingest_overflow as ingest
import conf.setenv as setenv

import logging.config

# set logging
logging.config.fileConfig("./conf/logging.conf")
logger = logging.getLogger(__name__)

logger.info("setting sys path for src.zip file")

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')


def init_pipeline(spark: pyspark.sql.session.SparkSession):
    if os.path.exists(setenv.base_path):
        for file in os.listdir(setenv.base_path):
            if re.compile(r'')


if __name__ == '__main__':
    logger.info("main programm started.")
    spark = 'test'#get_spark_session()
    init_pipeline(spark)
