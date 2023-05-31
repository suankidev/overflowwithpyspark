import datetime
import os
import pathlib
import sys

import pyspark.sql.session

import conf.setenv as setenv
import logging.config
import ingest.run_ingest_overflow as ingest

sys.path.insert(0, r'')

# set logging
logging.config.fileConfig(r"conf/logging.conf")
logger = logging.getLogger(__name__)

logger.info("setting sys path for src.zip file")

if os.path.exists('python.zip'):
    sys.path.insert(0, 'python.zip')
else:
    sys.path.insert(0, './python')


def init_pipeline(spark: pyspark.sql.session.SparkSession):
    if os.path.exists(setenv.base_path):
        for file in os.listdir(setenv.base_path):
            print(file)


def build_output(entry: pathlib.Path, long=False):
    if long:
        size = entry.stat().st_size
        date = datetime.datetime.fromtimestamp((entry.stat().st_mtime)).strftime("%b %d %H:%M:%S")

        return f"{size:>6d} {date} {entry.name}"
    return entry.name


if __name__ == '__main__':
    ingest.ingest_data()


