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

if __name__ == '__main__':
    ingest.ingest_data()

