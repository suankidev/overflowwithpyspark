import os
import sys
import logging.config

from src.flowutils.commonutils.SparkUtils import  get_spark_session
from src.ingest import  run_ingest_overflow
sys.path.insert(0, r'')

# set logging
logging.config.fileConfig(r"conf/logging.conf")
logger = logging.getLogger(__name__)

logger.info("setting sys path for src.zip file")

if os.path.exists('python.zip'):
    sys.path.insert(0, 'python.zip')
else:
    sys.path.insert(0, './python')


def run_pipeline():
    run_ingest_overflow.ingest_data(get_spark_session())




if __name__ == '__main__':
    print("*" * 20)
    print(__name__)
    run_pipeline()
    print("*" * 20)
