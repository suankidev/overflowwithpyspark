import re

from mftool import Mftool
from flowutils.commonutils.SparkUtils import get_spark_session,get_jdbc_df


#
# sys.path.insert(0, r'')
#
# # set logging
# logging.config.fileConfig(r"conf/logging.conf")
# logger = logging.getLogger(__name__)
#
# logger.info("setting sys path for src.zip file")
#
# if os.path.exists('python.zip'):
#     sys.path.insert(0, 'python.zip')
# else:
#     sys.path.insert(0, './python')

def run_pipeline():
    session = get_spark_session()
    from ingest.run_ingest_overflow import test_oracle_date
    test_oracle_date(session)


if __name__ == '__main__':
    print("*" * 20)

    print("pipeline started")

    run_pipeline()


