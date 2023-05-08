import os

from src.flowutils.sparkUtils import get_spark_session


pwd = os.getcwd()
# sys.path.extend([pwd])


def get():
    return get_spark_session()





