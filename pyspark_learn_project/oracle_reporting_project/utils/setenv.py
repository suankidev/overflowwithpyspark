import os
from oracle_reporting_project.utils.loggerUtils.logger_utils import *

os.environ.update({'log_path': 'C:\\Users\\sujee\pydev\\pyspark_learn_project\\logs'})
os.environ.update({'config_file': 'C:\\Users\\sujee\\pydev\\pyspark_learn_project\\conf'})


def init_logger(log_postfix):
    return get_logger(log_postfix)
