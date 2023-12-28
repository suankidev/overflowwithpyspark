import logging
import logging.config

logging.config.fileConfig(fname='C:\\Users\\sujee\\pydev\\pyspark_learn_project\\conf\\logging_to_file.conf')

logger = logging.getLogger("customLogger")

def add(a,b):
    res = a+b
    logger.info("addition of two number is {} is call from {}".format(res,__name__))