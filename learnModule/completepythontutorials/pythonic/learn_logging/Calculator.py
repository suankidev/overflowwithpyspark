import logging

import Employee
from Employee import log_path

log_path = "C:\\Users\\sujee\\pydev\\pyspark_learn_project\\resources\\output"

calc_log = log_path + "/calc_log.log"
# logging.basicConfig(filename=emp_log,
#                     level=logging.INFO,
#                     format='%(asctime)s : %(levelname)s : %(name)s :  %(message)s')


logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s :  %(message)s')
file_handler = logging.FileHandler(calc_log)

file_handler.setFormatter(formatter)

file_handler.setLevel(level=logging.ERROR) #setting particularly to the  log error
logger.addHandler(file_handler)



def add(x, y):
    return x + y


x, y = 10, 20
# logging.info('add: {} + {} = {}'.format(x, y, add(x, y)))
logger.info('add: {} + {} = {}'.format(x, y, add(x, y)))


logger.error('/y......')
