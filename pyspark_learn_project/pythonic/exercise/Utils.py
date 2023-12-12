""""
CEW
IDN
"""

import logging

# logging.basicConfig(level=logging.DEBUG)
log_path = "C:\\Users\\sujee\\pydev\\pyspark_learn_project\\resources\output"

# logging.basicConfig(filename=log_path+'test.log', level=logging.DEBUG)
#
logging.basicConfig(filename=log_path + 'test.log', level=logging.DEBUG,
                  format='%(asctime)s : %(levelname)s : %(name)s :  %(message)s')



def add(x, y):
    return x + y


def subtract(x, y):
    return x - y


def div(x, y):
    return x / y


x = 20
y = 10
# print('add: {} + {} = {} '.format(x, y, add(x, y)))
# print('sub: {} - {} = {} '.format(x, y, subtract(x, y)))
# print('div: {} + {} / {} '.format(x, y, div(x, y)))

logging.critical('add: {} + {} = {} '.format(x, y, add(x, y)))
logging.error('sub: {} - {} = {} '.format(x, y, subtract(x, y)))
logging.warning('div: {} + {} / {} '.format(x, y, div(x, y)))
logging.info('add: {} + {} = {} '.format(x, y, add(x, y)))
logging.debug('add: {} + {} = {} '.format(x, y, add(x, y)))

# by default logging is set to warning so only warning and above will log other will be ignored

# changing the lever


logging.info('add: {} + {} = {} '.format(x, y, add(x, y)))

try:
    print(1 / 0)
except Exception as msg:
    logging.exception(msg)
