import logging
import datetime
import os


def get_logger(log_postfix):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    date = datetime.date.today()
    log_file_name = os.getenv('log_path') + "\\" + log_postfix + "_" + str(date) + ".log"
    print("logging to the file :", log_file_name)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s :  %(message)s')
    file_handler = logging.FileHandler(log_file_name)
    stream_handler = logging.StreamHandler()

    file_handler.setFormatter(formatter)
    # file_handler.setLevel(level=logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
