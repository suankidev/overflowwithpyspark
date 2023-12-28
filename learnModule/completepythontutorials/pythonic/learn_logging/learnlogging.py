import logging.config
import logging
import addition


logging.config.fileConfig(fname='C:\\Users\\sujee\\pydev\\pyspark_learn_project\\conf\\logging_to_file.conf')
logger = logging.getLogger(__name__)

def main():
    print("adding two number")
    addition.add(10,5)
    logging.error("addition of two number is done")

if __name__ == '__main__':
    logging.error("log started"+__name__)
    main()
