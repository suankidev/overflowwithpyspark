import logging

log_path = "C:\\Users\\sujee\\pydev\\pyspark_learn_project\\resources\\output"

# emp_log = log_path + '/emp.log'

# logging.basicConfig(filename=emp_log,
#                     level=logging.INFO,
#                     format='%(asctime)s : %(levelname)s : %(name)s :  %(message)s')

'''''Advance logging'''
adv_emp_log = log_path + '/adv_emp.log'

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s :  %(message)s')
file_handler = logging.FileHandler(adv_emp_log)

file_handler.setFormatter(formatter)

logger.setLevel(logging.INFO)
logger.addHandler(file_handler)


'''End of advance logging config'''


class Employee:

    def __init__(self, first, last):
        self.first = first
        self.last = last
        # logging.info('Employee created {} {} '.format(self.first, self.last))
        '''advance loggin'''
        logger.info('Employee created {} {} '.format(self.first, self.last))

    @property
    def full_name(self):
        return "{} {} ".format(self.first, self.last)

    @full_name.setter
    def full_name(self, name):
        first, last = name.split(' ')
        self.first = first
        self.last = last


emp_1 = Employee('Sujeet', 'Kumar')
emp_2 = Employee('Ramesh', 'tim')
emp_3 = Employee('John', 'Doe')
