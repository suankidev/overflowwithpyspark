import os
import sys

user = os.getenv('username')
master = 'yarn'
env = 'prod'
base_path = r"C:\Users\sujee\Desktop\spark_input\stack-overflow-developer-survey-2019"

if user == 'sujee':
    master = 'local[2]'
    env = 'test'


