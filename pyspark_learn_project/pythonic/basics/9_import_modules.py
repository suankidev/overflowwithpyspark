import import_module_9_1 as mm
import sys

#import function itse lit only gives access to find_index,will not have access to test variable
from import_module_9_1 import find_index as fi,test

courses = ['history','Math','Physics','CompSci']

#when we import it run everything from that module
index = mm.find_index(courses,'Math')

print(index)

print("""
python is going to look for modules to
 following location""")
print(sys.path)

#to change the path where it will search
sys.path.append("C:\\Users\\sujee\\Desktop\\mypython_module")

import predefined
mystr = "this is test string"

predefined.search_string(mystr,'test')

#same can be done with environment variables


import random
import math
import datetime
import calendar
import  os
print(random.choice(courses))
print(math.radians(90)) #convert foreignhight to centigrates
print(datetime.date.today())
#print(calendar.isleap("2023"))


print(os.__file__)


import  antigravity