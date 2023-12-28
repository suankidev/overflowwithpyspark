"""
list and tuples: sequential order collection of data
set: unordered collection of data with no duplicates

"""

#list
courses = ['History','Math','Physics','Computer Science']

print(len(courses))
#total lenght -1 is last index
print(courses[0])
print(courses[len(courses)-1])
print(courses[-1])
print(courses)
#first two values
print(courses[0:2])
print(
    courses[:2],
    courses[2:] ,#until ends
    courses[-3:],
    courses[-4:1],
    courses[-4:-2],sep="\n"
)


#modifying a list
courses.append('Art')
print(courses)

#add to specific index
courses.insert(0,'Geo')
print(courses)

#using extends method if we have multiple items to add
course_2 = ['Trignometory','Algebra']

courses.extend(course_2) #added values
print(courses)

courses.append(course_2)  #append and insert will add list in the last and sepcific index not the value
print(courses)

#removing
courses.remove(course_2)
courses.remove('Art')
print(courses)

#pop will remove the value from the last

courses.reverse()
print(courses)
courses.sort()
print(courses)


#sort on number
nums = [1,23,35,75,8,9,11]

nums.sort()
print(nums)

courses.sort(reverse=True)

#sorted verion of cousres without changing the course variable
sorted_course = sorted(courses,reverse=True)

print(sorted_course)


#min no of nums
print(min(nums))
print(max(nums))
#print(help(list))

#find index of value

print(courses.index('Geo')) #if not present will error oout

print('Art' in courses)

#find 2nd max value in list
l = [45,67,34,8,6,65]

for item in courses:
    print(item)

print("enumrated values with tuple------")
for item in enumerate(courses):
    print(item)

print("===="*10)

for index,course in enumerate(courses,start=1):
    print(index,course)

#join method
course_str = '-'.join(courses)
print(course_str)

new_list = course_str.split('-')

print(new_list)

#==============tuple are immutable , not like list=================

list_1 = ['a','b','c','d']
list_2 = list_1

#proble with list
list_1[0] = 'Art'
print(list_2)


tuple_1 = ['a','b','c','d']
tuple_2 = tuple_1

#we can't change the tuple
print(tuple_2)

#===================sets
#sets: unorder and no duplicate
set_1 = {'a','b','c','d','c'}
set_2 = {'a','e','g','k','c'}
print(set_1)
print('a' in set_1) #sets are optimised for member sets

#check
print(set_1.intersection(set_2))
print(set_1.union(set_2))
print(set_1.difference(set_2))


#========create empty list sets tuples
l = []
l =list()

t =()
t =tuple()

set = set()
set = {} #this is not set , it's dictionory
