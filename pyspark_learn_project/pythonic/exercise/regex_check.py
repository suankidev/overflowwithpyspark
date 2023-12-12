# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 15:13:10 2023

@author: sujee
"""

#metacharecter
#%%




import re

target_string = "Emma loves \n Python"

# dot(.) metacharacter to match any character

result = re.search(r'.', target_string)
print(result.group())

# Output 'E'
# .+ to match any string except newline

result = re.search(r'.+', target_string)
print(result.group())

# Output 'Emma loves '


print()
result = re.search(r".+",target_string,re.DOTALL)

print(result.group())

#%%

#caret ( ^ ) to match a pattern at the beginning of each new line

target_string = "Emma is a Python developer and her salary is 5000$ \n Emma also knows ML and AI"

#match any four-letter word at the beginning of the string


import re

res = re.search(r"^\w{4}",target_string)

print(res.group())




#%%

str1 = "Emma is a Python developer \n Emma also knows ML and AI"
# dollar sign($) to match at the end of the string

pattern = re.compile(r"\w{2}$", re.MULTILINE)

print(pattern.search(str1).group())


#%%

target_string = "Numbers are 8,23, 886, 4567, 78453"

"""match all number """
#second pattern 0 or more time
"""zero or more repetitions of the preceding expression. And in this case, the preceding expression is the last \d, not all two of them."""

pattern = re.compile(r"\d\d*")

res = pattern.findall(target_string)

print(res)

"""numbers with a minimum of 1 digit and possibly any integer."""
#%%

"""numbers with a minimum of 1 digit and possibly any integer."""

pattern = re.compile(r"\d\d+")


"""? repeat either 0 or more time previous regex"""


#%%

"""calculate product if lower than 1000 else sum"""

num1 = 40
num2 = 30

if num1*num2 <= 1000:
    print(num1*num2)
else:
    print(num1+num2)


#%%

"""Print the sum of the current number
and the previous number"""

"""Write a program to iterate the first
10 numbers and in each iteration,
print the sum of the current and previous
 number."""
last = 0
for i in range(11):
    sum = last + i
    print("current num {} previouse num {} and sum = {}".format(i, last,sum))
    last = i


#%%
"""Print characters from a string that are present at an
even index number"""

"""For example, str = "pynative"
so you should display ‘p’, ‘n’, ‘t’, ‘v’."""

str = "pynative"

print(str[0::2])





#%%
#remove first n char
import re

def remove_chars(source, n):
    f = re.sub(r"\A.{4}",'',source)
    print(f)

    #or
    print(source[n::])



remove_chars("pynative", 4)


#%%

numbers_x = [10, 20, 30, 40, 10]
numbers_y = [75, 65, 35, 75, 30]

#check if first and last no in the list is same return True else False


if numbers_x[0] == numbers_x[-1]:
    print(True)
else:
    print(False)

#%%
#display no divisible by 5

l = [10, 20, 33, 46, 55]

for i in l:
    if i%5 == 0:
        print(i)



#%%
str_x = "Emma is good developer. Emma is a writer"

#return the count of Emma in string


res = re.findall(r"emma", str_x,re.I)

print(len(res))

rs = str_x.count('Emma')

print(rs)

#%%

"""
1
2 2
3 3 3
4 4 4 4
5 5 5 5 5
"""

for i in range(1,6):
    for j in range(i):
        print(i, end=" ")
    print()


#%%

"""Given a two list of numbers, write a program to
create a new list such that the new list should contain odd
numbers from the first list and even numbers from the second list."""



list1 = [10, 20, 25, 30, 35]
list2 = [40, 45, 60, 75, 90]

#result list: [25, 35, 40, 60, 90]


print(list1[2::2] + list2[0::2])


#%%
l = 7536



while l > 0:
    rs = l%10;
    print(rs)
    l =  l//10


#%%

income = 45000
t1 = 0/100
t2 = 10/100
t3 = 20/100

print(t1, t2, income)

if income > 10000:
    income = income - 10000
    t1_tax = 10000
else:
    t1_tax = 0

if income > 20000:
    income = income - 20000
    t2_tax = 20000
    t2_tax = 0










#%%


#print pahada


for r in range(1,11):
    n = r
    c = 1
    while c<=10:
        print(c*n, end = " ")
        c += 1
    print()




#%%

for row in range(0,6):
    for col in range(row):
        print("*", end=" ")
    print()
i= 6
while i >= 1:
    j = i
    while j >= 1:
        print("*",end=" ")
        j -= 1
    i -= 1
    print()


print("====")

for row in range(6,0,-1):
    for col in range(row,0,-1):
        print("*", end=" ")
    print()

#%%



def fun(*n):
    print(*n)


fun(12)


fun(12,13,14)




#%%

def calculation(a, b):

    return a+b, a-b

res = calculation(40, 10)
print(res)


#%%



def showEmployee(name,sal=0):

    print(name , sal)
showEmployee("Ben", 12000)
showEmployee("Jessa")


#%%

def outer(a,b):
    def inner(a,b):
        return a+b

    res = inner(a, b)
    return res+5


print(outer(10,15))

#%%
#recursive function to calculate sume from 0 to 10


sum = 0
def fun(num,acc=0):
    global sum
    if num == 0:
        sum = acc
    else:
        fun(num-1,acc=acc+num)
    return sum

print(fun(5555555555555))

#%%

def add(num):
    if num:
        return num + add(num-1)
    else:
        return 0

import sys
sys.setrecursionlimit(555555555)

print(add(5555555555555)) #RecursionError: maximum recursion depth exceeded


#%%

def add(num):
    if num:
        res = num + add(num-1)
        yield res
    else:
        return 0


f = add

for i in f(5):
    print(i)


#%%


def fact(num):

    fact = 1
    while num >= 1:
        fact = fact*num
        num -= 1

    return fact

print(fact(23))


#%%

a,b = 0,1
while b<=15:
    print(a, end=" ")
    a,b = b,a+b

#%%

def display_student(name, age):
    print(name, age)


d = display_student

d('a',11)


b = display_student('b',12)

#%%


def gen_list(num):
    return [i for i in range(num+1) if i%2 == 0]

print(gen_list(10))



x = [4, 6, 8, 24, 12, 2]


print(sorted(x)[int(len(x)-2)])


#%%


list1 = [54, 44, 27, 79, 91, 41]

list1.pop(0)  #removving


print(list1)

list1.insert(0,20)

print(list1)

list1.remove(44)

print(list1)
