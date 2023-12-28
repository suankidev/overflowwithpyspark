# -*- coding: utf-8 -*-
"""
Created on Mon May 31 16:40:05 2021

@author: sujee
"""
# %%


# raw format
print(r'C:/users/sujee/Desktop')

a, b = 0, 1

while a < 10:
    print(a)
    a, b = b, a + b

# %%

x = [1, 2, 3, 4]

print(type(x))

b = bytes(x)

print(type(b))

print(b[0])

# %%
"""
remove duplicate char sequence from str
"""

"""
1. create new empty string
2. traverse through main string
     1. traverse through new string upto the lenght of new string
     2. compare each char from main string to new string
     3. if it's present then break the inner loop
     4.special else for the inner for loop if it's seccussfully executed(mean
           there is no match in new string)
5.go to 2 and repeate.

"""
# keyword
import abc
import keyword
from typing import Text, final

print(keyword.kwlist)

s = 'mississippijfodaiejr aihe pe g;e e;gaeghdfkdfd'
s1 = ''

if s1 == '':
    print(s)

for i in s:
    if s1 == '':
        s1 = s1 + str(i)
    else:
        for j in s1:
            # print(i,j,sep="==")
            if (j == i):
                break

        else:
            s1 = s1 + str(i)

print()
print(s1)

# %%

"""
Q1. program to display all position of substring in the given main string:


1. while true loop
2.  pos=s.find(substr)
    """

s = "my name is not khan and i am not terrorist as it levelled for every khan"
sub = "khan"
flag = False
pos = -1
count = 0

print(s.find("khan", 1, len(s)))

while True:
    pos = s.find(sub, pos + 1, len(s))
    if pos == -1:
        break
    print("Found at {} ".format(pos), end="")
    print("substr is {}".format(sub))
    count = count + 1
    flag = True

    if flag == False:
        print("Substring not found")

print("Total occurrance of the substring is ", count)


#eith regex

import re
p = re.compile('khan')

print(len(re.findall(p,s)))

# %%
"""

"""
s = "my name is not khan and never was khan"
s.count("khan")
s.count("khan", 16, len(s))
# replace string with other strin

print(s.replace("khan", "gangadhar"))
# replace is not replacing old beside it's creating another string object

print(s)

# str has no append attribute s.append("shaktiman")

# string can be split to produce a list

print(s.split())

dt = '02-06-2021'
# split(' ',2)

for x in dt.split('-'):
    print(x)

mystr = ':'.join(s.split())

# or

print(mystr)

# every word is being capitalized
print('Sujeet kumar singh'.title())

# %%

name = "sujeet"
salary = 34598
age = 30

print("name is {} salary is {} and age is {}".format(name, salary, age))

print("name is {x} salary is {y} and age is {z}".format(z=age, x=name, y=salary))

# %%
"""
write a program to reverse a string
"""

s = 'durga software foundation'

s = s.title()

print(s)

print(s[::-1])

print(reversed(s))

print(''.join(reversed(s)))
for i in reversed(s):
    print(i, end="")

print("\n==")

for i in s.split()[::-1]:
    print(i)

# %%

"""
input=B4A1D3
output=BAD413
"""
value = 'B4A1D3'
outstring = ''
outdigit = ''
for i in value:
    if i.isdigit():
        outdigit = outdigit + str(i)
    if i.isalpha():
        outstring = outstring + str(i)

print("First output is", outstring + outdigit)

# we can do as below

myoutput = ''
for i in sorted(value):
    myoutput = myoutput + i

print("Output is", myoutput)

# %%

"""
Dictionary
"""

d = {"Sujeet": "Gorakhpur", "Rohit": "Solapoor"}

print(d.keys())
print(d.items())
print(d.values())

d["Sujeet"] = "UP"

print(d)

# %%

s = "abccccbbbaaabbbbbaaaaccc"

d = {}
for i in s:
    if i not in d.keys():
        d[i] = 1
    else:
        d[i] = d[i] + 1

print(d)
# %%
"""
write a program to eliminate duplicate form list

"""

mylist = [x for x in range(1, 10)] + [y for y in range(5)]

print(mylist)

print(set(mylist))

# %%


"""
predefined function / built in function
user defined functions
"""


# %%

def wish():
    """ I want to wish a person"""
    print("Good morning")


wish()


# %%
def wish(name):
    """ I want to wish a person"""
    print("Good morning {}".format(name))


wish("Sujeet")


# %%
def sum(a, b):
    return (a + b, b)


print(type(sum(12, 38)))


# %%
def f():
    """default value return by a function """
    print("hello")


f()

print(f())  # none by default

# %%

"""
4=4*3
36=12*3
72=36*2
72=72*1

1. fact=fact*fact-1
2. fact=fact*fact-1


"""

def factorial(num):
    """to find factorial of a number"""
    fact = 1
    while num >= 1:
        fact = fact * num
        num = num - 1
    print(fact)


factorial(6)

# %%
"""a function can return any no of values"""


def myFun():
    return 1, 2, 3, 4


a, b, c, d = myFun()

print(a, b, c, d, sep=':')

# by default function return a type of tupple

# %%

"""
Type of argument

def f1(a,b)       --a and b are formal parameter/argumnet
     body


f1(10,20)      ----actual argument

1. postitional argument
2. keyword arguments
3. default arguments
4. variable length argumnets
"""


def postitionalArgument(a, b):
    print(a - b)


# argument order, no of argumnet should match
postitionalArgument(10, 90)


def kywordArgument(name, msg):
    print(name, msg)


kywordArgument(msg="welcome to Goa ! ", name="Singham")


def defaultArgument(name="Guest"):
    print('hello {} welcome'.format(name))


defaultArgument()


def sum(num):
    rslt = 0
    for i in num:
        rslt = rslt + i
    print("sum of value is", rslt)


def variablLengthArgument(*n):
    sum(n)

def var_args(*n):
    print(n, len(n))

variablLengthArgument(10)
variablLengthArgument(10, 20)
variablLengthArgument(10, 20, 30)

# var_args(1, 2, 3, 'tst00') #(1, 2, 3, 'tst00')
var_args((4, 5, 6), ('a', 'b', 'c')) #((4, 5, 6), ('a', 'b', 'c'))
t = (2, 3, 4, 5, 6)
var_args(t)  # ((2, 3, 4, 5, 6),)
var_args(*t)  # (2, 3, 4, 5, 6)

def variablLengthArgument1(name, *n):
    print(name, n)


variablLengthArgument1('rajoo', 10, 20, 30)


# %%

def display(**kwargs):
    print(type(kwargs))
    print(kwargs)


display(name="Sujeet", marks=100, GF='Ankita')

display(**{'name': "Sujeet", 'marks': 100, 'GF': 'Ankita'})

d = {'name': "Sujeet", 'marks': 100, 'GF': 'Ankita'}

display(**d)

a = ("ram", "shyam", "dhyan")

print("{0} {1} {2}".format(*a))

def var_args_key(**k):
    print(type(k))
    print(k)


s = (1, 2, 3, 4, 5)
s1 = 1, 2, 3, 4, 5
s3 = {'one': 1, 'two': 2}

# var_args_key(s) #TypeError: var_args_key() takes 0 positional arguments but 1 was given
# var_args_key(s1) #TypeError: var_args_key() takes 0 positional arguments but 1 was given
# var_args_key(s3) #TypeError: var_args_key() takes 0 positional arguments but 1 was given
var_args_key(**s3)
#print(**s3) #'one' is an invalid keyword argument for print()
var_args_key(name='sujeet', age=29)
# %%
"""
A file of function and statement >> module >> package >>> libraray

"""

s = [int(x) for x in input("Enter some number with space::").split()]

print(type(s))

print(s)

sum_ = eval(input(("Enter some expression like 1+3-4::")))

print(sum_)

# %%

"""
['hi','hello',1,2,4]
"""

x = eval(input("Enter list"))

print(x)

# %%

a, b, c = [eval(i) for i in input("Enter three number with comma->").split(",")]

print(a, b, c, sep="\t")

# %%
"""command line argument
"""

import functools
import importlib
from sys import argv
import sys

print(type(argv))

print(argv)

print(sys.argv[0])

# %%

# all fundamental data type byte,int,float ,str,bool ,complex are immutable in python

x = [1, 2, 3, 4, 5, 6, 7]

# we have to convert into bytes
print(type(bytes(x)))

# byte array is mutable
print(type(bytearray(x)))

for i in range(4):
    pass

"""
list--insertion preserverd, duplication allowed, mutable + + +  idm
         [ ]
tuple--insertion preserved, duplication allowed, immutable + + - id-m
          ()

set---insertion not preserved, duplication not allowed, mutable - - +
          indexing and slicing not in set b/c insertion order not preserved
          we can't
          empty set   s=set()
          {}

forzen set--insertion not preserved, duplication not allowed, immutable - - -

dict--opposit to list duplicate key not allowed


"""

print(type((1, 2, 3,)))


""" LIst
Method	Description
append()	Adds an element at the end of the list
clear()	Removes all the elements from the list
copy()	Returns a copy of the list
count()	Returns the number of elements with the specified value
extend()	Add the elements of a list (or any iterable), to the end of the current list
index()	Returns the index of the first element with the specified value
insert()	Adds an element at the specified position
pop()	Removes the element at the specified position
remove()	Removes the first item with the specified value
reverse()	Reverses the order of the list
sort()	Sorts the list
"""

l = [1,2,3,4]

print(l.sort(), l)
print(sorted(l, reverse=False))

""" Dictionary

Method	Description
clear()	Removes all the elements from the dictionary
copy()	Returns a copy of the dictionary
fromkeys()	Returns a dictionary with the specified keys and value
get()	Returns the value of the specified key
items()	Returns a list containing a tuple for each key value pair
keys()	Returns a list containing the dictionary's keys
pop()	Removes the element with the specified key
popitem()	Removes the last inserted key-value pair
setdefault()	Returns the value of the specified key. If the key does not exist: insert the key, with the specified value
update()	Updates the dictionary with the specified key-value pairs
values()	Returns a list of all the values in the dictionary

"""

"""tuple
Method	Description
count()	Returns the number of times a specified value occurs in a tuple
index()	Searches the tuple for a specified value and returns the position of where it was found

"""

"""set

Method	Description
add()	Adds an element to the set
clear()	Removes all the elements from the set
copy()	Returns a copy of the set
difference()	Returns a set containing the difference between two or more sets
difference_update()	Removes the items in this set that are also included in another, specified set
discard()	Remove the specified item
intersection()	Returns a set, that is the intersection of two or more sets
intersection_update()	Removes the items in this set that are not present in other, specified set(s)
isdisjoint()	Returns whether two sets have a intersection or not
issubset()	Returns whether another set contains this set or not
issuperset()	Returns whether this set contains another set or not
pop()	Removes an element from the set
remove()	Removes the specified element
symmetric_difference()	Returns a set with the symmetric differences of two sets
symmetric_difference_update()	inserts the symmetric differences from this set and another
union()	Return a set containing the union of sets
update()	Update the set with another set, or any other iterable
"""

t =  {1,2,3,4}
t1 = {1,5,6,7}

print(t.difference(t1))  #{2, 3, 4}
print(t.symmetric_difference(t1)) #{2, 3, 4, 5, 6, 7}

# %%

"""

x and y

return x if x is false otherwise y

x or y

return x if x is true otherwise y



x=int(input("enter ::"))

print(x)


s={1,2,3,2,2,4,1,5,4}

print(s)
"""

a = 10
b = 20

c = 30

x = a if a > b else b if b > c else c

print(c)

# %%

"""

special operator
1. identity     (for address comparison purpose) in and not in
2. membership operator  is, is not

"""

list1 = [1, 2, 3, 4, 5]
list2 = [1, 2, 3, 4,5]

print(1 in list1)

print(list1 is list2)

print(list1 == list2)

# %%


# print formatter


a, b, c = 1, 2, 3

print(isinstance(a, int))
print(a, b, c, sep="---")
print(a, b, c, end="---")

print()

print("value of a is %s and b is %s" % (a, b))

print("value of a is sss {:5d}".format(a))

# replacement operator

print("the value of a is {0} and b is {1}".format(a, b))

print("the value of a is {} and b is {}".format(a, b))

# %%

"""
for-else
while-else
try-except-else-finally


else executed only when loop is successfully executed(break should not executed)

"""
num = 5
while num < 10:
    if num % 2 == 0:
        print("number {} is even".format(num))
        # break
    else:
        num = num + 1

else:
    print("Done")

# %%
"""
slice operator

step:
    + then move farword begin to end-1
    - then move backward begin to end+1


    """

s = '0123456789'

temp = s[::-1]

print(temp)

# %%

""" String related method for validation
lstrip()     remove space from left
rstrip()  remove space from right
strip() remove space from left and right

"""

cityList = ["Hyderabad", "Bangalore", "Kolkata"]

user_input = input("Enter the city name")

# %%

""" To check given substring present or not
find()           //return -1 if not found
index()      //raise exception if string or char not found

these will start seach from last

rfind()   - - reverse find
rindex()   -- reverse index

find(substring,bigin,end)
sorted(string_value)
"""

s = "sujeet kumar singh kumar"

print("u" in s)

# is and is not for refrence comparison
# == is for content comparison

print(s.find("kumar"))

print(s.index("k"))

print(s.find("sinno"))  # retrun -1

print(s.find('a', 3, 15))

s.index('tango')

# %%

"""
list

if we want to represent a group of individual object where (below point)
1. insertion order is preserved
2. duplicate objects are allowed
3. heterogeneous objects are allowed
4. growable (mutable)
5. by using  [   ] notation
6. index

l=[]

if we know elements alreaady

l=[10,20,30,40]

with dynamic input

l=eval(input("Enter list" ))
      >>>[10,20,30,40]
split()
s='learnign python is very easy'
l=s.split()

for a given sequence
l=list(sequence)
len(list)
count(10)   //no of accurance available in string
index(10)   //if it's not there then we will get error

l.append(elsement) ------append only applicable to list
l.insert(index,element)


l1.extend(l2) --will add l2 to l1,,l2 may be list,set,tuple,string and extend wont return anything

l.remove(10)


l.pop() --remove last element and return that element
l.pop(index)


l.reverse()
l.sort()   --ascending sorted
l.sort(reverse=True)


operator:

a=[1,2,3,4,5]
b=[56,6,666,7,8,9]

print(a+b)    //combine output

print(a*3) //repeat the same list


comparision:
x=['Dog','Cat','Ran' ]


x=[1,2,3,4,5,6]

y=x       //shallo copy

z=y
z[2]=345

x and y and z all are same

---------------

we can create diffrent object --deep copy

1. slice operator
            y=x[2:]

2. copy() funcion
         y=x.copy()
"""
x = [1, 2, 3, 4, 5, 6]

y = x

print(y is x)

# y[1]=100

print(y is x)

print(y)
print(x)

z = x.copy()
print(z is x)

z[1] = 3974937

print(z)
print(x * 10)

# %%
"""
comparison operator in list
x==y
1. the number of elemnets must be equal
2. the order should be same
3. the contain should be same( including case)

>,<,=>,<=

all relation operator only going to check for 1st elemnt of list only
"""

x = ['Dog', 'Cat', 'Rat']
y = ['Dog', 'Cat', 'Rat']
z = ['DOG', 'CAT', 'RAT']

print(x == y)
print(x is y)  # object are different is operator will check refrence comparison
print(x[0] is y[0])
print(x == z)

print(x != z)

print('Dog' in x)
print('Dog' not in x)

x.clear()
print(x)

# %%

"""
nested lists:
a list inside another list
list comprehension

list=[expression for x in sequence]
list=[expression for x in sequence if condition ]

"""

a = [1, 2, 3, 4, 5, [56, 6, 666, 7, 8, 9]]

b = [[56, 6, 666, 7, 8, 9], [56, 6, 666, 7, 8, 9], [56, 6, 666, 7, 8, 9]]

l = [x for x in range(100)]
m = [x for x in range(100) if x % 2 == 0]
print(l)

j = [x for x in m if x > 10 and x < 20]
print(j)

mylist = ["apple", "banana", "cherry"]

j = [i[0] for i in mylist]

print(j)

print(''.join([i[0] for i in mylist if 'e' in i]))

# %%
"""


slice operator

step:
    + then move farword begin to end-1
    - then move backward begin to end+1

       in the forward direction if end value is 0  then result is empty
       in the bakcward direction if end value is -1  then result is empty

    """

vowels = ['a', 'e', 'i', 'o', 'u']

words = "my name is sujeet and want to be a python developer"

for i in vowels:
    for j in words.split():
        if j.find(i) != -1:
            print(j, end=" ")

print(words.find('a'))

# %%
"""
important method of settings
"""

s = set()

print(type(s))

s.add('a')

# x may be list or tuple or set
l = [1, 2, 3, 4]
x = [1, 2, 9, 5]
y = [1, 2, 3, 4]
z = [1, 2, 3, 6]

s.update(l)

print(s)

s.update(x, y, z)
s.update(range(10, 20))
print(s)

# cloning deep(diffrent object will be created)
s1 = s.copy()

# remove random element form set pop()

# remove element remove(x)

"""
discard()
clear()
remove()


mathemetical operation:::

1. union()

s1.union(s2)

2. s1.intersection(s2)
      common elements

3. s1.difference(s2)

4. s1.symmetric_difference(s2)
      (S1 ^ S2)  excluding common object

5.


"""

# comprehenstion is valid for set

s3 = {x for x in range(60)}

print(s3)

# %%

"""
same as list but it's immutable
read version of list is tuple
tuple(list/squence)  anything is convertible
"""
mytuple = 10, 20, 30, 40

# or

# mytuple=(10,20,30,40)
a = (10)
print(type(a))

print(type(mytuple))

##how to represent single value

atuple = (10,)

print(type(atuple))

mylist = [10, 20, 30, 40]

mytuple = tuple(mylist)

print(type(mytuple))
# %%

"""
method

len
count(x)

index(20)
sorted(t)  #is always return  list

so tuple(sorted(x))

min(t)
max(t)

"""

t1 = (10, 20, 30, 40)

t2 = (11, 2, 3, 400)

print(t1 > t2)
# %%
"""
tuple packing and tuple unpacking

packing  mean == Gruoping into single untit
a=10
b=20
c=30
d=40

t=a,b,c,d

"""

a = 10
b = 20
c = 30
d = 40

t = a, b, c, d
print(t)

# unpacking

p, q, r, s = t

print(p)

# %%
"""
tuple comprehension:
        is not supported
"""

t = (i for i in range(10))

print(type(t))

# it's not tuple object it is generator object
for i in t:
    print(i)

# %%

"""
type of variable in python

1. Global variable
           outside of the function and available to all function in that module

2. local variable


3. nonlocal


"""

a = 10


def f2():
    a = 777
    print(a)


def f1():
    print("f1:", a)


f2()

f1()
# %%
"""
global keyword: to make available a to function
                      we can declare global vairble inside function

"""


def f1():
    global a
    a = 10
    print(a)


def f2():
    print(a)


f1()
f2()

# %%
spam = "ABC"


def scope_test():
    def do_local():
        spam = "local spam"

    def do_nonlocal():
        nonlocal spam
        spam = "nonlocal spam"

    def do_global():
        global spam
        spam = "global spam"

    spam = "test spam"

    do_local()
    print("After local assignment:", spam)
    do_nonlocal()
    print("After nonlocal assignment:", spam)
    do_global()
    print("After global assignment:", spam)


scope_test()
print("In global scope:", spam)

# %%
a = 1


def f1():
    global a
    a = 10
    global b
    b = 9999
    print(a)


def f2():
    print(globals()['b'])
    print(a)


f1()
f2()

print(globals())

# %%
"""
recursive function
"""


def facto(num):
    if num == 0:
        rslt = 1
    else:
        rslt = num * facto(num - 1)
    return rslt


print(facto(2))
print(facto(111))

# %%
"""
anonymous function
i=lambda input:expression
i=lambda input1,input2:input1*input2

"""
a = lambda n: n * n

print(a(10))

b = lambda a, b: print(a + b)

b(12, 13)

c = lambda a, b: a if a > b else b

print("bigger number is", c(10, 20))

# %%

"""
filter()
map()
reduce()
"""


def isEven(num):
    if num % 2 == 0:
        return True
    else:
        return False


l = [x for x in range(20)]
print(l)
print(list(filter(isEven, l)))

print(list(filter(lambda x: x % 2 != 0, l)))

# %%

"""
to perform some business logic we should go
fo map()

filter() return True and false


The filter() method filters the given sequence with
the help of a function that tests each element in the
sequence to be true or not.




filter(function, sequence)
Parameters:
function: function that tests if each element of a
sequence true or not.
sequence: sequence which needs to be filtered, it can
be sets, lists, tuples, or containers of any iterators.
Returns:
returns an iterator that is already filtered.


"""


def double(x):
    return x * x


l = [x for x in range(2, 20)]
print(list(map(double, l)))

print(list(map(lambda a: a * a, l)))

# %%

"""
reduce()

reduces into single elemnet """

from functools import reduce

l = [x for x in range(2, 20)]

print(l)

# print(list(reduce(lambda x,y:x+y,l)))

lis = [1, 3, 5, 6, 2, ]
# it doesn't produce list it produce only a reduce value as one
print(reduce(lambda x, y: x + y, l))

# %%
"""

in pthon everythin is a object
function aliasing
"""


def f1():
    pass


print(type(f1))
print(type(f1))

funalias = f1

print(id(funalias))
print(id(f1))
del f1

print(funalias())
"""
we can not use wiht f1
print(f1())
"""

# %%
"""
function inside function

"""


def outer():
    print('outer function started')

    def inner():
        print('inner function exxecution')

    inner()


outer()

# %%
"""
f=outer() --function call
f1=outer ---holding a refrence aliasing
"""


def outer():
    print('outer function started')

    def inner():
        print('inner function execution')

    return inner


mycall = outer()

print('call to inner function:::', (mycall()))

# %%
"""
function Decorators:

decorators help to make our code shorter and more
pythonic

without modifying existing function we are able to provide
extra functionality

it take function as argument and provide some extra functionality

"""


def suankideor(func):
    def inner(name):
        if name == 'sunny':
            print("meet me in khopcha")
        else:
            func(name)

    return inner


@suankideor
def wish(name):
    print('hello {} good evening'.format(name))


wish('sujeet')
wish('sunny')


# we have wish method (an existing functionality) now
# we want if name is sunny we want some other message


# %%

def suankideor(func):
    def inner(name):
        if name == 'sunny':
            print("meet me in khopcha")
        else:
            func(name)

    return inner

def wish(name):
    print('hello {} good evening'.format(name))


decorFunction = suankideor(wish)

decorFunction('sujeet')
decorFunction('sunny')


# %%
def smarDivision(func):
    def inner(a, b):
        if b == 0:
            print('Hello stupid .. how we can divide with zero')
        else:
            return func(a, b)

    return inner


@smarDivision
def div(a, b):
    return a / b


print(div(10, 2))
print(div(10, 5))
div(10, 0)



d = smarDivision(div)

d(20,0)
# %%

def faceMakup(func):
    def inner(name):
        print("{} face makeup done".format(name))
        func(name)

    return inner


def bleaching(func):
    def inner(name):
        print("{} bleaching done".format(name))
        func(name)

    return inner


# decorator executes top to bottom
@bleaching
@faceMakup
def makeup(name):
    print('{} makeup has been completed'.format(name))


makeup('surendra')

"""

"""

# %%

"""
l=[x*x for x in range(10000000000)]
here will be stored in memory

"""
# generator concept
"""
in case of generator value is not going to store in memory
for large amount of data we should use generator concept
"""
l = (x * x for x in range(10000000000))
#
# for i in l:
#    print(i)


print(type(l))
# %%


"""generator is functions which generate

    yield

    Yield is a keyword in Python that is used to return from a
    function without destroying the states of its local variable and
    when the function is called, the execution starts from the last yield statement.

"""


def mygen():
    yield "hi first value"
    yield "hi second value"
    yield "hi third value"


g = mygen()

print(type(g))

# print(next(g))
# print(next(g))

# print(next(g))
for x in g:
    print("====", x)


# %%

def countdown(num):
    print("Sart of countdown !")
    while (num > 0):
        yield num
        num = num - 1


values = countdown(900)

for i in values:
    print(i)

# %%
"""
fiboncacchi number

10

1. temp=0     rslt=0
2. temp=1   rslt=rslt+temp
3. temp=2   rslt=rslt+temp


"""


def finaccinumber():
    a, b = 0, 1
    while True:
        yield a
        a, b = b, a + b


get = finaccinumber()

for i in get:
    if i > 100:
        break
    print(i)

# %%
"""
import module1 as md

aliasing module
md.add(10,20)

import module1

from module1 import x,add
from module1 import x as a,add as sum
from module1 import *

module1.add(10,20)
"""

from module1 import add, prod

add(20, 30)

# reloading module

prod(10, 20)

# %%
import time
from importlib import reload
import module1

print("first line of in reloaded import")
module1.add(10, 20)

time.sleep(10)

reload(module1)
print("after sleep")

module1.prod(10, 20)

# %%
# to list out member used by this program
# import module1
# print(dir())

print(dir(module1))
##print(globals())

"""
usage of default of particular modue

#to list out member used by this program
import module1
#print(dir())
print(dir(module1))
##print(globals())
['__builtins__', '__cached__', '__doc__', '__file__', '__loader__', '__name__', '__package__', '__spec__', 'add', 'prod', 'x', 'y']
"""
l = ['__builtins__', '__cached__', '__doc__', '__file__', '__loader__', '__name__', '__package__', '__spec__', 'add',
     'prod', 'x', 'y']

for i in l:
    print(" value of {} is {}".format(i, i))

# %%
"""
__name__ value become __main__ if the python program called directly
otherwise 'module1' will be the value
"""
# import module1
from imp import reload

reload(module1)

print(__name__)

module1.f1()

# %%

"""
sqrt()
ceil()
floor()
sin()
tan()
pow()
p
random()     0-1 not inclusive

"""
import math
import random

for i in range(10):
    print('%0.0f' % (random.random() * 1000))

print("random is inclusive")
print(random.randint(1, 100))
print("OTP IS", random.randint(1000, 9999))
print("uniform is not inclusive")
print(random.uniform(1, 100))
print("randrange function")
print()

print(random.randrange(1, 100))
print(random.randrange(1, 11, 2))  # 1,3,5,7,9
print(random.randrange(1, 100, 2))

print()
print("choice is use generate random from list ")

l = [x for x in range(1, 10)]

for i in range(len(l)):
    print(random.choice(l))

# %%
"""
otp genration
chr(randint(65,65+25)) ---[A-Z]
randint(0,9)

"""
import random

print("=" * 10, end=">")
print("Program to generate OTP")
print("OTP IS %0.0d" % (random.random() * 1000000))

print("=" * 10, end=">")
print("""Program to generate random password of 6 length where 1,3,5 should be alphabate symbol""")

print(chr(random.randint(65, 65 + 25)), random.randint(0, 9), chr(random.randint(65, 65 + 25)), \
      chr(random.randint(65, 65 + 25)), random.randint(0, 9), chr(random.randint(65, 65 + 25)), sep='')

# %%
"""package
group of related module into single unit

3. special file __init__.py should be there in directory
      but only applicable py-3.3

4. naming convenion
    unique name convention(internet domain name)
    com.suanki.dep


DEP functionality:


LOAN functionality:


REM functionality:


import dep_pack.checkbalance
"""
from com.suanki.dep import checkbal

checkbal()

# %%

"""
excetion handling in python
1. syntax error
2. Runtime error


in exception handling only runtime error is going to handle


e.g.
ZeroDivisionError
TypeError
ValueError
FileNotFoundError
EOFError




try:
    read file from londan
excetp FileNotFoundError:
      use local file and continue rest of program



====Default exception handling:

terminated..


--python exception hierarchy:

                        BaseException
                            ||
----------------------------------------------------------
||                ||              ||                     ||
Exception     SystemExit   GeneratorExit             KeyBoradInterrupt
||
AttributeError
ArithmaticError---->>ZeroDivisionError,FloatingPointError,OverflowError
EOFError
NameError
LookupError---->>IndexError,KeyError
OSError-->>FileNotFoundError,InterruptedError, PermissionError,TimeOutError
TypeError
ValueError



1. every exception in python is class
2.customized exception handling



"""
# %%

print("stmt-1")

try:
    print(10 / 0)
except ZeroDivisionError:
    print(10 / 2)

# %%
"""
multiple except block
"""
try:
    x = int(input('Enter 1st number'))
    y = int(input('Enter 2nd number'))
    print(x / y)
except ZeroDivisionError:
    print("Can't devide with zero")
except ValueError:
    print('provide valid number')

# %%

try:
    x = int(input('Enter 1st number'))
    y = int(input('Enter 2nd number'))
    print(x / y)
except(ZeroDivisionError, ValueError):
    print("done with exxception")

# %%
try:
    x = int(input('Enter 1st number'))
    y = int(input('Enter 2nd number'))
    print(x / y)
except (ZeroDivisionError, ValueError) as msg:
    print("done with exxception", msg)
# %%
"""
default execption block
must be last

"""
try:
    x = int(input('Enter 1st number'))
    y = int(input('Enter 2nd number'))
    print(x / y)

except(ZeroDivisionError, ValueError):
    print("done with exxception")
except:
    print("other exception in default exception!")
# %%
"""
os._exit(0)

from os module exit is there for immediate exit from
code
"""
import os

try:
    print(10 / 1)
    # os._exit(0)
    """

    status code 0 represent normal termination
    nonzero is abnormal termination
    """

except ZeroDivisionError:
    print("Excetption occured")
else:
    print("hello suceessfully executed without exception")
finally:
    print("Cleanup codes go here....")
    print("closing the resources!")

# %%
"""
1. try block compulsory we should write either except or finally block
2. except without try is invalid
3. finally without try is invalid
4. we can take multiplle except block for
th same try but we can't take multiple else or finnaly block
5. else wihtout except is invalid
6. try-except-else-finally --order is important
7.

"""
try:
    print('hi')
finally:
    print('final')

try:
    print(10 / 1)
except:
    print("error")
else:
    print('no error')
# %%


"""
user defined exception
like..
InSufficientFundsExcepton
"""


class ToYoungException(Exception):
    # constructor is __init+
    def __init__(self, args):
        self.msg = args


class ToOldException(Exception):
    # constructor is __init+
    def __init__(self, args):
        self.msg = args



age = int(input('enter age:'))

if age > 60:
    raise ToOldException('Age is too old')

elif age < 18:
    raise ToYoungException('age should be minimum 18 year')

else:
    print('thanks for registration!')

# %%
"""
Logging:

 1. using log files we can debugging
 2. like number of request


import logging


logging levels:(6 levels)

1. CRITICAL  e.g. something like number above 50
2. ERROR   ==40
3. WARNING  ==30
4. INFO  ==20
5. DEBUG  ==10
6. NOTSET   ==0


CEWIDN -- short name




by default only 1-3 levels are going to show.


How to implement logging

1. name of the file
2. level message
 basicConfig() of logging module

logging.basicconfig(filename='log.txt',level=logging.WARNING)


write a messages:

logging.debug(message)
logging.info(message)
logging.warning(message)
logging.critical(message)
logging.error(message)

"""

# %%

import logging

logging.basicConfig(filename='log.txt', level=logging.WARNING)
# warning or higher level will be logged into file only
# for every thing logging we have to set level=logging.NOTSET


print('logging into demo')

logging.debug('debugging info')
logging.critical('cretical err')

# logging exception into file

logging.info('A new request came')
try:
    print(10 / 0)
except Exception as msg:
    print("exception occured", msg)
    logging.exception(msg)
except ValueError as msg:
    print('Enter only int value')
    logging.exception(msg)

logging.info("Request processing completed")

# %%
"""
debugging:

bug or defect when we get variation in expected result than actual result
defect/bug:-- process of identifying and fixing the bug is debugging(responcebility of developer)


In java println we use to debug

two way:

assert conditional_expression

assert conditional_expression,message
"""


def sequare(num):
    return num * num


# assert statements
x = 10

if (x / 2 == 5):
    # balance should be deducted
    x = x - 2

print(x)
assert x == 8

print(x)

# statement 2000 line code and x should be 5

assert x == 5, 'it\'s not as expected x should be 5'
print(x)

# if test sucessess then assert will be ignored by compiler


assert sequare(2) == 4, 'not expected'

"""
assert is helping during the development phase.

exception is alternative provider something went wrong


"""

# %%

"""
File handling.
--------------

1. text files
      text

2. binary files
      img,video

Opening a file.
--------------


f=open(filename,mode)

 allowed mode:
------------
r--default mode
w--overwrite, no file then create new file
a--append
r+  read and write , previous date not deleted, point to first line only
w+  write and read, overwrite existing data
a+  append and then read
x   open a file in exclusive creation mode(write the data and file should
 not be there) for write operation
       otherwise FileExistsError(if file alreay there)

for binary file mode append b:
---------------------------
    rb,wb,ab,r+b,w+b,a+b,ab


closin file:
-----------
f.close()

         TooManyOpenFile exception

various properies of the file object:
------------------------------------

f.name
f.mode
f.closed
f.readable()
r.writable()


"""
# %%
"""

writing data to file
---------------------

f.write(str)

f.writelines(list of lines)

"""

f = open('C:/Users/sujee/Desktop/file1.txt', 'w')

print(f.name)
print(f.readable())
print(f.closed)

f.write('durga\n')

list = ['sunny\t', 'buny\t', 'chinni\t']

f.writelines(list)

f.close()
print(f.closed)

# %%


"""
reading from file

read()  to read total data from file
rand(n)  read n character from file
readline()  line only one line it's like yield
radlines()    to read all lines into a list

"""
f = open('C:/Users/sujee/Desktop/file1.txt', 'r')

"""
data=f.read()

print(type(data))

for i in data:
    print(i,end='')

"""
"""
data1=f.read(10)
print(data1)

"""
"""
data2=f.readline()

print(data2,end='')
print(f.readline(),end='')
"""

l = f.readlines()

print(l)

f.close()

# %%

"""

with statement:
-----------

close automatically

"""

# f=open('C:/Users/sujee/Desktop/file1.txt','r')

with open('C:/Users/sujee/Desktop/file1.txt', 'a') as f:
    f.write('Sujeet kumar singh\n')

# file already closed
assert f.closed == True, 'File not close'

f1 = open('C:/Users/sujee/Desktop/file1.txt', 'r+')

"""
tell() -- tell the positon by index

seek() --- seek to particular location forward

seek(offset,fromwhichlocation) ---valid only py-2 not in py-3

offset-how many no of position to jump

fromwhere--
-----
0--begning of file
1=from current positon


"""

def sum(a,b):
    return a+b

import unittest
class Test(unittest.TestCase):

    def test1(self):
        self.



print(f1.read(5))
print(f1.tell())

f1.seek(116)
print(f1.read(5))

# %%
"""
os module
          path submodule
             isfile()

for information about files

os.path.isfile(fname)

"""

# given file exists or not if ther print contain read from keybord


import os, sys

##f1=open('C:/Users/sujee/Desktop/file1.txt','r+')
fname = input('input file name ')
path = 'C:/Users/sujee/Desktop/'
fname = path + fname
if os.path.isfile(fname):
    print('file exists')
    f = open(fname, 'r')
    #    print(f.read())
    linecount = wordcount = charcount = 0
    for line in f:
        linecount = linecount + 1
        charcount = charcount + len(line)
        words = line.split()
        wordcount = wordcount + len(words)
    print('no of lines', linecount)
else:
    sys.exit(0)

# %%


f1 = open('C:/Users/sujee/Desktop/file1.txt', 'r+')

for i in f1:
    print(str(i), end='')

# %%

"""

Handling binary data

"""

f1 = open('p.jpg', 'rb')

f2 = open('newpic.jpg', 'wb')

by = f1.read()

f2.write(by)

# for mp4 is the same as image


# %%
"""
csv module

"""
import csv

with open('emp.csv', 'w', newline='') as f:
    w = csv.writer(f)  # return csv writer object pointing to f
    w.writerow(['eno', 'ename', 'esal', 'eaddr'])
    n = int(input("Enter no of employees"))
    for i in range(n):
        eno = int(input("Enter employee number:"))
        ename = input("enter employee name")
        esal = input("enter employee sal")
        eaddr = input("enter employee addr")
        w.writerow([eno, ename, esal, eaddr])

print("Total employee data written to csv file successfully")

# %%
"""
read data from csv
"""

import csv

f = open('emp.csv', 'r')

r = csv.reader(f)  # return reader object

data = list(r)

print(data)  # list of list object

for line in data:
    for word in line:
        print(word, '\t', end='')
    print()

# %%

"""
zipping and unzipping files:

1. memory utilization willbe imporved
2. Transport time will be reduced
3. improve performance of application

zipfile module


"""

import zipfile

# zipfile.ZipFile(class is there)


f = zipfile.ZipFile('files.zip', 'w', zipfile.ZIP_DEFLATED)
f.write('flowers.csv')
f.write('BooksRead.csv')
f.close()
# %%

# unzipping

f1 = zipfile.ZipFile('files.zip', 'r', zipfile.ZIP_STORED)

names = f1.namelist()

print(names)

for i in name:
    f1 = open(name, 'r')
    data = f1.read()
    print(data)

# %%
"""
Working with directory
----------------------
os module

"""
import os

print(os.getcwd())

# to create directory

# os.mkdir('createdbypython')

# compulsory createbypython (parent dir must be there)

# os.makedir('createdbypython/pythonvideos/abc')

"""
create multiple sub directories

"""

# os.makedirs('sub1/sub2/sub3')

# only sub3 will removed
# os.rmdir('sub1/sub2/sub3')

# os.removedirs('sub1/sub2/')

# rename a directory

# os.rename('sub1','sub1renamed')


# to contains of dir not subdir

os.listdir()
os.listdir('com')

# to display directory and subdirectory contain
"""
os.walk(path,todown=True,onerror=None,followlinks=false)

"""
for i in os.walk(os.getcwd()):
    print(i)

# %%

import os

for dirpath, dirname, filesname in os.walk('.'):
    print(dirpath)
    print(dirname)
    print(filesname)
    print()

# %%

"""

run other program from python

os.system('command string')

"""
import os

# os.system('cd C:/Users/sujee/Desktop/')

os.system("dir *.py")

os.system('C:/Program Files/Mozilla Firefox/firefox.exe')

# %%

"""
according to epoch time...date consider jan 1st 1970 12 am

get information about file
      statics of file
st_mode
st_ino
st_dev
st_nlink      hardlink
st_uid        user id
st_gid
st_size
st_atime     last accessed time  (jan1st 1970 12 am -----millisecond in b/w-----curren dt time)
st_mtime     last header modified time
st_ctime  last file status change

os.stat('abc.txt')
"""

stat = os.stat('test.py')  # print file information

from datetime import date, datetime, timezone

print(datetime.fromtimestamp(stat.st_atime))
# %%
"""
pickling and unpickling of object

it's like java seriliazation and deserialization .


pickle module

--write object stat into file is pickling
dump(objec,file)

--load object state from file
load(file)
"""
import pickle


class Employee:
    def __init__(self, eno, ename, eaddr):
        # variable declaration and initialization
        self.eno = eno
        self.ename = ename
        self.eaddr = eaddr

    def display(self):
        print(self.eno, '\t', self.ename, '\t', self.eaddr)


# creation of object

# e1=Employee(100,'suanki','Gorakhpur')
# e1.display()


with open('emp.dat', 'wb') as f:
    e = Employee(200, 'sujeet', 'UP')

    pickle.dump(e, f)
    print('pickling completed')
    print()

with open('emp.dat', 'rb') as f1:
    myobj = pickle.load(f1)
    print('employee information after unickling')
    myobj.display()

# %%
"""

compulsory one variabl must be pass inside def
self or some positional argument



consturctor:

 1. __init__
 2. to perform initialization of object



"""


class Student:
    def display(self):
        print('hi')

    def __init__(self, name) -> None:
        """
        all variable inside this is instance
        and outside inti all variable is class level variable"""
        self.enam = name


s = Student('rajoo')
s1 = Student('sunny')
# s.display('rajoo')


print(s.enam)
print(s1.enam)

# %%
"""
variable:

1. static variable
2. non-static variable
3. local variable


method:

static method
non static methods
classs method




static vairiable:

   static variable is class level variable variable and shared by
   each object
    class Organization:
        organization_name='suanki pvt ltd.'

   print(Organization.organization_name)

function vs method:

function declared inside class is a method and outside of a class
is called as function

"""


# static variable:

class Organization:
    '''static vs instance variable'''

    org_name = 'suanki ltd'

    def define(self):
        self.org_name = 'suanki software ltd.'


org = Organization()

print(org.org_name)

org.define()

print(org.org_name)  # instance variable

print(Organization.org_name)

# instance method fo org object
org.roll = 1234

print(org.roll)
# printing documentation string

# print(Organization.__doc__)
# help(Organization)
# print(org.__doc__)


# in the form of dictionay all variables

print(org.__dict__)

# %%

'''types of variable'''

"""
instance variable(non-static)

1. if the value of variale is varied from object ot object
2. for every object a seperate copy of instance varialbes
3. in general inside constructor we have to declare
using self keyword

4. accessing : withing class using self
              but outside using refrence variable


5. we can declare instance method inside
method using self, when we call that method we that variable come
to our object

6. where we can declare instance variable
         a. inside constructor
         b. inside method
         c. from outside of class
                 by using  object refrence
                 class Student:

                Student().myvar1='hi'
 7. remove instance variable from an object

      a. inside class
              del self.variablenam
      b. outside class
             del objectreference.variablename
"""

"""
static variabl:



class Strudent:

    '''static variable or class related variable'''
    school-name='Sharda university'


    varius place to declare:

    1. withing the class directly
    2. inside constructor by using class name
              student.staticvarible='suanki ltd'
   3. inside instance method using class name

   4. instide static method of class like belwo
           by using cls variable or class name

    5. outside of class using classname
    @classmthod
     def m2(cls):
            cls.staticvariablename='suanki ltd'


    def __init__(self,n):
        '''instance using self key'''
        self.name='name'

           Student.var='suanki ltd'

"""


class student:
    # static var
    name = 'suanki ltd'

    def __init__(self, n, s) -> None:
        self.name = n
        self.sal = s
        Student.myvar = 'hello'

    def mymthod(self):
        self.myvar1 = 'hi'
        Student.myvar3 = 'djfkfj'

    @classmethod
    def myclassmethod(cls):
        cls.myvar2 = 'hi hello'

    # not require to pass any argumnet
    @staticmethod
    def mystaticmethod():
        hi = 'hello'

    '''or'''

    def mystaticmethod2(x, y):
        '''also a static method b/c wihtoud sellf'''
        print(x + y)


Student.myar5 = 'hi helloo i am static variable'

""""

where we can modify static variable:

from anywhere we can use classnmae
or inside by cls variable (inside class method)
"""

# %%

'''
polymorphism
      --operator overloading
      --method overloading
      --
'''


class P:
    def marry(self):
        print('anki')


class C(P):
    def marry(self):
        print('suanki')


C().marry()

'''
1. Duck typing philosophy of python
        dynamically typed
                 def f1(obj):
                     obj.talk()
                      if obj is talk lik duck and wal like duck then it's duck object
2.overloading
      operator overloading
      mehod overloadin
      constructor overloading
3. overriding
       method overriding
       constructor overriding


'''


# %%
# duck type philosphoy
class Duck:
    def talk(self):
        print('Quake Quake Quake...')


class Dog:
    def brak(self):
        print('Bow bow bow...')

    def talk(self):
        print('Bow bow bow...')


class Cat:
    def talk(self):
        print('meow meow meow..')


class Goat:
    def talk(self):
        print('meah meah meah...')


l = [Duck(), Dog(), Cat(), Goat()]

for i in l:
    i.talk()

print()

# hassattr function

for obj in l:
    if hasattr(obj, 'talk'):
        obj.talk()
    elif hasattr(obj, 'bark'):
        obj.bark()

# %%
'''
10+20
str+str
str*3
10*3

#operator overloading
'''


class Book:
    def __init__(self, pages):
        self.pages = pages

    def __add__(self, other):
        return self.pages + other.pages

    def __sub__(self, other):
        pass

    def __mul__(self, other):
        pass

    def __div__(self, other):
        pass

    def __floordiv__(self, other):
        pass

    def __mod__(self, other):
        pass

    def __pow__(self, other):
        pass

    def __iadd__(self, other):  # +=
        pass

    def __imul__(self, other):
        pass

    def __idiv__(self, other):  # /=
        pass

    def __ifloordiv__(self, other):
        ''' //=
        '''
        pass

    def __imod__(self, other):  # %=
        pass

    def __ipow__(self, other):  # **=
        pass

    def __lt__(self, other):  # <
        pass

    def __gt__(self, other):  # <
        pass

    def __ge__(self, other):  # <
        pass

    def __le__(self, other):  # <
        pass

    def __eq__(self, other):  # <
        pass

    def __ne__(self, other):  # !=
        if self.pages != other.pages:
            return False
        else:
            return True


b1 = Book(10)
b2 = Book(10)
b3 = Book(20)

# for every operator there is a method associated with
# magic methods are there which is defined in object class

print(b1 + b2)

print(b1 != b2)
print(b1 != b3)


# %%

# operator *
class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def __gt__(self, other):
        return self.marks > other.marks


s1 = Student('ram', 80)
s2 = Student('rajoo', 70)

print(s1 > s2)


# %%
class Emp:
    def __init__(self, name, sal):
        self.name = name
        self.sal = sal

    def __mul__(self, other):
        return self.sal * other.days


class TimeSheet:
    def __init__(self, name, days):
        self.name = name
        self.days = days


e1 = Emp('ram', 500)
t1 = TimeSheet('ram', 28)

print('This month salary', e1 * t1)

# %%

'''
method overloading
constructore overloading
both are not officially available

'''


class Test:
    def m1(self):
        print('no args')

    def m1(self, a):
        print('no args')

    def m1(self, a, b):
        print('except this all above method m1 will be replaced')

    # other way
    def myfunctionoverloading(self, a=None, b=None, c=None):
        # if else check
        if a != None and b != None:
            pass


t = Test()

# t.m1() only one method will be there which is last remaining will be replaced
t.m1(10, 20)

# %%
'''
Abstraction:

Abstract method
Abstract class
interfaces

'''

import abc


# abstract class
# partially implemented class
class Vehicle(abc.ABC):

    @abc.abstractmethod
    def getnoofWheel(self):
        pass


# if class extending abc class and contains atleast one
# abstract method then object creation is not possible

# Vehicle().getnoofWheel()

# abstract class can contain both abstract method and normal method
# child class is resposible to provide implementation

class Bus(Vehicle):
    def getnoofWheel(self):
        return 7


Bus().getnoofWheel()


# interface does not contain concrete method mean
# if all is same as abstract


class Dbinterface(abc.ABC):
    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def disconnect(self):
        pass


class oraclecon(Dbinterface):
    def connect(self):
        # return super().connect()
        print('connected to oracle')

    def disconnect(self):
        return super().disconnect()


# convert input str into class global() return dictionary

x = input('enter dbname')

classname = globals()[x]
classname.connect()
# %%

'''
public,private ,protected
'''


class x:
    def __init__(self) -> None:
        # private variable by double __
        self.__x = 10


# outside class not accessible outside class
# x().__x

# %%
class Student:
    def __init__(self, name, roll):
        self.name = name
        self.roll = roll

    def __str__(self):
        return self.name + ' ' + str(self.roll)


def main():
    s1 = Student('rajoo', 1234)
    print(s1)


# %%
'''local varible
'''


class hell:
    def hello(self):
        i = 0
        while i < 10:
            print('local variable to method', i)
            i += 1


hell().hello()

# %%
'''
passing members of one class to another class:

Inner classes:


without existing outer one type of object if there
is not chance of inner(other) object

class Car:
   class Engine:


'''


class Human:
    def m1(self):
        print('outer')

    def __init__(self) -> None:
        self.mybrain = self.Brain()

    class Brain:
        def m2(self):
            print('inner')


Human().m1()

o = Human()
o.Brain().m2()
o.mybrain.m2()

# %%

'''
Garbage Collection

if any object is not having a refrence variable it will be
available for gc
'''

import gc

print(gc.isenabled())

gc.disable()

print(gc.isenabled())

gc.enable()

print(gc.isenabled())

"""
destructor call by gc for gracefull killing of object
just before destroying the object


__del__()
"""

import time


class Test:
    def __init__(self):
        print('Object initialization !')

    def __del__(self):
        print('it is call by gc as  and "inside destructor"')
        print('fullfiling wish and destroying object')
        print(id(self))

    def myfun(self):
        print('hi')


t1 = Test()
time.sleep(1)
t1 = None  # del t1
t2 = Test()

print(t2)

t2.myfun()
print(t1)
print(t2)

l1 = [Test(), Test(), Test()]

time.sleep(1)
del l1

"""to find number of references of an object"""

# return one extra count temperary object

sys.getrefcount(t2)

# %%
'''
inheritance and polymorphism


how we can user members of one class inside another class:
1. composition (Has-a relation)
2. using inheritance(Is-a Relation)


if there is no constructor in child class then parent
class child constructor got exccuted
'''


class X:
    a = 10

    def __init__(self):
        print('inside X', X.a)
        print('inside x constructor')

    @classmethod
    def m1(cls):
        print('X static method')

    @staticmethod
    def m2():
        print('X static method')


class Y(X):
    b = 20

    def __init__(self) -> None:
        print(Y.b)
        print(Y.a)
        Y.m1()
        Y.m2()
        super().__init__()


Y()


# %%

class Person:
    def __init__(self, name, age):
        self.age = age
        self.name = name

    def eatndrink(self):
        print('eating and drinking biryani')


class SE(Person):
    def __init__(self, name, age, eno, esal):
        super().__init__(name, age)
        self.eno = eno
        self.esal = esal

    def work(self):
        print('inside se coding in python')


s = SE('rajoo', 29, 12345, 647646)

print(s.name, s.age, s.eno, s.esal)
s.eatndrink()
s.work()


# after 1 year we can add another class


class Student(Person):
    def __init__(self, name, age, addr):
        super().__init__(name, age)
        self.addr = addr


# %%
'''super()

1. we can explicitly use parent class member from
super() except instance variable of parent and child

2. most recent instance variable by default will
 be there if parent and child both has same instance as same name


by using super() we cant' call instance variable
of parent class



'''


class A:
    a = 8888
    b = 9000

    def m1(self):
        self.c = 9999
        self.a = 10000


class B(A):

    def __init__(self) -> None:
        self.a = 1000
        super().m1()
        #       print(super().c) #invalid
        print(super().a)  # calling static variable using super() cant access instance variable of parent class
        print(self.b)


B()


# %%

class P:
    def __init__(self):
        self.b = 10


class C(P):
    def __init__(self):
        self.b = 20  # uncomment will print b letest value
        print(self.b)
        super().__init__()
        # letest value is 10 which is asigne to b
        print(self.b)


C()

# %%


'''Inheritance
'''


# Single inheritance

class P:
    def m1(self):
        print('parent method')


class C(P):
    def m2(self):
        print('chiled method')


C().m1


# Multi level inheritance

class D(C):
    def m3(self):
        print('inside d')


# %%


# multiple inheritance
class A:
    pass


class B:
    pass


class C(A, B):  # A class method and variable  is prefred as the order
    pass


# cyclic  not in python


# %%
class Student:
    university = "Suanki UGC"

    def __init__(self, first, last):
        self.first = first
        self.last = last

    @property
    def full_name(self):
        return 'full name : {} {}'.format(self.first, self.last)

    @full_name.setter
    def full_name(self, name):
        first, last = name.split(' ')
        self.first = first
        self.last = last

    @full_name.getter
    def full_name(self):
        return 'full name : {} {}'.format(self.first, self.last)

    @full_name.deleter
    def full_name(self):
        '''this method delete the value'''
        print('Delete name')
        self.first = None
        self.last = None


st = Student('sujeet', 'singh')

print(st.full_name)
name = 'Ramesh kumar'
st.full_name = name

print(st.full_name)

del st.full_name

print(st.full_name)



#%%

"""
Regular Expressions:

re module

1. compile()
how many time python in string
1st convert into regex object

2. finditer():

matcher=pattern.finditer('Learning python is very easy...')
     a. start()  start index of math
     b. end() end+1 index of the match
     c. group() -return matched string


"""

import re

pattern = re.compile('ab')
# pattern=re.compile('[A-Z')

print(type(pattern))

matcher = pattern.finditer('abaababa')

print(type(matcher))

count = 0
for match in matcher:
    count += 1
    print(match.start(), match.end(), match.group())

print('{} time match'.format(count))

'''
charecter classes:
 [abc]  either a or b or c
 [^bc]  except b and c
 [a-z]
 [A-Z]
 [0-9]
 [a-zA-Z0-9]
 [^a-zA-Z0-9]

predefined charecter classes:

\s
\S
\D
\w
\W
.

Quantifier:

a      --exactly one a
a+     --atleast one a
a*      --any no of a including 0 time
a?     --at most one a(either 1 a or zero no of a)
a{n}    --exactly n
a{n,m}     --n to m time       rg.finditer(a{2},str)
a{n,}     --
a$    -- ends with a
^a    --start with a


'''

print('*' * 5)
mystr = 'Hi i am 28 hmm'

mymatch = re.finditer('h|I', mystr)

for i in mymatch:
    print(i.start(), i.group())

"""
important function:

1. match()
2. fullmatch()
3. search()
4. findall()
5. finditer()
6. sub()
7. subn()
8. split()
9. compile()

"""
# %%

# to check given pattern at beginning of the target string
# if it's available then return match object otherwise None

import re

if re.match('pattern', 'pattern is my string') != None:
    print('pattern  in the begning')

if re.fullmatch('pattern', 'pattern') != None:
    print('full pattern  match')

# match object of first occurance
if re.search('pattern', 'this is my pattern') != None:
    print('pattern  matched in full string ')

# find all the matches and return the list
print(re.findall('[0-9]', 'this is my 89pattern 677suanki'))

# sub() substitution or replacement

print(re.sub('rajoo', 'sujeet', 'my name is rajoo'))

print(re.sub('kam', 'sujeet', 'my name is rajoo'))

# subn()  replace digit with xxx and not of replacement
# return tuple
print(re.subn('\d', 'xxxx', 'a78ds7'))

l = re.split('\.', 'com.suanki.ltd')
print(l)

s = "Learning python is very easy"

res = re.search("^Learn", s)

if res != None:
    print('start with learn')

# %%

"""
web scrapping by using regular expression
"""

import re, urllib
import urllib.request

sites = ['http://google.com', 'http://reddit.com']

for site in sites:
    print('Searching..', site)
    u = urllib.request.urlopen(site)
    text = u.read()
    # print whole page print(text)
    title = re.findall("<title>.*</title>", str(text), re.I)
    print(title[0])

    # %%

    u = urllib.request.urlopen('https://www.redbus.in/info/contactus')
    text = u.read()
    # print whole page print(text)
    title = re.findall('[0-9]{10,15}', str(text), re.I)
    print(title)

    for i in title:
        print(i)

# %%

"""date time"""

import datetime

print(datetime.date(2021, 11, 14))

tday = datetime.date.today()

print(tday)

print(tday.isoweekday())  # 1(monday) to 7(sunday)

bday = datetime.date(2021, 7, 22)

td = datetime.timedelta(days=7)

print(tday + td)
tdelta = tday - bday

print(tdelta)

# %%

import datetime

t = datetime.time(9, 30, 45, 343849)

print(t)

d = datetime.date(2016, 12, 26)

print(d)

td = datetime.datetime(2016, 12, 26, 9, 30, 45, 343849)

print(td)

# %%
import datetime
import pytz

td_today = datetime.datetime.today()

td_now = datetime.datetime.now()

td_utc = datetime.datetime.utcnow()

print(td_today)
print(td_now)
print(td_utc)

tdz_today = datetime.datetime(2016, 12, 26, 9, 30, 45, 343849, tzinfo=pytz.UTC)

print(tdz_today)

tdy = datetime.datetime.now(tz=pytz.UTC)

print(tdy.astimezone(pytz.timezone('Asia/Kolkata')))

import re

for i in pytz.all_timezones:
    if re.search('Asia', i) is not None:
        print(i, tdy.astimezone(pytz.timezone(i)))

t = datetime.datetime.now(tz=pytz.timezone('Asia/Kolkata'))

print(t.strftime('%B %Y %d'))

# %%
"""
1. process based multi tasking --executing several task but a seprate process

2. thread based multitasking--executing several task as part of same service

create thread:

1. by extending thread class
2. withoud extending thread class
3. create thread without using any class

"""
import threading

print(threading.current_thread().getName())

# %%

import threading


def display():
    for i in range(100):
        print(threading.current_thread().getName(), '----', i)


t = threading.Thread(target=display)
t.start()
for i in range(100):
    print(threading.current_thread().getName())

# %%
import threading


class Mythread(threading.Thread):
    def run(self):
        '''job of thread should be defined here'''
        for i in range(10):
            print('child thread')


t = Mythread()
t.start()

for i in range(10):
    print(threading.current_thread().getName())

# %%
'''
creating a thread without extending a thread

'''
import threading


class MyThread:
    def display(self):
        for i in range(10):
            print(threading.current_thread().getName(), '-', i)


obj = MyThread()
t1 = threading.Thread(target=obj.display())
t2 = threading.Thread(target=obj.display())
t3 = threading.Thread(target=obj.display())
t4 = threading.Thread(target=obj.display())
t5 = threading.Thread(target=obj.display())
t6 = threading.Thread(target=obj.display())
t7 = threading.Thread(target=obj.display())

t1.start()
t2.start()
t3.start()
t4.start()
t5.start()
t6.start()
t7.start()

for i in range(10):
    print(i, threading.current_thread().getName())

# %%
import time


def double(num):
    for i in num:
        time.sleep(1)
        print('double', i * 2)


def sequare(num):
    for i in num:
        time.sleep(1)
        print('sequare', i * i)


num = [10, 20, 30, 40, 50]

starttime = time.time()
double(num)
sequare(num)
print('total time taken', time.time() - starttime)

# %%
import threading
import time


def double(num):
    for i in num:
        time.sleep(1)
        print('double', i * 2)


def sequare(num):
    for i in num:
        time.sleep(1)
        print('sequare', i * i)


num = [10, 20, 30, 40, 50]

starttime = time.time()
# double(num)
# sequare(num)
t1 = threading.Thread(target=double, args=(num,))
t2 = threading.Thread(target=sequare, args=(num,))

t1.start()
t2.start()
t1.join()
t2.join()

print('total time taken', time.time() - starttime)

# %%


import threading
import time


def test():
    print(threading.current_thread().getName())


t = threading.Thread(target=test, name='suanki')
t.start()
print(threading.current_thread().ident)
print(t.ident)

print(threading.active_count())

# %%
import threading
import time


def display():
    print('Startd', threading.current_thread().getName())
    time.sleep(3)


t1 = threading.Thread(target=display, name='thread t1')
t2 = threading.Thread(target=display, name='thread t2')
t3 = threading.Thread(target=display, name='thread t3')
t4 = threading.Thread(target=display, name='thread t4')

t1.start()
t2.start()
t3.start()
t4.start()

l = threading.enumerate()
for t in l:
    print(t.name, t.is_alive())

time.sleep(3)

l1 = threading.enumerate()
for t in l1:
    print(t.name, t.is_alive())

# %%
'''
main thread is always non-daemon
 for all remaining threads daemon nature will be inherite from parent

 last non-daemon thread terminates then daemon thread gets terminated
 (daemon thread to provide support for non-daemon thread , if non-daemon thread terminate no use of backgoung thread i.e. daemon)

'''
import threading


def display():
    for i in range(20):
        print('hi i am lazy thread')
        time.sleep(3)


t = threading.Thread(target=display)
t.setDaemon(True)
print(t.daemon)
t.start()
# time.sleep(5)
print('main thread')

# %%

import threading


def job1():
    print('job one')
    print(threading.current_thread().daemon)
    t = threading.Thread(target=job2, name='chiled thread2')
    t.start()


def job2():
    print('job two')
    print(threading.current_thread().daemon)


t = threading.Thread(target=job1, name='chiled thread')

t.setDaemon(True)
t.start()

# %%
'''
synchronization
at a time only one thread
1. Lock
2. RLock
3. Semaphore
'''

# data inconsistency problem


import threading
import time

count = 0


def wish(name):
    global count
    # this can be executed by only one thread
    # crtical source code inside for loop
    for i in range(10):
        count += 1
        print(i, end=':')
        time.sleep(1)
        print(name + ' playing')
    # this can be executed by any thread
    count = 0
    for i in range(20):
        count += 1
        print('completed task ', threading.currentThread().getName(), count)


t1 = threading.Thread(target=wish, args=('ram',), name='one')
t2 = threading.Thread(target=wish, args=('shyam',), name='two')
t3 = threading.Thread(target=wish, args=('Dhoni',), name='three')
t1.start()
t1.join()
t2.start()
t2.join()
t3.start()
t3.join()

print('i am main ')
print(count)

# %%

import threading
import time

count = 0
lcount = 0
l = threading.Lock()


def wish(name):
    global count
    # this can be executed by only one thread
    # crtical source code inside for loop
    l.acquire()
    for i in range(10):
        count += 1
        print(i, end=':')
        print(name + ' playing')
    l.release()
    # this can be executed by any thread
    global lcount
    for i in range(20):
        lcount += 1
        print('completed task ', threading.currentThread().getName())


t1 = threading.Thread(target=wish, args=('ram',), name='one')
t2 = threading.Thread(target=wish, args=('shyam',), name='two')
t3 = threading.Thread(target=wish, args=('Dhoni',), name='three')
t1.start()
t2.start()
t3.start()

time.sleep(10)

# %%

'''problem with lock
there is no tracking mechanism with lock i.g. owner of lock
problem in recursion
def fact(n):
    l.acquire()
    if n !=0:
        result=n*fact(n-1)
    l.release()
reentrant lock
'''

import threading

l = threading.RLock()
print('main thread tryuing to acquire lock...')
l.acquire()
print('main thread acquire lock...')
l.acquire()  # no of hold 2
# recursion function
print('main thread acquire lock again...')
l.release()  # here lock won't be release no of hold is 1
l.release()  # lock will be released here only

# %%
'''
limiting the thread execution
in some scenario synchronization
we shoul use semaphore
'''
import threading
import time

# s=threading.Semaphore() it's exactly as Lock()
# default count value is :1

# s=threading.Semaphore(5)
# five thread
# s.acquire() #5-1

s = threading.Semaphore()


def wish(name):
    s.acquire()
    for i in range(10):
        print(name, 'is playing')
        time.sleep(.02)
    s.release()


t1 = threading.Thread(target=wish, args=('ram',), name='one')
t2 = threading.Thread(target=wish, args=('shyam',), name='two')
t3 = threading.Thread(target=wish, args=('Dhoni',), name='three')
t1.start()
t2.start()
t3.start()

# %%


import threading
import time

# s=threading.Semaphore() it's exactly as Lock()
# default count value is :1

# s=threading.Semaphore(5)
# five thread
# s.acquire() #5-1

s = threading.Semaphore(2)


def wish(name):
    s.acquire()
    for i in range(10):
        print(name, 'is playing')
        time.sleep(2)
    s.release()


t1 = threading.Thread(target=wish, args=('ram',), name='one')
t2 = threading.Thread(target=wish, args=('shyam',), name='two')
t3 = threading.Thread(target=wish, args=('Dhoni',), name='three')
t1.start()
t2.start()
t3.start()

# %%
'''
Interthread communication

producer and consumer problem

1. event
2. condition
3. queue

'''

"""
EVENT:::--

#signal and wait
event internalyy manages the flag this flag can be
set or clear
. if flag is set ( green signal) then other thread should execute
. if flag is clear (red signal) the other thread should wait

"""

import threading

'''
e=threading.Event()

e.set()

e.is_set()

e.clear()
e.wait()
e.wait(5)

'''
import time

e = threading.Event()


def consumer():
    print('waiting for green signal')
    e.wait()
    print('got greean signal starting....')


def producer():
    time.sleep(2)
    print('producing iterms')
    print('giving notification to consumer by setting event')
    e.set()


t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=consumer)

t1.start()
t2.start()

# %%
import threading
import time

e = threading.Event()


def trafficPolice():
    while True:
        time.sleep(5)
        print('Traffic police giving green signal')
        e.set()
        time.sleep(10)
        print('traffic police giving red signal')
        e.clear()


def driver():
    num = 0
    while True:
        print('Driver waiting for green signal')
        e.wait()
        print('Traffic signal is Green .. Vehical can move')
        while e.is_set():
            num = num + 1
            print('Vehical Number: ', num, ' Crossing the signal')
            time.sleep(1)


t1 = threading.Thread(target=trafficPolice)
t2 = threading.Thread(target=driver)

t1.start()
t2.start()

# %%


import threading
import time

e = threading.Event()


def trafficPolice():
    while True:
        time.sleep(5)
        print('Traffic police giving green signal')
        e.set()
        time.sleep(10)
        print('traffic police giving red signal')
        e.clear()


def driver():
    num = 0
    while True:
        print('Driver waiting for green signal')
        e.wait()
        print('Traffic signal is Green .. Vehical can move')
        while e.is_set():
            num = num + 1
            print('Vehical Number: ', num, ' Crossing the signal')
            time.sleep(1)
        print('Traffic signal is red Driver has to wait')


t1 = threading.Thread(target=trafficPolice)
t2 = threading.Thread(target=driver)

t1.start()
t2.start()

# %%
'''interthread communication by using condition object

some kind of state change

      condition object is always associated with lock(Rlock)
     acquire()
     release()
c=Condition()

methods:

c.acquired()
c.release()

c.wait()==thread will enter into waiting state
c.wait(time)

c.notif()    for one thread
c.notifyall()  for all thread

'''

"""
case study:


producer thread:
     generate item
     acquire the condition object c.acquire
     add iterm to the resource
     c.notify() or c.notifyall()
     c.release()


consumer thread:
c.acquire()
ir iterm is not thre
c.wait()
consume item
c.release()


"""
# %%

import threading


def consume(c):
    c.acquire()
    print('consumer waiting fr updation....')
    c.wait()
    print('consumer got the notification and consuming the item...')
    c.release()


def produce(c):
    c.acquire()
    print('producer producing item...')
    print('producer giving the notification...')
    c.notify()
    c.release()


c = threading.Condition()
t1 = threading.Thread(target=consume, args=(c,))
t2 = threading.Thread(target=produce, args=(c,))

t1.start()
t2.start()

# %%
import threading
import time
import random

items = []


def produce(c):
    while True:
        c.acquire()
        item = random.randint(1, 100)
        print('producer producing the item')
        items.append(item)
        c.notify()
        c.release()
        time.sleep(5)


def printit(c):
    while True:
        c.acquire()
        print('waiting for list to update')
        c.wait()
        print('going to show')
        for i in items:
            print(i, end=' ')
        print()
        c.release()


c = threading.Condition()
t1 = threading.Thread(target=produce, args=(c,))
t2 = threading.Thread(target=printit, args=(c,))

t1.start()
t2.start()

# %%

"""pdbc

"""
'''
1. import data base specific module1
import cx_Oracle
import pymysql #for mysql

2. establish connection /w python and Db
    con=cs_Oracle.connect('scott/tiger@localhost')


3. cursor object
   cursor=con.cursor()

 4.execute oure query
      cursor.execute(sqlquer) ---single query
      cursor.executescript(sqlqueries) --to execute a string of sql queries seperate
      cursor.executemany() to execute a parameterized query

5. fethc the results
cursor.fetchone()--fetch only one row
fetchall()--fetch all rows
fetchmany(n) --to fetch n rows


     '''

# %%
import cx_Oracle

# to connect with oracle db
try:
    query = 'create table test3(eno number,ename varchar2(30))'
    con = cx_Oracle.connect('suanki/yourpassword@localhost:1521/pdborcl')
    cursor = con.cursor()
    cursor.execute(query)
except Exception as e:
    print(e)
    if con != None:
        con.rollback()
finally:
    if cursor != None:
        cursor.close()
    if con != None:
        con.close()

# %%

import cx_Oracle

# to connect with oracle db
try:
    # query='create table test3(eno number,ename varchar2(30))'
    query = "insert into test3 values(1,'rajoo')"
    con = cx_Oracle.connect('suanki/yourpassword@localhost:1521/pdborcl')
    cursor = con.cursor()
    cursor.execute(query)
    con.commit()
except Exception as e:
    if con != None:
        con.rollback()
    print(e)
finally:
    if cursor:
        cursor.close()
    if con:
        con.close()

# %%
import cx_Oracle

# to connect with oracle db
try:
    query = "insert into test3 values(:eno,:ename)"
    records = [(2, 'ram'),
               (3, 'shyam'),
               (4, 'shaquil')
               ]
    con = cx_Oracle.connect('suanki/yourpassword@localhost:1521/pdborcl')
    cursor = con.cursor()
    cursor.executemany(query, records)
    con.commit()
    print("Done")
except Exception as e:
    if con != None:
        con.rollback()
    print(e)
finally:
    if cursor:
        cursor.close()
    if con:
        con.close()

# %%
import cx_Oracle

# to connect with oracle db
try:
    con = cx_Oracle.connect('suanki/yourpassword@localhost:1521/pdborcl')
    cursor = con.cursor()
    while True:
        eno = int(input('enter emp number:'))
        ename = input('emp name:')
        query = "insert into test3 values(%d,'%s')"
        cursor.execute(query % (eno, ename))
        con.commit()
        print('record inserted')
        option = input('do you want insert more recore Y/N')
        if option == 'N' or option == 'n' or option == 'No' or option == 'no':
            break

except Exception as e:
    if con != None:
        con.rollback()
    print(e)
finally:
    if cursor:
        cursor.close()
    if con:
        con.close()

# %%

import cx_Oracle

try:
    con = cx_Oracle.connect('suanki/yourpassword@localhost:1521/pdborcl')
    cursor = con.cursor()
    query = "update test3 set ename='superman' where ename='%s'"
    name = input('enter name')
    cursor.execute(query % (name))
    con.commit()
except Exception as e:
    print(e)
    con.rollback()
finally:
    cursor.close()
    con.close()
# %%

import cx_Oracle

try:
    con = cx_Oracle.connect('suanki/yourpassword@localhost:1521/pdborcl')
    cursor = con.cursor()
    query = "select * from test3"
    cursor.execute(query)
    row = cursor.fetchone()

    while row is not None:
        print(row)
        row = cursor.fetchone()

except Exception as e:
    print(e)
finally:
    cursor.close()
    con.close()

# %%

import cx_Oracle

try:
    con = cx_Oracle.connect('suanki/yourpassword@localhost:1521/pdborcl')
    cursor = con.cursor()
    query = "select * from test3"
    cursor.execute(query)
    row = cursor.fetchall()

    for data in row:
        print(data[0], data[1])

except Exception as e:
    print(e)
finally:
    cursor.close()
    con.close()

# %%
import cx_Oracle

try:
    con = cx_Oracle.connect('suanki/yourpassword@localhost:1521/pdborcl')
    cursor = con.cursor()
    query = "select * from test3"
    cursor.execute(query)
    n = int(input('no of row to fetch: '))
    row = cursor.fetchmany(n)
    for data in row:
        print(data[0], data[1])

except Exception as e:
    print(e)
finally:
    cursor.close()
    con.close()

# %%


'''
DATETIME
'''

import datetime as dt
import time as t

print(dt.date.today())
print(t.time())
