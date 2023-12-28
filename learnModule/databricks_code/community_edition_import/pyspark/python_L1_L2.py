# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###python type
# MAGIC """
# MAGIC Python has the following data types built-in by default, in these categories:
# MAGIC
# MAGIC Text Type:	str 
# MAGIC
# MAGIC Numeric Types:	int, float, complex
# MAGIC
# MAGIC Sequence Types:	list, tuple, range
# MAGIC
# MAGIC Mapping Type:	dict
# MAGIC
# MAGIC Set Types:	set, frozenset
# MAGIC
# MAGIC Boolean Type:	bool
# MAGIC
# MAGIC Binary Types:	bytes, bytearray, memoryview
# MAGIC
# MAGIC None Type:	NoneType
# MAGIC
# MAGIC
# MAGIC """
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###decalring python types
# MAGIC x = str("Hello World")	str	
# MAGIC
# MAGIC x = int(20)	int	
# MAGIC
# MAGIC x = float(20.5)	float	
# MAGIC
# MAGIC x = complex(1j)	complex	
# MAGIC
# MAGIC x = list(("apple", "banana", "cherry"))	list	
# MAGIC
# MAGIC x = tuple(("apple", "banana", "cherry"))	tuple	
# MAGIC
# MAGIC x = range(6)	range	
# MAGIC
# MAGIC x = dict(name="John", age=36)	dict	
# MAGIC
# MAGIC x = set(("apple", "banana", "cherry"))	set	
# MAGIC
# MAGIC x = frozenset(("apple", "banana", "cherry"))	frozenset	
# MAGIC
# MAGIC x = bool(5)	bool	
# MAGIC
# MAGIC x = bytes(5)	bytes	
# MAGIC
# MAGIC x = bytearray(5)	bytearray	
# MAGIC
# MAGIC x = memoryview(bytes(5))	memoryview
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###python scope of the variables
# MAGIC
# MAGIC https://www.programiz.com/python-programming/global-local-nonlocal-variables#:~:text=In%20Python%2C%20we%20can%20declare,%2C%20global%2C%20and%20nonlocal%20scope.&text=Here%2C%20the%20sum%20variable%20is,is%20called%20a%20local%20variable.

# COMMAND ----------

import random

print(random.randrange(1, 10))




# COMMAND ----------

x = int(1)   # x will be 1
y = int(2.8) # y will be 2
z = int("3") # z will be 3
x = float(1)     # x will be 1.0
y = float(2.8)   # y will be 2.8
z = float("3")   # z will be 3.0
w = float("4.2") # w will be 4.2

x = str("s1") # x will be 's1'
y = str(2)    # y will be '2'
z = str(3.0)  # z will be '3.0'


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ###Strings are Arrays
# MAGIC **strings in Python are arrays of bytes representing unicode characters.
# MAGIC

# COMMAND ----------

#string operations

a = "Hello, World!"
print(len(a))



txt = "The best things in life are free!"
print("free" in txt)


if "free" in txt:
  print("Yes, 'free' is present.")



print("expensive" not in txt)




# COMMAND ----------

b = "Hello, World!"

print(b[2:5])
print(b[:5])

# COMMAND ----------

b = "Hello, World!"
print(b[-5:-2])

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ###STring method
# MAGIC
# MAGIC https://www.w3schools.com/python/python_strings_methods.asp
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

txt = "I love apples, apple are my favorite fruit"

x = txt.count("apple")  #string.count(value, start, end)  start =0 

print(x)


x = txt.count("sujeet")

print(x)


txt = "Hello, welcome to my world."

x = txt.endswith(".")  #string.endswith(value, start, end)

print(x)

# COMMAND ----------

# MAGIC %md
# MAGIC The find() method finds the first occurrence of the specified value.
# MAGIC
# MAGIC The find() method returns -1 if the value is not found.
# MAGIC
# MAGIC The find() method is almost the same as the index() method, the only difference is that the index() method raises an exception if the value is not found. (See example below)

# COMMAND ----------

txt = "Hello, welcome to my world."

x = txt.find("welcome")  #string.find(value, start, end)

print(x)
print(txt.find("q"))

print(txt.index("q"))

# COMMAND ----------


txt = "For only {price:.2f} dollars!"
print(txt.format(price = 49))
txt1 = "My name is {fname}, I'm {age}".format(fname = "John", age = 36)
txt2 = "My name is {0}, I'm {1}".format("John",36)
txt3 = "My name is {}, I'm {}".format("John",36)


# COMMAND ----------

#(space)!#%&? etc.

txt = "Company12 this"

x = txt.isalnum()
x = txt.isalpha()
x = txt.isdigit()
x = txt.isdecimal()
x =  txt.islower()

x= txt.isupper()
x= txt.islower()


x = txt.strip()

print(txt.split())
print(txt.split(","))

print(txt.startswith('C'))
print(txt.upper())


# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ##Boolean
# MAGIC
# MAGIC *Anything which has some values consider true 
# MAGIC
# MAGIC

# COMMAND ----------

bool(False)
bool(None)
bool(0)
bool("")
bool(())
bool([])
bool({}) 

# COMMAND ----------


class myclass():
  def __len__(self):
    return 0

 #One more value, or object in this case, evaluates to False, and that is if you have an object that is made from a class with a __len__ function that returns 0 or False:    

myobj = myclass()
print(bool(myobj)) 



# COMMAND ----------



def myFunction() :
  return True

if myFunction():
  print("YES!")
else:
  print("NO!") 




# COMMAND ----------



x = 200

print(isinstance(x, int)) 





# COMMAND ----------

# MAGIC %md
# MAGIC ##operator in python
# MAGIC https://www.w3schools.com/python/python_operators.asp
# MAGIC
# MAGIC Python divides the operators in the following groups:
# MAGIC
# MAGIC     Arithmetic operators
# MAGIC     Assignment operators
# MAGIC     Comparison operators
# MAGIC     Logical operators
# MAGIC     Identity operators
# MAGIC     Membership operators
# MAGIC     Bitwise operators
# MAGIC
# MAGIC     
# MAGIC - Addition 	x + y 	
# MAGIC - Subtraction 	x - y 	
# MAGIC - Multiplication 	x * y 	
# MAGIC - Division 	x / y 	
# MAGIC - Modulus 	x % y 	
# MAGIC - Exponentiation 	x ** y 	
# MAGIC - Floor division 	x // y
# MAGIC
# MAGIC

# COMMAND ----------



print(9//3)

print(9/3)

print(9%3)


# COMMAND ----------


#Identity operators are used to compare the objects, not if they are equal, but if they are actually the same object, with the same memory location:
x= 10
y = 11
z = y
print(x is y)
print(z is y)

print(id(x))
print(id(y))
print(id(z))



# COMMAND ----------

# MAGIC %md
# MAGIC ##Python Membership Operators
# MAGIC
# MAGIC ####Membership operators are used to test if a sequence is presented in an object:
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

print(10 in [1,2,3,4,5,10])

# COMMAND ----------


#Addition + and subtraction - has the same precedence, and therefor we evaluate the expression from left to right:
print(5 + 4 - 7 + 3) 


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##Python Collection
# MAGIC
# MAGIC ####List items are  Indexed,ordered, changeable, and allow duplicate values.
# MAGIC
# MAGIC     Method 	  Description
# MAGIC     append()	Adds an element at the end of the list
# MAGIC     clear()	  Removes all the elements from the list
# MAGIC     copy()	  Returns a copy of the list
# MAGIC     count()	  Returns the number of elements with the specified value
# MAGIC     extend()	Add the elements of a list (or any iterable), to the end of the current list
# MAGIC     index()	  Returns the index of the first element with the specified value
# MAGIC     insert()	Adds an element at the specified position
# MAGIC     pop()	    Removes the element at the specified position
# MAGIC     remove()	Removes the item with the specified value
# MAGIC     reverse()	Reverses the order of the list
# MAGIC     sort()	  sorts the list
# MAGIC
# MAGIC ####Tuple item are indexed, ordered, unchangeable,  allow duplicate
# MAGIC     -count()	Returns the number of times a specified value occurs in a tuple
# MAGIC     -index()	Searches the tuple for a specified value and returns the position of where it was found
# MAGIC
# MAGIC
# MAGIC ####SET item are unindexed, unordered, unchangeable, no duplicate
# MAGIC    *Note: Set items are unchangeable, but you can remove items and add new items.
# MAGIC
# MAGIC     Method	Description
# MAGIC     add()	Adds an element to the set
# MAGIC     clear()	Removes all the elements from the set
# MAGIC     copy()	Returns a copy of the set
# MAGIC     difference()	Returns a set containing the difference between two or more sets
# MAGIC     difference_update()	Removes the items in this set that are also included in another, specified set
# MAGIC     discard()	Remove the specified item
# MAGIC     intersection()	Returns a set, that is the intersection of two other sets
# MAGIC     intersection_update()	Removes the items in this set that are not present in other, specified set(s)
# MAGIC     isdisjoint()	Returns whether two sets have a intersection or not
# MAGIC     issubset()	Returns whether another set contains this set or not
# MAGIC     issuperset()	Returns whether this set contains another set or not
# MAGIC     pop()	Removes an element from the set
# MAGIC     remove()	Removes the specified element
# MAGIC     symmetric_difference()	Returns a set with the symmetric differences of two sets
# MAGIC     symmetric_difference_update()	inserts the symmetric differences from this set and another
# MAGIC     union()	Return a set containing the union of sets
# MAGIC     update()	Update the set with the union of this set and others
# MAGIC
# MAGIC

# COMMAND ----------

thislist = ["apple", "banana", "cherry"]
print(thislist)


#list length

print("list length: ", len(thislist))

#list item can be any type
list1 = ["apple", "banana", "cherry"]
list2 = [1, 5, 7, 9, 3]
list3 = [True, False, False]

list1 = ["abc", 34, True, 40, "male"]


print(type(list1))


#accessing items
print(thislist[1])

thislist = ["apple", "banana", "cherry", "orange", "kiwi", "melon", "mango"]

print(thislist[2:5])



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Python Collections (Arrays)
# MAGIC -There are four collection data types in the Python programming language:
# MAGIC     
# MAGIC     List is a collection which is ordered and changeable. Allows duplicate members.
# MAGIC
# MAGIC     Tuple is a collection which is ordered and unchangeable. Allows duplicate members.
# MAGIC
# MAGIC     Set is a collection which is unordered, unchangeable*, and unindexed. No duplicate members.
# MAGIC
# MAGIC     Dictionary is a collection which is ordered** and changeable. No duplicate members.
# MAGIC     *Set items are unchangeable, but you can remove and/or add items whenever you like.
# MAGIC
# MAGIC     **As of Python version 3.7, dictionaries are ordered. In Python 3.6 and earlier, dictionaries are unordered.

# COMMAND ----------


thislist = ["apple", "banana", "cherry", "orange", "kiwi", "melon", "mango"]


for i in range(0,len(thislist)):
    print(thislist[i])





# COMMAND ----------

thislist = ["apple", "banana", "cherry", "orange", "kiwi", "mango"]

thislist[1:3] = ["blackcurrant", "watermelon"]


print(thislist)

thislist[1:3] = ["watermelon"]

print(thislist)


# COMMAND ----------

thislist = ["apple", "banana", "cherry", "orange", "kiwi", "melon", "mango"]

# append()	Adds an element at the end of the list

thislist.append('appendded_apple')
print(thislist)

# clear()	  Removes all the elements from the list

thislist.clear()

print(thislist)


# copy()	  Returns a copy of the list

thislist = ["apple", "banana", "cherry", "orange", "kiwi", "melon", "mango"]

thislist_one = thislist.copy()

print(id(thislist_one))
print(id(thislist))

thislist.append('tst')

print(thislist_one)
print(thislist)


# count()	  Returns the number of elements with the specified value

thislist.count('banana')


# extend()	Add the elements of a list (or any iterable), to the end of the current list

thislist.extend([10,11])
print(thislist)

thislist.append([10,11])
print(thislist)

# index()	  Returns the index of the first element with the specified value


print(thislist.index(10))

try:
  print(thislist.index(13))
except ValueError:
  print("no item")

# insert()	Adds an element at the specified position

thislist.insert(0,13)

print(thislist)

# pop()	    Removes the element at the specified position and return the value

print(thislist.pop(0))
try:
  print(thislist.pop(50))
except IndexError:
  print("index error")

# remove()	Removes the item with the specified value

thislist.remove(10)

try:
  thislist.remove("13")
except ValueError:
  print("item is not in the list")
print(thislist)

# reverse()	Reverses the order of the list

thislist.reverse()
print(thislist)

# sort()	  sorts the list

thislist.sort() #won't work when diffrent types in the signle list

print(thislist)

# COMMAND ----------

#list of fruites containing letter start with a


fruits = ["apple", "banana", "cherry", "kiwi", "mango"]
newlist = [fruit for fruit in fruits if fruit.startswith('a')]

print(newlist)

# COMMAND ----------

t = [x if x<9 else 10 for x in range(10) if x>5]

print(t)

# COMMAND ----------

#customization of sort function

def myfunc(n):
  return abs(n - 50)

thislist = [100, 50, 65, 82, 23]
thislist.sort(reverse  = True)
print(thislist)

thislist.sort(key = myfunc)

print(thislist)

# COMMAND ----------

thislist = ["banana", "Orange", "Kiwi", "cherry"]

thislist.sort(key = str.lower)

print(thislist)

# COMMAND ----------

#make a copy of list

thislist = ["apple", "banana", "cherry"]
mylist = list(thislist)
print(mylist)

print(id(thislist))
print(id(mylist))

# COMMAND ----------

#Join two list:


list1 = ["a", "b", "c"]
list2 = [1, 2, 3]

list3 = list1+list2
print(list3)

list4 = list()

list1.extend(list2)
print(list1)

# COMMAND ----------

#tuple


thisset = {"apple", "banana", "cherry"}
print(thisset)


#true and 1 is considered same value

thisset = {"apple", "banana", "cherry", True, 1, 2}

print(thisset)
print(len(thisset))


set1 = {"apple", "banana", "cherry"}
set2 = {1, 5, 7, 9, 3}
set3 = {True, False, False}

set1 = {"abc", 34, True, 40, "male"}


# COMMAND ----------

#Once a set is created, you cannot change its items, but you can add new items.


thisset = {"apple", "banana", "cherry"}

thisset.add("orange")

print(thisset)


# COMMAND ----------

thisset = {"apple", "banana", "cherry"}
tropical = {"pineapple", "mango", "papaya"}

mylist = ["kiwi", "orange"]

#add any iterable
thisset.update(mylist)
thisset.update(tropical)


print(thisset)
print(thisset)

#remove

thisset.remove("banana")

print(thisset)

#remove item with discard()  --> this will not raise error if item not exist or remove() ---> but this one will


thisset.discard('banana')

thisset.pop()#Remove a random item by using the pop() method:

# COMMAND ----------

# MAGIC
# MAGIC %md 
# MAGIC
# MAGIC ###python dictionary
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

thisdict = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}
print(thisdict["brand"])


thisdict = {
  "brand": "Ford",
  "electric": False,
  "year": 1964,
  "colors": ["red", "white", "blue"]
}


print(thisdict['colors'])

# COMMAND ----------

print(len(thisdict))

# COMMAND ----------

thisdict = dict(name = "John", age = 36, country = "Norway")
print(thisdict)

# COMMAND ----------

car = {
"brand": "Ford",
"model": "Mustang",
"year": 1964
}

#The list of the keys ,values,items is a view of the dictionary, meaning that any changes done to the dictionary will be reflected in the keys list,items and values.

x = car.keys()
y = car.items()
z = car.values()

print(x) #before the change
print(y) #before the change
print(z) #before the change

car["color"] = "white"

print("======")

print(x) #before the change
print(y) #before the change
print(z) #before the change


# COMMAND ----------

#change value

car = {
"brand": "Ford",
"model": "Mustang",
"year": 1964
}

car['year'] = 2020

print(car['year'])

car.update({'year':2021})

print(car)


#add item

car["color"] = "red"

print(car)

car.update({'wheel':'rearDrive'}  )

print(car)


#remove item

car.pop("model")
print(car)

last_item = car.popitem()
print(type(last_item)) #last item remove 

# COMMAND ----------



# COMMAND ----------

#nested dictionary

myfamily = {
  "child1" : {
    "name" : "Emil",
    "year" : 2004
  },
  "child2" : {
    "name" : "Tobias",
    "year" : 2007
  },
  "child3" : {
    "name" : "Linus",
    "year" : 2011
  }
}

print(myfamily)

# COMMAND ----------

child1 = {
  "name" : "Emil",
  "year" : 2004
}
child2 = {
  "name" : "Tobias",
  "year" : 2007
}
child3 = {
  "name" : "Linus",
  "year" : 2011
}

myfamily = {
  "child1" : child1,
  "child2" : child2,
  "child3" : child3
}

# COMMAND ----------


#initate dict with default keys
x = ('key1', 'key2', 'key3')
y = 0

thisdict = dict.fromkeys(x, y)

print(thisdict)

t =(12,13,14)

print(dict.fromkeys(x,t))






# COMMAND ----------

x = ('key1', 'key2', 'key3')

thisdict = dict.fromkeys(x)

print(thisdict)

# COMMAND ----------

car = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}

x = car.setdefault("model", "Bronco")

print(x)

x = car.setdefault("color", "Brown")

print(x)

print(car.get('color','Black')) #will return Brown

print(car.get('new_model',2020))



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Python method

# COMMAND ----------

def myfunction(*kids):
    print(kids, type(kids))


myfunction("this is test call")
myfunction("this" ,"is test call")
myfunction("this", "is test", "call")


# COMMAND ----------

def my_function(child3, child2, child1):
  print("The youngest child is " + child3)

my_function(child1 = "Emil", child2 = "Tobias", child3 = "Linus")


#kwargs

def my_function(**kwargs):
  print("The youngest child is " + kwargs['child3'], type(kwargs))

my_function(child1 = "Emil", child2 = "Tobias", child3 = "Linus")






# COMMAND ----------

def my_function(country = "Norway"):
  print("I am from " + country)



my_function()

# COMMAND ----------

def my_function(x):
  if x > 5:
      return "x is bigger than 5"
  else:
      return "x is lesser than 5"


print(my_function())

# COMMAND ----------

def tri_recursion(k):
  if(k > 0):
     return k + tri_recursion(k - 1)
  else:
    return 0


print("\n\nRecursion Example Results")
print(tri_recursion(999))


# COMMAND ----------


#lambda function
#How to really use lambda function

def myfunc(n):
  return lambda a : a * n

print(myfunc(10)(20))

mydoubler = myfunc(2)

print(mydoubler(11))





# COMMAND ----------

# MAGIC %md
# MAGIC **Class concepts

# COMMAND ----------


class Person:
  def __init__(self, name, age):
    self.name = name
    self.age = age

  def __str__(self):
      return f"<Person {self.name} {self.age}"
p1 = Person("John", 36)

print(p1)



# COMMAND ----------

#inheritance

class Person:
  def __init__(self, fname, lname):
    self.firstname = fname
    self.lastname = lname

  def printname(self):
    print(self.firstname, self.lastname)


class Student(Person):
  def __init__(self, fname, lname, sid):
    super().__init__(fname, lname)
    self.sid = sid

s1 = Student('s1','last_s1',1234)


s1.sid


# COMMAND ----------

mytuple = ("apple", "banana", "cherry")

mytupleiter = iter(mytuple)

print(next(mytupleiter))
print(next(mytupleiter))


# COMMAND ----------

class MyNumbers:
  def __iter__(self):
    self.a = 1
    return self

  def __next__(self):
    if self.a <= 20:
      x = self.a
      self.a += 1
      return x
    else:
      raise StopIteration

myclass = MyNumbers()
myiter = iter(myclass)

for x in myiter:
  print(x)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Questions

# COMMAND ----------

#Print the sum of the current number and the previous number


mylist = [1,2,3,5,6,7,9]
result = 0

for l in range(len(mylist)):
    result = result + mylist[l]
    print(result)




    



# COMMAND ----------


