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







