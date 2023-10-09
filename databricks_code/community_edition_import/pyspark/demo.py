# Databricks notebook source
"""file_location = "/FileStore/tables/retail-data/2010_12_08.csv"
random_file = "dbfs:/FileStore/tables/tstfile.csv"

"""

# COMMAND ----------

l1 = [1,2,4,5]
l2 = [2,3,5,6,7,8]

list_len = len(l2)
diff_list = list()

for i in range(0,list_len):
   if l1[i] not in l2:
         diff_list.append(l1[i])
   if l2[i] not in l1:
         diff_list.append(l2[i])

print(diff_list)  #l1 -2

diff_list = []

# COMMAND ----------

name = 'this is list of names'

name_list = name.split()

print(name_list)

name_list.reverse()

# print(name_list)

# print(' '.join(name_list))

reversed_list = [name[::-1] for name in name_list]
print(reversed_list)

listen

silent


# COMMAND ----------

user_input = 'silent'
user_input1 = 'listen'

print(sort(user_input))

for c in user_input:
  pass
  
  


