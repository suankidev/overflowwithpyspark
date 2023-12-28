# Databricks notebook source
"""inmomory scan ( b/c reading from cached df)
filter
hashaggregate
exchange hashpartitioning
hashaggregate
write"""





import re
pattern = re.compile(r"data", flags=(re.IGNORECASE|re.MULTILINE))

mystring ="this is data of data for 2023 04 24!"

# COMMAND ----------

rslt = re.search(pattern,mystring)

print(rslt)



# COMMAND ----------





