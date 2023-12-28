# Databricks notebook source
# MAGIC %fs
# MAGIC
# MAGIC head dbfs:/FileStore/tables/weird_file/productrevenu.csv
# MAGIC

# COMMAND ----------


#dbutils.fs.ls("/FileStore/tables/weird_file/productrevenu.csv")



# COMMAND ----------

parameter_option = {'sep':'~|', 'header':True, 'inferSchema':False}

df  = spark.read.format('csv').option('delimiter','~|').load('dbfs:/FileStore/tables/weird_file/productrevenu.csv')

# COMMAND ----------

df.show()
