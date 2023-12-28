# Databricks notebook source
# MAGIC %md
# MAGIC ##### Find out ?
# MAGIC 1. find no of python developer in India
# MAGIC 2. find top 5 country with highest python developer
# MAGIC 3. find top 5 earner in each country
# MAGIC 4. find language used for top 5 earner in each country
# MAGIC 5. How many dev is coding as hoby in India and USA
# MAGIC 5. compare survey 2022 with 2019 
# MAGIC
# MAGIC               a. how much increase in python devloper as whole
# MAGIC               b. how much increase in python developer in each country and select only top 5
# MAGIC               c. 
# MAGIC         
# MAGIC         ...and many more
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Run ingest data on datalakes

# COMMAND ----------



# COMMAND ----------

india_stockoverflow_df=spark.read.parquet("dbfs:/mnt/suankistoragedatalakes/processed/indiasurvey")

# COMMAND ----------

display(india_stockoverflow_df)

# COMMAND ----------


