# Databricks notebook source
# MAGIC %run ./setup/mount_storage_layer

# COMMAND ----------

# MAGIC %run ./ingest/ingest_survey_on_datalakes

# COMMAND ----------

# MAGIC %run ./transformer/transformer_survey_2019

# COMMAND ----------

# MAGIC %md
# MAGIC ###magic command
# MAGIC %python
# MAGIC %sh
# MAGIC %sql
# MAGIC %scala
# MAGIC %sh
# MAGIC %fs
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

for i in dbutils.fs.ls("/mnt"):
  print(i)

# COMMAND ----------

dbutils.fs.help('ls')
