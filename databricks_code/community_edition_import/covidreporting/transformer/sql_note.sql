-- Databricks notebook source
create database if not exists overflow;

-- COMMAND ----------

show databases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("show databases").show()

-- COMMAND ----------

create table customer
(name String,
addr string
)
stored as parquet
location "dbfs:/mnt/suankistoragedatalakes/processed/cusomer"

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use overflow

-- COMMAND ----------


