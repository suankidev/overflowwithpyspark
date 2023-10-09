# Databricks notebook source
# MAGIC %md
# MAGIC ###running setup script to mount the storage layer

# COMMAND ----------

# MAGIC %run ../setup/mount_storage_layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###reading stockoverflow file and schema detail files

# COMMAND ----------

file_options={'header':True,'inferSchema':False}
stock_overflow_df=spark.read.format('csv').options(**file_options).load(f"{survey_path}/survey_results_public.csv")
stock_overflow_schema_details=spark.read.format('csv').options(**file_options).load(f"{survey_path}/survey_results_schema.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####DATA CLEAN

# COMMAND ----------

import pyspark.sql.functions as f

column_select = ['employment','Country','edlevel','yearscode','JobSat','CareerSat','CurrencySymbol','ConvertedComp','DatabaseWorkedWith','LanguageDesireNextYear',
                'SocialMedia','Gender']

stock_overflow_df_selected = stock_overflow_df.select(column_select)

country_filt = f.col('country') == 'India'




# COMMAND ----------

#stock_overflow_df_selected.write.format('parquet').mode("Overwrite").partitionBy('country').save("dbfs:/mnt/suankistoragedatalakes/processed/stockOverflowSurvey2019")

# COMMAND ----------

stock_overflow_df_selected.filter(country_filt).write.format('parquet').mode("overwrite").save("dbfs:/mnt/suankistoragedatalakes/processed/indiasurvey")

# COMMAND ----------

stock_overflow_df_selected.createOrReplaceTempView('india_data_temp')

# COMMAND ----------

stock_overflow_df_selected.filter(country_filt).write.format('csv').option('header',True).option('sep',',')\
.save("dbfs:/mnt/suankistoragedatalakes/processed/indiasurvery_2019")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists overflow;
# MAGIC use overflow;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS indiasurvery_2019;
# MAGIC create table indiasurvery_2019(
# MAGIC employment string,
# MAGIC Country string,
# MAGIC edlevel string,
# MAGIC yearscode string,
# MAGIC JobSat string,
# MAGIC CareerSat string,
# MAGIC CurrencySymbol string,
# MAGIC ConvertedComp string,
# MAGIC DatabaseWorkedWith string,
# MAGIC LanguageDesireNextYear string,
# MAGIC SocialMedia string,
# MAGIC Gender string
# MAGIC )
# MAGIC using csv
# MAGIC options(
# MAGIC path = "dbfs:/mnt/suankistoragedatalakes/processed/indiasurvery_2019",
# MAGIC sep = ',',
# MAGIC header = True
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in overflow

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from indiasurvery_2019

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from indiasurvery_2019 limit 10

# COMMAND ----------


