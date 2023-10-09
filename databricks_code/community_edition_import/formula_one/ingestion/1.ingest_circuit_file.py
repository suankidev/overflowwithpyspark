# Databricks notebook source
# MAGIC %md
# MAGIC ###ingest circuit file
# MAGIC 1. read the csv file using the spark dataframe reader
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import StructField, StructType,StringType, IntegerType, DoubleType



# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuit_df=spark.read.option('header',True).schema(circuits_schema).csv('/mnt/suankistoragedatalakes/raw/circuits.csv')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###select required column

# COMMAND ----------

column_select = ['circuitId','circuitRef','name','location','country','lat','lng','alt']
circuits_selected_df = circuit_df.select(column_select)

# COMMAND ----------

# MAGIC %md
# MAGIC ####rename the column
# MAGIC

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 


# COMMAND ----------

# MAGIC %md
# MAGIC #####add ingestion date as column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuit_final_df= circuits_renamed_df.withColumn('ingestion_date',current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to datalake as parquet

# COMMAND ----------

circuit_final_df.write.mode('overwrite').parquet("/mnt/suankistoragedatalakes/processed/circuits")

# COMMAND ----------

display(circuit_final_dfcircuits_renamed_df)

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/suankistoragedatalakes/processed/circuits

# COMMAND ----------

df = spark.read.parquet('/mnt/suankistoragedatalakes/processed/circuits')

# COMMAND ----------

display(df)
