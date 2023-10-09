# Databricks notebook source
for i in dbutils.fs.ls("dbfs:/FileStore/tables/retail-data/"):
  print(i[0])
  
df = spark.read.format('csv').option('inferSchema',True).option('header',True)\
    .load("dbfs:/FileStore/tables/retail-data/2010_12_02.csv") 

#df.show(5, truncate=False)

df1 = spark.read.format('csv').option('inferSchema',True).option('header',True)\
    .load("dbfs:/FileStore/tables/retail-data/2010_12_01.csv") 








# COMMAND ----------

df.explain()

# COMMAND ----------

#spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", True)
from pyspark.sql.functions import col

df.write.format('parquet').mode('overwrite').partitionBy('Country')\
.save("dbfs:/FileStore/tables/output/retail_data")




# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/output/retail_data")

# COMMAND ----------

df3 = spark.read.format('parquet').load("dbfs:/FileStore/tables/output/retail_data")

df3.show()

# COMMAND ----------


#understanding the spark jobs


retail_df = spark.read.format('csv').option('inferSchema',True).option('header',True)\
    .load("dbfs:/FileStore/tables/retail-data/2010_12_02.csv") 




# COMMAND ----------

import pyspark.sql.functions as f
retail_df_grouped = (retail_df.filter('unitprice >= 5').groupBy('stockcode')
                     .agg(f.sum(f.col('quantity')).alias('quantity_sum'))
                    )




# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;
# MAGIC create database if not exists demo;
# MAGIC show databases;

# COMMAND ----------

retail_df_grouped.write.mode('overwrite').saveAsTable('demo.grouped_reatil_data')

# COMMAND ----------

retail_df_grouped.write.format('parquet').mode('overwrite').save('dbfs:/FileStore/tables/output/retail_data')

# COMMAND ----------

retaildf= spark.read.parquet('dbfs:/FileStore/tables/output/retail_data')

# COMMAND ----------

retaildf.show()

# COMMAND ----------

df1 = spark.range(107394739473979879)
df2 = spark.range(347937493749837498)



# COMMAND ----------

df1.union(df2).count()
