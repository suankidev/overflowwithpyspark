# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC data = [
# MAGIC   {"employeeId": 1234,
# MAGIC    "sales": 10000, 
# MAGIC    "number":Row(phone=654464,mobile=54534135), 
# MAGIC    "friends":["Suraj","ramesh","kamesh"],
# MAGIC    "contacts":{"mobile": "+1 234 567 8901", "home": "+1 234 567 8911"}
# MAGIC    }, 
# MAGIC   
# MAGIC   {"employeeId": 3232,
# MAGIC     "sales": 30000,
# MAGIC     "number":Row(phone=654464,mobile=54534135),
# MAGIC     "friends":["Suraj","ramesh","kamesh"],
# MAGIC     "contacts":{"mobile": "+1 234 567 8901", "home": "+1 234 567 8911"}
# MAGIC    }
# MAGIC ]
# MAGIC
# MAGIC df = spark.createDataFrame(data)
# MAGIC
# MAGIC

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create  temp view sales as 
# MAGIC select 1 as batchId ,
# MAGIC 	from_json('[{ "employeeId":1234,"sales" : 10000 },{ "employeeId":3232,"sales" : 30000 }]',
# MAGIC          'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
# MAGIC   current_timestamp() as insertDate
# MAGIC union all 
# MAGIC select 2 as batchId ,
# MAGIC   from_json('[{ "employeeId":1235,"sales" : 10500 },{ "employeeId":3233,"sales" : 32000 }]',
# MAGIC                 'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
# MAGIC                 current_timestamp() as insertDate

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select performance.employeeId from sales

# COMMAND ----------

# MAGIC %md
# MAGIC column performance  is array of struct type

# COMMAND ----------

import pyspark.sql.functions as f
spark.sql("select * from sales").select( f.collect_set(f.col("performance"))).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended sales

# COMMAND ----------

spark.table("sales").printSchema()

# COMMAND ----------

spark.table("sales").select("performance","performance.sales").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select collect_list(performance.sales)  from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select flatten( collect_list(performance.sales) ) from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select aggregate(flatten( collect_list(performance.sales)),0, (x,y) -> x +y )  as totalSale from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select reduce(flatten( collect_list(performance.sales)),0, (x,y) -> x +y )  as totalSale from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select explode(performance) as t  from sales) select  t.sales from cte

# COMMAND ----------

spark.sql("select * from sales").collect()

# COMMAND ----------

spark.sql("select * from sales").collect()[0]["performance"][0]

# COMMAND ----------

# MAGIC
# MAGIC %python
# MAGIC #test
# MAGIC #https://sparkbyexamples.com/pyspark/pyspark-json-functions-with-examples/
# MAGIC
# MAGIC
# MAGIC from pyspark.sql import SparkSession,Row
# MAGIC from pyspark.sql.functions import col
# MAGIC jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
# MAGIC df=spark.createDataFrame([(1, jsonString)],["id","value"])
# MAGIC # df.show(truncate=False)
# MAGIC
# MAGIC #Convert JSON string column to Map type
# MAGIC from pyspark.sql.types import MapType,StringType
# MAGIC from pyspark.sql.functions import from_json
# MAGIC df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))
# MAGIC df2.printSchema()
# MAGIC # df2.withColumn("test",col("value").getField("Zipcode")).show(truncate=False)
# MAGIC
# MAGIC # from pyspark.sql.functions import to_json,col
# MAGIC # df2.withColumn("value",to_json(col("value"))) \
# MAGIC #    .show(truncate=False)
# MAGIC
# MAGIC # from pyspark.sql.functions import json_tuple
# MAGIC # df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")) \
# MAGIC #     .toDF("id","Zipcode","ZipCodeType","City") \
# MAGIC #     .show(truncate=False)
# MAGIC
# MAGIC # from pyspark.sql.functions import get_json_object
# MAGIC # df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
# MAGIC #     .show(truncate=False)
# MAGIC
# MAGIC # from pyspark.sql.functions import schema_of_json,lit
# MAGIC # schemaStr=spark.range(1) \
# MAGIC #     .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""))) \
# MAGIC #     .collect()[0][0]
# MAGIC # print(schemaStr)
# MAGIC

# COMMAND ----------

# diffrence b/w straming live and live table
#
#

# COMMAND ----------

for i in dbutils.fs.ls("dbfs:/FileStore/tables/suankiData/activity_data/"):
  print(i.path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create streaming live table test as select * from csv.`dbfs:/FileStore/tables/suankiData/activity_data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC create  live table test as select * from csv.`dbfs:/FileStore/tables/suankiData/activity_data/`

# COMMAND ----------

new_dataframe_name = _sqldf

# COMMAND ----------

new_dataframe_name.printSchema()
