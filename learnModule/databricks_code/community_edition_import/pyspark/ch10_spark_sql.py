# Databricks notebook source
spark.sql("""create table test_flight (dest string, orgin string, count int) 
using csv options(header true, path '/FileStore/tables/data/fligt_data/2015_summary.csv')""")

# COMMAND ----------

spark.sql("""
create table test_partioned using csv partitioned by (dest) as select dest,orgin,count from test_flight
""")

# COMMAND ----------

spark.sql("""select * from test_partioned where dest='India'""").show()

# COMMAND ----------

spark.sql("select current_date").show()
df=spark.read.csv("/FileStore/tables/data/retail-data",header=True,inferSchema=True)
df.createOrReplaceTempView("flightView")

# COMMAND ----------

spark.sql("""
CREATE TABLE flights(DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG) 
USING CSV options(header true, path '/FileStore/tables/data/fligt_data/2015_summary.csv') 
""")

# COMMAND ----------

# MAGIC %fs rm -r /user/hive/warehouse/retailer_db.db

# COMMAND ----------

spark.sql("""CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights
""")

# COMMAND ----------

spark.sql("""drop table flights""")

# COMMAND ----------

# MAGIC %fs rm -r /user/hive/warehouse/

# COMMAND ----------

spark.sql("drop table flight_from_select")

# COMMAND ----------

spark.sql("create table if not exists flight_from_select as select * from flights")
"""
In this example, we are creating a Hive-compatible table because we did not explicitly specify the
format via USING. We can also do the following:
will create a hive table and have control over data and metadata
"""

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/tables/data/fligt_data/part_data

# COMMAND ----------

#spark.sql("create database test")
#spark.sql("""use test""")

# COMMAND ----------

spark.sql("""
create external table hive_flight_data (dest string,origin string , count int)
row format delimited 
fields terminated by ','
lines terminated by '\n'
location '/FileStore/tables/data/fligt_data/' """)

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/tables/data/fligt_data/

# COMMAND ----------

spark.sql("""
create  table hive_flight_data1 (dest string,origin string , count int)
row format delimited 
fields terminated by ','
lines terminated by '\n'
location '/FileStore/tables/data/fligt_data/' """)

# COMMAND ----------

spark.sql("select * from test.flights")

# COMMAND ----------

spark.sql("""
drop table test.hive_flight_data""")

# COMMAND ----------

from pyspark.sql.catalog import Catalog as c

spark.sql("drop table flights")

print(c.listDatabases)

# COMMAND ----------

spark.sql("select * from flights limit 10").show()

# COMMAND ----------

spark.sql("""
CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5
""")

# COMMAND ----------

spark.sql("""
drop table flights1""")

# COMMAND ----------

spark.sql("""
CREATE EXTERNAL TABLE hive_flights (
DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/FileStore/tables/data/fligt_data/'
""")

# COMMAND ----------

spark.sql("""
INSERT INTO flights_from_select
SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20
""")

# COMMAND ----------

spark.sql("""
INSERT INTO flights_from_select
SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20
""")

# COMMAND ----------

spark.sql("""
DESCRIBE TABLE flights
""").show()

# COMMAND ----------

spark.sql("""
DESCRIBE TABLE partitioned_flights
""").show()

# COMMAND ----------

spark.sql("""
REFRESH table partitioned_flights
""")

# COMMAND ----------

spark.sql("""
REFRESH table partitioned_flights
""")

# COMMAND ----------

spark.sql("""
CREATE VIEW just_usa_view AS
SELECT * FROM flights WHERE dest_country_name = 'United States'
""")

# COMMAND ----------


