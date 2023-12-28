// Databricks notebook source
// MAGIC %md
// MAGIC **`Question.  I have data 800 mb of data on hdfs for a dataset , what will be the memory partitions while reading into DF`**

// COMMAND ----------

// MAGIC %md
// MAGIC **`Question.  df2.join(df1).join(df3)  , How many Job and Task would get created`**

// COMMAND ----------

// MAGIC %md
// MAGIC **`Question. How many Job and Task would get created for below along with Steps involved in Catalyst optimizer -> CBO ->Adaptive query engine`**
// MAGIC
// MAGIC ---
// MAGIC jobs:
// MAGIC
// MAGIC 1. df.filter(f.col("city") === "Boston").filter(f.col("year") === "2018" and f.col("month").isin("11","12"))
// MAGIC 2. df.groupBy("city").agg(sum(f.col("amt")))
// MAGIC 3. df.repartion(8,col("city"))
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC **`Question.  I have data 800 mb of data on hdfs , what will be the memory partitions while reading into DF`**

// COMMAND ----------

// MAGIC %md
// MAGIC **`Question.  What is AQE, How does it works`**

// COMMAND ----------

// MAGIC %md
// MAGIC **`Question.  i have a table transaction and customer, with cust_id`**
// MAGIC
// MAGIC sample data
// MAGIC
// MAGIC ```+----------+----------+----------+---------------+----------+----+-----+---+-------------+------+-----------+
// MAGIC |cust_id   |start_date|end_date  |txn_id         |date      |year|month|day|expense_type |amt   |city       |
// MAGIC +----------+----------+----------+---------------+----------+----+-----+---+-------------+------+-----------+
// MAGIC |C0YDPQWPBJ|2010-07-01|2018-12-01|TZ5SMKZY9S03OQJ|2018-10-07|2018|10   |7  |Entertainment|10.42 |boston     |
// MAGIC |C0YDPQWPBJ|2010-07-01|2018-12-01|TYIAPPNU066CJ5R|2016-03-27|2016|3    |27 |Motor/Travel |44.34 |portland   |
// MAGIC ```
// MAGIC ---
// MAGIC 1. find the city wise  total expense  on  expense_type
// MAGIC 2. find year and city wise total expenses on diffrent expense type
// MAGIC 3. find year,month and city wise total expense on diffrent expense type
// MAGIC 4. which month of year has max expense
// MAGIC 5. which month of year has max expense on Entertainment

// COMMAND ----------



// COMMAND ----------

// MAGIC %fs
// MAGIC ls FileStore/tables/suankiData/

// COMMAND ----------

import org.apache.spark.sql.{functions => f}

val transactionDF = spark.read.parquet("dbfs:/FileStore/tables/suankiData/transaction.parquet/")
val customerDF = spark.read.parquet("dbfs:/FileStore/tables/suankiData/customer.parquet")



// COMMAND ----------

transactionDF.show(5,false)

// COMMAND ----------

customerDF.show(5,false)

// COMMAND ----------


