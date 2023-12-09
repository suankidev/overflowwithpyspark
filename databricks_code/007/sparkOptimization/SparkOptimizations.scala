// Databricks notebook source
// MAGIC %run ../CommonUtils/SetupData

// COMMAND ----------

import org.apache.spark.sql.{functions => f}

// COMMAND ----------

// MAGIC %md
// MAGIC ####UnderStanding *explain*
// MAGIC
// MAGIC There are two type of Transformation.
// MAGIC
// MAGIC 1. **Narrow Transformation:**  one input memory partition contribute to one output memory partition 
// MAGIC
// MAGIC    *i.g:   filter/where, sortBy,*
// MAGIC    - select, withColumn , withColumnRenamed
// MAGIC    - map(), mapPartition(), flatMap(), filter(), union() , reduce()
// MAGIC
// MAGIC
// MAGIC 2. **Wide Transformation:** one input memory partition contribute to more than one output memory paritions
// MAGIC    
// MAGIC     - groupByKey() and reduceByKey() and  aggregateByKey(), aggregate(), join(), repartition() 
// MAGIC     - groupBy, count, distinct, aggregatefunciont on groupby
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC # [execution flow](FileStore/tables/suankiData/image/executionFlow.jpg)
// MAGIC
// MAGIC query -> unresolved logical plan -> metadata catalog -> resolved logical plan -> catalyst catalog -> optimized  logical plan
// MAGIC -> physical plan -> Cast based model -> selected physical plan --> RDD 
// MAGIC
// MAGIC AQE -> migth optimized the plan at runtime by going back again to "resolved logical plan" and doing again the same thing
// MAGIC
// MAGIC i.g:  by default spark shuffle creates 200 partiion but AQE might adjust the shuffle partition on runtime depends on input shuflle partion size(it can set to 2..or 3 ..or any other number n)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Performance Tuning
// MAGIC - Caching Data In Memory
// MAGIC - Other Configuration Options
// MAGIC - Join Strategy Hints for SQL Queries
// MAGIC - Coalesce Hints for SQL Queries
// MAGIC - Adaptive Query Execution
// MAGIC - Coalescing Post Shuffle Partitions
// MAGIC - Spliting skewed shuffle partitions
// MAGIC - Converting sort-merge join to broadcast join
// MAGIC - Converting sort-merge join to shuffled hash join
// MAGIC - Optimizing Skew Join
// MAGIC - Misc
// MAGIC <a href="https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions">sparkDoc</a>
// MAGIC
// MAGIC <a href="https://engineering.salesforce.com/how-to-optimize-your-apache-spark-application-with-partitions-257f2c1bb414/">Optimizing the partitions</a>
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC
// MAGIC # Partitioning
// MAGIC ##### Objectives
// MAGIC 1. Get partitions and cores
// MAGIC 1. Repartition DataFrames
// MAGIC 1. Configure default shuffle partitions
// MAGIC
// MAGIC ##### Methods
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`repartition`**, **`coalesce`**, **`rdd.getNumPartitions`**
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkConf.html" target="_blank">SparkConf</a>: **`get`**, **`set`**
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html" target="_blank">SparkSession</a>: **`spark.sparkContext.defaultParallelism`**
// MAGIC
// MAGIC ##### SparkConf Parameters
// MAGIC - **`spark.sql.shuffle.partitions`**, **`spark.sql.adaptive.enabled`**

// COMMAND ----------

//setting to default parallelism , assuming larg datasets
spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
print(spark.conf.get("spark.sql.shuffle.partitions"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC
// MAGIC ### Partitioning Guidelines
// MAGIC - Make the number of partitions a multiple of the number of cores
// MAGIC - Target a partition size of ~200MB
// MAGIC - Size default shuffle partitions by dividing largest shuffle stage input by the target partition size (e.g., 4TB / 200MB = 20,000 shuffle partition count)
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> When writing a DataFrame to storage, the number of DataFrame partitions determines the number of data files written. (This assumes that <a href="https://sparkbyexamples.com/apache-hive/hive-partitions-explained-with-examples/" target="_blank">Hive partitioning</a> is not used for the data in storage. A discussion of DataFrame partitioning vs Hive partitioning is beyond the scope of this class.)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC
// MAGIC # Query Optimization
// MAGIC
// MAGIC We'll explore query plans and optimizations for several examples including logical optimizations and exanples with and without predicate pushdown.
// MAGIC
// MAGIC ##### Objectives
// MAGIC 1. Logical optimizations
// MAGIC 1. Predicate pushdown
// MAGIC 1. No predicate pushdown
// MAGIC
// MAGIC ##### Methods 
// MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.explain.html#pyspark.sql.DataFrame.explain" target="_blank">DataFrame</a>: **`explain`**

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC
// MAGIC ### Adaptive Query Execution
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/aspwd/partitioning_aqe.png" width="60%" />
// MAGIC
// MAGIC In Spark 3, <a href="https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution" target="_blank">AQE</a> is now able to <a href="https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html" target="_blank"> dynamically coalesce shuffle partitions</a> at runtime. This means that you can set **`spark.sql.shuffle.partitions`** based on the largest data set your application processes and allow AQE to reduce the number of partitions automatically when there is less data to process.
// MAGIC
// MAGIC The **`spark.sql.adaptive.enabled`** configuration option controls whether AQE is turned on/off.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Adaptive query optimizaton
// MAGIC - spark.conf.set("spark.sql.adaptive.enabled",true)
// MAGIC - spark.conf.set("spark.sql.adaptive.coalescePartitions",true)
// MAGIC - spark.conf.set("spark.databricks.io.cache.enabled",true)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Cost based optimization
// MAGIC //cast based model
// MAGIC
// MAGIC - spark.conf.set("spark.sql.cbo.enabled",true)
// MAGIC - spark.conf.set("spark.sql.cbo.joinReorder.enabled",true)
// MAGIC - spark.conf.set("spark.sql.cbo.histogram.enabled",true)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC
// MAGIC ### Caching
// MAGIC
// MAGIC By default the data of a DataFrame is present on a Spark cluster only while it is being processed during a query -- it is not automatically persisted on the cluster afterwards. (Spark is a data processing engine, not a data storage system.) You can explicity request Spark to persist a DataFrame on the cluster by invoking its **`cache`** method.
// MAGIC
// MAGIC If you do cache a DataFrame, you should always explictly evict it from cache by invoking **`unpersist`** when you no longer need it.
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/icon_best_32.png" alt="Best Practice"> Caching a DataFrame can be appropriate if you are certain that you will use the same DataFrame multiple times, as in:
// MAGIC
// MAGIC - Exploratory data analysis
// MAGIC - Machine learning model training
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Aside from those use cases, you should **not** cache DataFrames because it is likely that you'll *degrade* the performance of your application.
// MAGIC
// MAGIC - Caching consumes cluster resources that could otherwise be used for task execution
// MAGIC - Caching can prevent Spark from performing query optimizations, as shown in the next example

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC
// MAGIC ### Predicate Pushdown
// MAGIC
// MAGIC Here is example reading from a JDBC source, where Catalyst determines that *predicate pushdown* can take place.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Handling Data Skewness 
// MAGIC
// MAGIC
// MAGIC Although data skewness can impact your Spark application’s performance significantly, several strategies can help manage and mitigate this issue:
// MAGIC
// MAGIC 1. **Custom Partitioning:** Instead of relying on Spark’s default partitioning strategy, implementing a custom partitioning strategy can help distribute data more evenly across partitions. For example, range partitioning can be more effective when dealing with numeric keys.
// MAGIC
// MAGIC 2. **Salting:** Salting is a technique where a random value (salt) is appended to the key, which helps distribute the data more evenly across partitions. This can be particularly useful when dealing with hot keys.
// MAGIC
// MAGIC 3. **Dynamic Partition Pruning:** Dynamic partition pruning is a technique used in Spark to optimize join operations by skipping the scanning of irrelevant partitions in both datasets. This can help improve performance in the case of data skewness caused by join operations.
// MAGIC
// MAGIC 4. **Splitting Skewed Data:** Another strategy is to split the skewed data across multiple partitions. This involves identifying the skewed keys and redistributing the data associated with these keys.
// MAGIC
// MAGIC 5. **Avoid GroupBy for Large Datasets:** When possible, avoid using GroupBy operations on large datasets with non-unique keys. Alternatives such as reduceByKey, which performs a combine operation locally on each partition before performing the grouping operation, can be more efficient.

// COMMAND ----------

// MAGIC %md 
// MAGIC [dataSkewNewssProblemSolution](https://community.cloud.databricks.com/?o=2941430867364584#notebook/2227913417139108/command/2227913417139109)

// COMMAND ----------

// MAGIC %python
// MAGIC # Ensure that the driver class is loaded
// MAGIC class.forName("org.postgresql.Driver")
// MAGIC
// MAGIC jdbc_url = "jdbc:postgresql://server1.training.databricks.com/training"
// MAGIC
// MAGIC # Username and Password w/read-only rights
// MAGIC conn_properties = {
// MAGIC     "user" : "training",
// MAGIC     "password" : "training"
// MAGIC }
// MAGIC
// MAGIC pp_df = (spark
// MAGIC          .read
// MAGIC          .jdbc(url=jdbc_url,                 # the JDBC URL
// MAGIC                table="training.people_1m",   # the name of the table
// MAGIC                column="id",                  # the name of a column of an integral type that will be used for partitioning
// MAGIC                lowerBound=1,                 # the minimum value of columnName used to decide partition stride
// MAGIC                upperBound=1000000,           # the maximum value of columnName used to decide partition stride
// MAGIC                numPartitions=8,              # the number of partitions/connections
// MAGIC                properties=conn_properties    # the connection properties
// MAGIC               )
// MAGIC          .filter(col("gender") == "M")   # Filter the data by gender
// MAGIC         )
// MAGIC
// MAGIC pp_df.explain(True)
// MAGIC
// MAGIC
// MAGIC #Note the lack of a Filter and the presence of a PushedFilters in the Scan. The filter operation is pushed to the database and only #the matching records are sent to Spark. This can greatly reduce the amount of data that Spark needs to ingest.
// MAGIC
// MAGIC

// COMMAND ----------

cached_df = (spark
            .read
            .jdbc(url=jdbc_url,
                  table="training.people_1m",
                  column="id",
                  lowerBound=1,
                  upperBound=1000000,
                  numPartitions=8,
                  properties=conn_properties
                 )
            )

cached_df.cache()
filtered_df = cached_df.filter(col("gender") == "M")

filtered_df.explain(True)

/**
In addition to the Scan (the JDBC read) we saw in the previous example, here we also see the InMemoryTableScan followed by a Filter in the explain plan.

This means Spark had to read ALL the data from the database and cache it, and then scan it in cache to find the records matching the filter condition.**/

// COMMAND ----------

val df100 = spark.read.csv(DataPath.retailData).explain  //FileScan csv create one job

// COMMAND ----------

val df101 = spark.read.option("header",true).csv(DataPath.retailData)
//trigger one spark job


// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",false)
val retailDF = spark.read 
.option("header",true)
.option("inferScheam",true) 
.csv(DataPath.retailData)

df102.explain(false)
/*
total two job 
1. inferSchema 
2. csv load
*/

// COMMAND ----------


retailDF
.withColumn("date",f.current_date)  //PushedFilters: [IsNotNull(Country), EqualTo(Country,Germnay)]
.filter(f.col("country") === "Germnay")
.select("invoiceno", "stockcode","country")
.explain(false)

// COMMAND ----------

retailDF.rdd.getNumPartitions

// COMMAND ----------

retailDF.repartition(8).explain
/**
//
Exchange RoundRobinPartitioning(8), REPARTITION_BY_NUM, [plan_id=459]
   earlier        now
   part-1         part-1

   part-2         part-2

   ..             --
   ..             ..
   part-7         part-7
                  part-8


spark will take 1 row from part-1 will put in part-1
                2nd row from part-1 will put in part-2
                 ......................   part-3
                                          part-8
**/

// COMMAND ----------

retailDF.coalesce(1).explain

// COMMAND ----------

// MAGIC %md 
// MAGIC ###How Spark Performs Joins

// COMMAND ----------

// MAGIC %run /007/sparkOptimization/Joins

// COMMAND ----------

// MAGIC %md
// MAGIC ###How DAG?
// MAGIC [Link]("https://www.youtube.com/watch?v=O_45zAz1OGk&t=758s")
