// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ##Vectorization in spark and hive
// MAGIC
// MAGIC -> process batch of records instead of row
// MAGIC -> less cpu cycles
// MAGIC -no object inspections
// MAGIC -> better machine code-smid
// MAGIC --> work with columner format
// MAGIC
// MAGIC set hive.vectorized.execution.enabled = true;

// COMMAND ----------

// MAGIC %md
// MAGIC ##spark memory management
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

Executor memory(1gb 300mb) =  reserved memory (300mb) + (user memory(25 % of 1 gb = 250mb)   +  spark memory fraction )1 GB

spark memory fraction = (execution memory + storage memory(caching or broadcast))

Note:
user memory holds  object created by user
spark.memory.fraction

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ## know you hardware and network

// COMMAND ----------



// COMMAND ----------

// MAGIC %md 
// MAGIC ###minimize data scans (lazy load)
// MAGIC -partitions
// MAGIC
// MAGIC -filter the data before joins
// MAGIC
// MAGIC -spark partitons type
// MAGIC
// MAGIC   1. input:    control-size
// MAGIC        1. spark.default.parallelist(  don't use)
// MAGIC        2. #######spark.sql.files.maxParitionBytes (mutable) 
// MAGIC   2. shuffle
// MAGIC        1. spark.sql.shuffle.partiions
// MAGIC   3. output
// MAGIC      1. coalesce(n)
// MAGIC      2. repartions(n)
// MAGIC      df.writ.opiotn("maxRecordsPerFile",N)
// MAGIC

// COMMAND ----------

// MAGIC %md 
// MAGIC ###Calculate shuffle partitons
// MAGIC 1. largest shuffle stage
// MAGIC      - targe size < = 200mb/partitions
// MAGIC partition count = stage input data/ targe size
// MAGIC   -- solve for partition count
// MAGIC
// MAGIC
// MAGIC   Example
// MAGIC
// MAGIC   shuffle stage input = 210 GB
// MAGIC
// MAGIC   x = 210000 MB / 200 MB = 1050
// MAGIC
// MAGIC   spark.conf.set("spark.sql.shuggle.partions",1050)
// MAGIC   but if cluster has 2000 cores
// MAGIC
// MAGIC   then shuffle.partions  to 2000
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

suppose there are spills -> 54 GB shuffle writes

target shuffle part sizze === 100m

p = 54gb /100m = 540

so should we set shuffle.part  to 540

suppose we have 96 corees   ===> 540p/96 = 5.625

96 * 5 = 480

no , we should set 480, b/c at a tiime 480 partition can be processed
