// Databricks notebook source
// MAGIC %md
// MAGIC **Q1. count the words in the sample file **

// COMMAND ----------

// MAGIC %fs
// MAGIC
// MAGIC head dbfs:/FileStore/tables/suankiData/sample_text.txt

// COMMAND ----------

val sampleTxt = spark.sparkContext.textFile("dbfs:/FileStore/tables/suankiData/sample_text.txt")

sampleTxt.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x+y).filter(row => row._1 ==  "happened").collect


// COMMAND ----------

//find the largest string
sampleTxt.flatMap(x => x.split(" ")).reduce( (x,y) => if(x.length > y.length) x else y )

// COMMAND ----------

sampleTxt.flatMap(x => x.split(" ")).map(rowObject => (rowObject, 1)).reduceByKey((x,y) => x+y).collect

// COMMAND ----------

//how cogroup works, rquire pair rdd

val rdd2 = spark.sparkContext.parallelize(1 to 10, 2).map(row => (row, row*2))
val rdd3 = spark.sparkContext.parallelize(10 to 20, 2).map(row => (row, row*3))
val rdd4 = spark.sparkContext.parallelize(20 to 30, 2).map(row => (row, row*4))

rdd2.cogroup(rdd3, rdd4).collect

// COMMAND ----------

//use case class in rdd



val fligtRdd = spark.sparkContext.textFile("dbfs:/FileStore/tables/suankiData/flightData")

fligtRdd.map(row => row.split(",")).map(row => (row(0),row(1),row(2)))
.map{
 case (a:String, b:String, c:String) => (a,b,c)
}.collect


