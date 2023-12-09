// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ##Definition:
// MAGIC an RDD represents an immutable, partitioned collection of records that can be operated
// MAGIC on in parallel. Unlike DataFrames though, where each record is a structured row containing
// MAGIC fields with a known schema, in RDDs the records are just Java, Scala, or Python objects of the
// MAGIC programmer’s choosing.
// MAGIC
// MAGIC Let’s formally define RDDs. Internally, each RDD is characterized by five main properties:
// MAGIC
// MAGIC *A list of partitions
// MAGIC
// MAGIC *A function for computing each split
// MAGIC
// MAGIC *A list of dependencies on other RDDs
// MAGIC
// MAGIC *Optionally, a Partitioner for key-value RDDs (e.g., to say that the RDD is hashpartitioned)
// MAGIC
// MAGIC *Optionally, a list of preferred locations on which to compute each split (e.g., block
// MAGIC
// MAGIC *locations for a Hadoop Distributed File System [HDFS] file)

// COMMAND ----------

import org.apache.spark.sql.Row


 val row = Row(1, true, "a string", null)  // row: Row = [1,true,a string,null]
 val firstValue = row(0)  // firstValue: Any = 1
 val fourthValue = row(3)  // fourthValue: Any = null

 val firstValueBypremtiveAccess = row.getInt(0)   // firstValue: Int = 1
 val isNull = row.isNullAt(3)    // isNull: Boolean = true


 


// COMMAND ----------

// MAGIC %sql
// MAGIC desc retail_db.orders

// COMMAND ----------

spark.sql("SELECT order_id, order_status FROM retail_db.orders").limit(5).rdd.map{
  case Row(key:Int,value:String) => key
}.collect

// COMMAND ----------

//can be access using match
spark.sql("SELECT order_id, order_status FROM retail_db.orders").rdd.map {
   case Row(key: Int, value: String) =>  key -> value
 }.toDF().show(2)

// COMMAND ----------

val fullName = Row("Sujeet","Kumar","Singh")

//creating row from seq

val SeqOfData = scala.collection.mutable.Seq("This","is" ,"Test" ,"Sequence", null, 1, 'c')

val RowOfSequence = Row.fromSeq(SeqOfData)

 println(fullName.toSeq) //WrappedArray(1, true, a string, null)



// COMMAND ----------

println(fullName(0)) //sujeet
println(fullName.getString(0)) //
println(RowOfSequence.getAs[String](2))

// COMMAND ----------

spark.sql("SELECT order_id, order_status FROM retail_db.orders").rdd.map{
   case Row(key: Int, value: String) =>  Row(key -> value)
 }.filter(row => ! row.anyNull).count()  //anyNull to test if there are null

// COMMAND ----------

// MAGIC %sql
// MAGIC show tables 

// COMMAND ----------



// COMMAND ----------

val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
.split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)


// COMMAND ----------

words.collect()

// COMMAND ----------

words.distinct().count()

// COMMAND ----------

words.filter(row => row.startsWith("S")).collect()  //filter

// COMMAND ----------

words.toDF("somVal").show()

// COMMAND ----------

val mapWord = words.map(row => (row(0), row, row.startsWith("S")))
mapWord.collect()

// COMMAND ----------

words.map(row => Row(row(0), row, row.startsWith("S"))).map{
  row => row.getAs[Any](0) == 'S'
}.collect()

// COMMAND ----------

mapWord.filter(row => row._3 == true).collect() //tuple of 3

// COMMAND ----------

//flatMap
words.flatMap(row => row).collect()

// COMMAND ----------

//sortBy

words.sortBy(row => row.length() * -1).collect()

// COMMAND ----------

words.collect

// COMMAND ----------

val wordKeyby = words.map(row => (row.toLowerCase, 1))
val keywords = words.keyBy(row => row(0))

// COMMAND ----------

keywords.collect()

// COMMAND ----------

keywords.mapValues(row => row.toUpperCase).collect()

// COMMAND ----------

keywords.flatMapValues(word => word.toUpperCase).collect()

// COMMAND ----------

keywords.lookup('S')

// COMMAND ----------

val chars = words.flatMap(word => word.toLowerCase.toSeq)
chars.collect

// COMMAND ----------

val KVcharacters = chars.map(letter => (letter, 1))
KVcharacters.countByKey()

// COMMAND ----------

KVcharacters.groupByKey().map(row => (row._1, row._2.reduce((x,y) => x+y))).collect()

// COMMAND ----------

words.collect()

// COMMAND ----------

words.map(row => (row, 1)).reduceByKey((x,y) => x+y).collect()

// COMMAND ----------

words.map(row => (row, 1)).reduceByKey((x,y) => x+y).filter(row => row._1 == "Spark").collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ###Exercise on RDD

// COMMAND ----------

// MAGIC %fs
// MAGIC
// MAGIC ls dbfs:/FileStore/tables/suankiData/retail-data/

// COMMAND ----------

// MAGIC %fs
// MAGIC head dbfs:/FileStore/tables/suankiData/retail-data/2010_12_08.csv

// COMMAND ----------

Q1.  load retail data in df and save the data using partition by country and stockcode
Q2.  write logic to  partition column of retailer table
Q2.  load the retailer data as dwrite the data partition by country and 

// COMMAND ----------

// MAGIC %scala
// MAGIC
// MAGIC val retailDF = spark.read.option("header","true").csv("dbfs:/FileStore/tables/suankiData/retail-data/*")
// MAGIC
// MAGIC retailDF.count()
// MAGIC
// MAGIC //saving as
// MAGIC
// MAGIC retailDF.write.mode("overwrite").partitionBy("Country","StockCode").saveAsTable("retail_db.retailer")
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC describe formatted retail_db.retailer

// COMMAND ----------

// MAGIC %fs
// MAGIC
// MAGIC ls dbfs:/user/hive/warehouse/retail_db.db/retailer/Country=Australia

// COMMAND ----------

// MAGIC %scala
// MAGIC //Q2.  write logic to  find the partition column of retailer table
// MAGIC val retailSql = spark.sql("desc retail_db.retailer")
// MAGIC

// COMMAND ----------

retailerRdd.show(false)

// COMMAND ----------

val col_list = retailSql.select("col_name").collect
.map(_.getAs[String](0))
.dropWhile(row => !row.matches("# col_name"))

col_list.tail.toSet


// COMMAND ----------


