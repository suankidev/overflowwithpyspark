// Databricks notebook source
// MAGIC %md
// MAGIC There are times when higher-level manipulation will not meet the business or engineering problem you are trying to solve. For those cases, you
// MAGIC might need to use Spark’s lower-level APIs
// MAGIC
// MAGIC **There are two sets of low-level APIs**
// MAGIC 1. one for manipulating distributed data (RDDs)
// MAGIC 2. another for distributing and manipulating distributed shared variables (broadcast variables and
// MAGIC accumulators).
// MAGIC
// MAGIC
// MAGIC
// MAGIC **When to Use the Low-Level APIs?**
// MAGIC 1. need very tight control over physical data placement across the cluster/partioner/skewness
// MAGIC 2. maintain some legacy codebase 
// MAGIC 3. custome shared variable manupulation
// MAGIC
// MAGIC
// MAGIC **What is RDD?**
// MAGIC  RDD represents an immutable, partitioned collection of records that can be operated
// MAGIC on in parallel. Unlike DataFrames though, where each record is a structured row containing
// MAGIC fields with a known schema, in RDDs the records are just Java, Scala, or Python objects of the
// MAGIC programmer’s choosing.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC **each RDD is characterized by five main properties:**
// MAGIC 1. A list of partitions
// MAGIC 2. A function for computing each split
// MAGIC 3. A list of dependencies on other RDDs
// MAGIC 4. Optionally, a Partitioner for key-value RDDs (e.g., to say that the RDD is hashpartitioned)
// MAGIC 5. Optionally, a list of preferred locations on which to compute each split (e.g., block
// MAGIC locations for a Hadoop Distributed File System [HDFS] fil

// COMMAND ----------

// MAGIC %run ./commonRdd

// COMMAND ----------

rangeOfNumbers.toDF().rdd.map(rowObject => rowObject.getLong(0)).take(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Transformation on RDD
// MAGIC
// MAGIC 1. distinct: take the distinct value
// MAGIC 2. filter: filter the row
// MAGIC 3. map: *Specify a function that returns the value that you want, given the correct input. You then apply that, record by record*
// MAGIC 4. flatMap: *flatMap provides a simple extension of the map function we just looked at. Sometimes, each current row should return multiple rows*
// MAGIC 5. sortBy: words.sortBy(row => row.length * -1)
// MAGIC
// MAGIC ###Action
// MAGIC 1. collect: *collect the data on the driver*
// MAGIC 2. count:
// MAGIC 3. countByValue: *words.countByValue() You should use this method only if the resulting map is expected to be small because the entire thing is loaded into the driver’s memory.*
// MAGIC 3. reduce:  *words.reduce((x,y) => if(x.length > y.length) x else y)  //longest words*
// MAGIC 4. redcuceByKey: *require key.value pair
// MAGIC 5. *words.first()*
// MAGIC 6. min : *spark.sparkContext.parallelize(1 to 20).min()*
// MAGIC 7. max: *spark.sparkContext.parallelize(1 to 20).max()*
// MAGIC 8. words.saveAsTextFile("file:/tmp/bookTitle")
// MAGIC
// MAGIC

// COMMAND ----------

words.collect

// COMMAND ----------

words.distinct.count

// COMMAND ----------

//filter the RDD to keep only the words that begin with the letter “S”:
def LetterStartWith(abc:String):Boolean= abc.startsWith("S")

words.filter(rowObject => LetterStartWith(rowObject)).collect

// COMMAND ----------

val words2 = words.map(word => (word, word(0), word.startsWith("S")))

words2.filter(rowObject => rowObject._3 == true).take(5)

// COMMAND ----------

//flatMap
words.flatMap(row => row).collect

// COMMAND ----------

words.sortBy(row => row.length * -1).collect

// COMMAND ----------

val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

fiftyFiftySplit(1).collect()

// COMMAND ----------

words.reduce((x,y) => if(x.length > y.length) x else y)  //longest words

// COMMAND ----------

words.countByValue() ///scala.collection.Map[String,Long]

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ###Caching
// MAGIC
// MAGIC Using cache() and persist() methods, Spark provides an optimization mechanism to store the intermediate computation of an RDD, DataFrame, and Dataset so they can be reused in subsequent actions(reusing the RDD, Dataframe, and Dataset computation results).
// MAGIC  
// MAGIC   cache() method default saves it to memory (MEMORY_ONLY) 
// MAGIC   whereas persist() method is used to store it to the user-defined storage level. 
// MAGIC
// MAGIC 1) *persist() : Dataset.this.type*
// MAGIC 2) *persist(newLevel : org.apache.spark.storage.StorageLevel) : Dataset.this.type*
// MAGIC
// MAGIC
// MAGIC `
// MAGIC MEMORY_ONLY – **This is the default behavior of the RDD cache()** method and stores the RDD or DataFrame as deserialized objects to JVM memory. When there is no enough memory available it will not save DataFrame of some partitions and these will be re-computed as and when required. This takes more memory. but unlike RDD, this would be slower than MEMORY_AND_DISK level as it recomputes the unsaved partitions and recomputing the in-memory columnar representation of the underlying table is expensive
// MAGIC
// MAGIC MEMORY_ONLY_SER – This is the same as MEMORY_ONLY but the difference being it stores RDD as serialized objects to JVM memory. It takes lesser memory (space-efficient) then MEMORY_ONLY as it saves objects as serialized and takes an additional few more CPU cycles in order to deserialize.
// MAGIC
// MAGIC MEMORY_ONLY_2 – Same as MEMORY_ONLY storage level but replicate each partition to two cluster nodes.
// MAGIC
// MAGIC MEMORY_ONLY_SER_2 – Same as MEMORY_ONLY_SER storage level but replicate each partition to two cluster nodes.
// MAGIC
// MAGIC MEMORY_AND_DISK – **This is the default behavior of the DataFrame or Dataset**. In this Storage Level, The DataFrame will be stored in JVM memory as a deserialized object. When required storage is greater than available memory, it stores some of the excess partitions into the disk and reads the data from the disk when required. It is slower as there is I/O involved.
// MAGIC
// MAGIC MEMORY_AND_DISK_SER – This is the same as MEMORY_AND_DISK storage level difference being it serializes the DataFrame objects in memory and on disk when space is not available.
// MAGIC
// MAGIC MEMORY_AND_DISK_2 – Same as MEMORY_AND_DISK storage level but replicate each partition to two cluster nodes.
// MAGIC
// MAGIC MEMORY_AND_DISK_SER_2 – Same as MEMORY_AND_DISK_SER storage level but replicate each partition to two cluster nodes.
// MAGIC
// MAGIC DISK_ONLY – In this storage level, DataFrame is stored only on disk and the CPU computation time is high as I/O is involved.
// MAGIC
// MAGIC DISK_ONLY_2 – Same as DISK_ONLY storage level but replicate each partition to two cluster nodes.
// MAGIC `

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

val rdd1 = words.cache()

val rdd2 = words2.persist(StorageLevel.MEMORY_AND_DISK)

// COMMAND ----------

rdd2.getStorageLevel

// COMMAND ----------

println(words.getCheckpointFile,
words.getNumPartitions,
 words.getResourceProfile,
words.getStorageLevel)

// COMMAND ----------

// MAGIC %md
// MAGIC ###checkpoint
// MAGIC *Checkpointing
// MAGIC is the act of saving an RDD to disk so that future references to this RDD point to those
// MAGIC intermediate partitions on disk rather than recomputing the RDD from its original source. This is
// MAGIC similar to caching except that it’s not stored in memory, only disk.
// MAGIC *

// COMMAND ----------

spark.sparkContext.setCheckpointDir("dbfs:/FileStore/tables/suankiData/checkpointing")
words.checkpoint()

// COMMAND ----------

// MAGIC %fs
// MAGIC
// MAGIC ls dbfs:/FileStore/tables/suankiData/checkpointing

// COMMAND ----------

words.count()

// COMMAND ----------

words.pipe("wc -l").collect()  //Array(9, 9)  so total 18 line ..b/c it results per partitions 

// COMMAND ----------

// MAGIC %md
// MAGIC ####mapPartitions
// MAGIC *Spark operates on a per-partition basis when it comes to
// MAGIC actually executing code. You also might have noticed earlier that the return signature of a map
// MAGIC function on an RDD is actually MapPartitionsRDD. This is because map is just a row-wise alias
// MAGIC for mapPartitions, which makes it possible for you to map an individual partition (represented
// MAGIC as an iterator)*

// COMMAND ----------

//words.map(part => part)  //org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[394]


words.mapPartitions(part => Iterator[Int](1)).sum()

/**
Naturally, this means that we operate on a per-partition basis and allows us to perform an
operation on that entire partition.
**/

// COMMAND ----------

words.mapPartitions(part => part.map(row => (row, row.startsWith("S")))).collect  //doing same thing as  partition by partition

// COMMAND ----------

// in Scala
def indexedFunc(partitionIndex:Int, withinPartIterator: Iterator[String]) = {
      withinPartIterator.toList.map(
      value => s"Partition: $partitionIndex => $value").iterator
}

words.mapPartitionsWithIndex(indexedFunc).collect()



// COMMAND ----------

words.mapPartitionsWithIndex((indx,part) => part.toList.map(row => row).iterator).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC #####foreachPartition
// MAGIC *Although mapPartitions needs a return value to work properly, this next function does not.
// MAGIC foreachPartition simply iterates over all the partitions of the data. The difference is that the
// MAGIC function has no return value. This makes it great for doing something with each partition like
// MAGIC writing it out to a database*

// COMMAND ----------

// MAGIC
// MAGIC %fs
// MAGIC
// MAGIC ls dbfs:/FileStore/tables/suankiData/

// COMMAND ----------

val file = new java.io.File(raw"dbfs:/FileStore/tables/suankiData/temp/random.txt")

val pw = new java.io.PrintWriter(file)
pw.write("test_file")


// COMMAND ----------

val col= "this is test class for big data analytics".split(" ")
val words = spark.sparkContext.parallelize(col,2)

words.foreachPartition { iter =>
  import java.io._
  import scala.util.Random
   
  val randomFileName = new Random().nextInt()

  //  val pw = new PrintWriter(new File(s"dbfs:/FileStore/tables/suankiData/temp/random.txt"))
    val pw = new PrintWriter(new File(s"C:\\Users\\sujee\\OneDrive\\Documents\\bigdata_and_hadoop\\scala\\tmp\\random_$randomFileName.txt"))
   
  while (iter.hasNext) {
      pw.write(iter.next())
    } 

  pw.close()
}

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###glom
// MAGIC *glom is an interesting function that takes every partition in your dataset and converts them to
// MAGIC arrays. This can be useful if you’re going to collect the data to the driver and want to have an
// MAGIC array for each partition.*

// COMMAND ----------

words.glom.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ** org.apache.spark.sql.Row**

// COMMAND ----------

import org.apache.spark.sql.Row


 val row = Row(1, true, "a string", null)  // row: Row = [1,true,a string,null]
 val firstValue = row(0)  // firstValue: Any = 1
 val fourthValue = row(3)  // fourthValue: Any = null

 val firstValueBypremtiveAccess = row.getInt(0)   // firstValue: Int = 1
 val isNull = row.isNullAt(3)    // isNull: Boolean = true

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
