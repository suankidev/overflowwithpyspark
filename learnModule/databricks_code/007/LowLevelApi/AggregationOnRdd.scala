// Databricks notebook source
// MAGIC %run ./commonRdd

// COMMAND ----------

// MAGIC %md
// MAGIC ###key-value pair
// MAGIC *Whenever you
// MAGIC see ByKey in a method name, it means that you can perform this only on a PairRDD type. The
// MAGIC easiest way is to just map over your current RDD to a basic key–value structure. This means
// MAGIC having two values in each record of your RDD*
// MAGIC

// COMMAND ----------

val wordsKP = words.map(rowObject => (rowObject(0),rowObject))
wordsKP.collect

// COMMAND ----------

//keyBy
words.keyBy(rowObject => rowObject(0)).collect()

// COMMAND ----------

//Mapping over Values

val keyword = words.keyBy(row => row(0))
keyword.mapValues(row => row.toUpperCase).collect

// COMMAND ----------

//flatMapValue
keyword.flatMapValues(row => row.toUpperCase).collect

// COMMAND ----------

//Extracting Keys and Values
keyword.keys.collect() 
keyword.values.collect()

// COMMAND ----------

//lookup: lookup for pariticular keys

keyword.lookup('S')  //keyword.lookup('S').length

// COMMAND ----------

// MAGIC %md
// MAGIC ###Aggregation
// MAGIC *You can perform aggregations on plain RDDs or on PairRDDs, depending on the method that
// MAGIC you are using*

// COMMAND ----------

val chars = words.flatMap(word => word.toLowerCase.toSeq)
val KVcharacters = chars.map(letter => (letter, 1))
val nums = sc.parallelize(1 to 30, 5)



// COMMAND ----------

KVcharacters.countByKey

// COMMAND ----------

// MAGIC %md
// MAGIC ####Understanding Aggregation Implementations
// MAGIC doing the same thing with groupByKey and reduceByKye
// MAGIC
// MAGIC 1. reduceByKye: *implementation is much more stable because the reduce happens within each partition and
// MAGIC doesn’t need to put everything in memory. Additionally, there is no incurred shuffle during this
// MAGIC operation; everything happens at each worker individually before performing a final reduce.*
// MAGIC 2. groupByKey:  *each executor must hold all values for a given key in memory
// MAGIC before applying the function to them. Why is this problematic? If you have massive key skew,
// MAGIC some partitions might be completely overloaded with a ton of values for a given key, and you
// MAGIC will get OutOfMemoryErrors.*
// MAGIC

// COMMAND ----------

KVcharacters.groupByKey().map(row => (row._1, row._2.reduce((x,y) => x+y))).collect()

// COMMAND ----------

KVcharacters.reduceByKey((x,y) => x+y).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ###CoGroup
// MAGIC *CoGroups give you the ability to group together up to three key–value RDDs together in Scala*

// COMMAND ----------


import scala.util.Random
val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct

val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))

charRDD.cogroup(charRDD2, charRDD3).take(5)

// COMMAND ----------

//Zips -> to zip two rdd

val numRange = sc.parallelize(0 to 17, 2)
words.zip(numRange).collect

// COMMAND ----------

// MAGIC %md
// MAGIC ####Join
// MAGIC RDDs have much the same joins as we saw in the Structured API

// COMMAND ----------

val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
val outputPartitions = 10
KVcharacters.join(keyedChars).collect    //96
KVcharacters.join(keyedChars, outputPartitions).collect

// COMMAND ----------

// MAGIC %md
// MAGIC ###Custom Partitioning

// COMMAND ----------

// in Scala
import org.apache.spark.Partitioner
class DomainPartitioner extends Partitioner {
def numPartitions = 3
def getPartition(key: Any): Int = {
val customerId = key.asInstanceOf[Double].toInt
if (customerId == 17850.0 || customerId == 12583.0) {
return 0
} else {
return new java.util.Random().nextInt(2) + 1
}
}
} k
eyedRDD
.partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
.take(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ###cutom Serilizer

// COMMAND ----------

// spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

// COMMAND ----------

class SomeClass extends Serializable {
var someValue = 0
def setSomeValue(i:Int) = {
someValue = i
this
}
} 
sc.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num))

// COMMAND ----------


