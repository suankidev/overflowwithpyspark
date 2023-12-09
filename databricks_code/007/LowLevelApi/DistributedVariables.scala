// Databricks notebook source
spark

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the Resilient Distributed Dataset (RDD) interface, the second kind of low-level
// MAGIC API in Spark is two types of “distributed shared variables”:
// MAGIC      These  variables you can use in your user-defined functions (e.g., in a map
// MAGIC function on an RDD or a DataFrame) that have special properties when running on a cluster.
// MAGIC
// MAGIC 1. **broadcast variables**: broadcast variables let you save a large value on all the worker nodes and reuse it across
// MAGIC many Spark actions without re-sending it to the cluster. 
// MAGIC       Broadcast variables are shared, immutable variables that are cached on every machine in the cluster instead of serialized with every single task. 
// MAGIC
// MAGIC 2. **accumulators**:  accumulators let you add declare a variable and shared across the machine so that updating a
// MAGIC value inside of a variety of transformations and propagating that value to the driver node in an
// MAGIC efficient and fault-tolerant way. (e.g.,
// MAGIC to implement a counter so you can see how many of your job’s input records failed to parse)
// MAGIC
// MAGIC   Accumulators provide a mutable variable that a Spark cluster can safely update on a per-row
// MAGIC basis. 

// COMMAND ----------

// MAGIC %fs
// MAGIC
// MAGIC ls dbfs:/FileStore/tables/suankiData/flight_data

// COMMAND ----------

// MAGIC %md
// MAGIC ###Accumulators
// MAGIC
// MAGIC // val accUnnamed = new LongAccumulator
// MAGIC
// MAGIC // val acc = spark.sparkContext.register(accUnnamed)
// MAGIC

// COMMAND ----------



case class Flight(DEST_COUNTRY_NAME: String,
ORIGIN_COUNTRY_NAME: String, count: BigInt)

val flights = spark.read
.format("csv")
.option("header",true).option("inferSchema",true).load("dbfs:/FileStore/tables/suankiData/flight_data")
.as[Flight]

//Now let’s create an accumulator that will count the number of flights to or from China.
import org.apache.spark.util.LongAccumulator

// val accUnnamed = new LongAccumulator
// val acc = spark.sparkContext.register(accUnnamed)
val accChina = spark.sparkContext.longAccumulator("China")



// COMMAND ----------

def accChinaFunc(flight_row: Flight) = {
          val destination = flight_row.DEST_COUNTRY_NAME
          val origin = flight_row.ORIGIN_COUNTRY_NAME
          if (destination == "China") {
              accChina.add(flight_row.count.toLong)
          } 
          if (origin == "China") {
               accChina.add(flight_row.count.toLong)
          }
}

flights.foreach(flight_row => accChinaFunc(flight_row))

accChina.value 

// COMMAND ----------

// MAGIC %md
// MAGIC ###broadcast

// COMMAND ----------

/**
broadcast variables:
**/

val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
.split(" ")

//if you will not give parallelization 2 then spark.sparkContext.defaultParallelism will be  used
val words = spark.sparkContext.parallelize(myCollection, 2)   //Array(Spark, The, Definitive, Guide, :, Big, Data, Processing, Made, Simple)

//broadcasting variables for lookup
val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,"Big" -> -300, "Simple" -> 100)
val suppBroadcast = spark.sparkContext.broadcast(supplementalData)



// COMMAND ----------

/*we could transform our RDD using this value. In this instance, we will create a key–value
pair according to the value we might have in the map*/

//transformation


words.map(row => (row, suppBroadcast.value.getOrElse(row,0))).collect() 
//Array[(String, Int)] = Array((Spark,1000), (The,0), (Definitive,200), (Guide,0), (:,0), (Big,-300), (Data,0), (Processing,0), (Made,0), (Simple,100))



// COMMAND ----------

//This method is accessible within serialized functions
suppBroadcast.value

// COMMAND ----------

// MAGIC %md
// MAGIC ###Custom Accumulator

// COMMAND ----------

// in Scala
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.AccumulatorV2


val arr = ArrayBuffer[BigInt]()


class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
      private var num:BigInt = 0
      
      def reset(): Unit = {
          this.num = 0
      } 
      
      def add(intValue: BigInt): Unit = {
          if (intValue % 2 == 0) {
      this.num += intValue
        }
      } 
      def merge(other: AccumulatorV2[BigInt,BigInt]): Unit = {
          this.num += other.value
      } 
      def value():BigInt = {
         this.num
      } 
      def copy(): AccumulatorV2[BigInt,BigInt] = {
         new EvenAccumulator
      } 
      def isZero():Boolean = {
         this.num == 0
      }
} 

val acc = new EvenAccumulator
val newAcc = sc.register(acc, "evenAcc")

acc.value 

flights.foreach(flight_row => acc.add(flight_row.count))
acc.value 

// COMMAND ----------

// MAGIC %md
// MAGIC #Exercise to test knowledge

// COMMAND ----------



// COMMAND ----------

val flightRdd = spark.sparkContext.textFile("dbfs:/FileStore/tables/suankiData/flight_data/2011_summary.csv")
val firstRow = flightRdd.first

val finalflightRdd = flightRdd.filter(row => row !=firstRow && !row.trim.contains("Eustatius")).map(row => row.split(","))

// COMMAND ----------


val usCount = spark.sparkContext.longAccumulator("Test")
usCount.value           


finalflightRdd
.foreach(row => {
usCount.add(row(2).toLong)
})

 usCount.value



// COMMAND ----------

val myMappedValue = Map("United States" -> "Business Class", "China" -> "Economic class", "other" -> "Lower Class")

val myMappedValueBr = spark.sparkContext.broadcast(myMappedValue)

finalflightRdd.map(row => (row(0), row(1), row(2), myMappedValueBr.value.getOrElse(row(0), "Lower class"))).take(5)


