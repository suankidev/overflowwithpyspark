// Databricks notebook source
// MAGIC %run ../CommonUtils/SetupData

// COMMAND ----------

// MAGIC %md
// MAGIC ####Structured API  and Operations
// MAGIC 1. Datasets
// MAGIC 2. DataFrames
// MAGIC 3. Sql tabels and views

// COMMAND ----------

// MAGIC %md
// MAGIC What is Apache spark ?
// MAGIC
// MAGIC Apache Spark is a unified computing engine and a set of libraries for parallel data processing on computer clusters
// MAGIC
// MAGIC Unified ?
// MAGIC    
// MAGIC Spark is designed to support a wide range of data analytics tasks, ranging from simple data loading and SQL queries to machine learning and streaming computation, over the same computing engine and with a consistent set of APIs
// MAGIC
// MAGIC Computing engine ?
// MAGIC
// MAGIC we mean that Spark handles loading data from storage systems and performing computation on it, not permanent storage as the end itself. You can use Spark with a wide variety of persistent storage systems, including cloud storage systems
// MAGIC
// MAGIC Libraries ?
// MAGIC
// MAGIC structured data (Spark SQL), machine learning (MLlib), stream processing (Spark Streaming and the newer Structured Streaming), and graph analytics (GraphX)
// MAGIC
// MAGIC
// MAGIC A cluster ?
// MAGIC           A cluster, or group, of computers, pools the resources of many machines together, giving us the ability to use all the cumulative resources as if they were a single computer. 
// MAGIC The cluster of machines that Spark will use to execute tasks is managed by a cluster manager like Spark’s standalone cluster manager, YARN, or Mesos.
// MAGIC
// MAGIC
// MAGIC **Spark Applications ?**
// MAGIC
// MAGIC Spark Applications consist of a driver process and a set of executor processes. The driver process runs your main() function, sits on a node in the cluster, and is responsible for three things: maintaining information about the Spark Application; responding to a user’s program or input; and analyzing, distributing, and scheduling work across the executors (discussed momentarily).
// MAGIC
// MAGIC spark application : driver process and executors
// MAGIC driver process : spark Session and user code
// MAGIC spark session:  spark Session get initialized through driver processs..one to one b/w spark-appl
// MAGIC
// MAGIC
// MAGIC
// MAGIC Executors?
// MAGIC
// MAGIC The executors are responsible for actually carrying out the work that the driver assigns them. This means that each executor is responsible for only two things: executing code assigned to it by the driver, and reporting the state of the computation on that executor back to the driver node
// MAGIC
// MAGIC
// MAGIC
// MAGIC **SparkSession ?**
// MAGIC
// MAGIC you control your Spark Application through a driver process called the SparkSession. The SparkSession instance is the way Spark executes user-defined manipulations across the cluster. There is a one-to-one correspondence between a SparkSession and a Spark Application
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC DataFrame ?
// MAGIC
// MAGIC A DataFrame is the most common Structured API and simply represents a table of data with rows and columns. The list that defines the columns and the types within those columns is called the schema.
// MAGIC he reason for putting the data on more than one computer should be intuitive: either the data is too large to fit on one machine or it would simply take too long to perform that computation on one machine
// MAGIC
// MAGIC
// MAGIC Partitions ?
// MAGIC
// MAGIC To allow every executor to perform work in parallel, Spark breaks up the data into chunks called partitions. 
// MAGIC
// MAGIC
// MAGIC Transformation ?
// MAGIC
// MAGIC In Spark, the core data structures are immutable, meaning they cannot be changed after they’re created.
// MAGIC you need to instruct Spark how you would like to modify it to do what you want. These instructions are called transformations.
// MAGIC
// MAGIC Transformations are the core of how you express your business logic using Spark. There are two types of transformations: those that specify narrow dependencies, and those that specify wide dependencies.
// MAGIC
// MAGIC NARROW Dependency ?
// MAGIC     where only one partition contributes to at most one output partition
// MAGIC
// MAGIC With narrow transformations, Spark will automatically perform an operation called pipelining, meaning that if we specify multiple filters on Data Frames, they’ll all be performed in-memory. The same cannot be said for shuffles
// MAGIC
// MAGIC
// MAGIC Wide dependency ?
// MAGIC
// MAGIC A wide dependency (or wide transformation) style transformation will have input partitions contributing to many output partitions. You will often hear this referred to as a shuffle 
// MAGIC
// MAGIC
// MAGIC Lazy Evaluation ?
// MAGIC
// MAGIC Spark does not immediately modify the data , first it build up a plan of transformations that you would like to apply to your source data. By waiting until the last minute to execute the code, Spark compiles this plan from your raw DataFrame transformations to a streamlined physical plan that will run as efficiently as possible across the cluster
// MAGIC
// MAGIC
// MAGIC Action ?
// MAGIC
// MAGIC Transformations allow us to build up our logical transformation plan. To trigger the computation, we run an action.
// MAGIC
// MAGIC 	1. Actions to view data in the console
// MAGIC 	2. Actions to collect data to native objects in the respective language
// MAGIC 	3. Actions to write to output data sources
// MAGIC 	
// MAGIC
// MAGIC Example ?
// MAGIC
// MAGIC val flightData2015 = spark .read
// MAGIC .option("inferSchema", "true") .option("header", "true")
// MAGIC .csv("/data/flight-data/csv/2015-summary.csv")
// MAGIC
// MAGIC Note: prod we should have the schema
// MAGIC
// MAGIC Analysis of Example?
// MAGIC Spark reads in a little bit of the data and then attempts to parse the types in those rows according to the types available in Spark
// MAGIC Reading data is transformation, therefore it's a lazy operation
// MAGIC
// MAGIC
// MAGIC Spark.conf.set("spark.sql.shuffle.paritions",5)
// MAGIC
// MAGIC NOTE:  
// MAGIC The logical plan of transformations that we build up defines a lineage for the Data Frame so that at any given point in time, Spark knows how to recompute any partition by performing all of the operations it had before on the same input data. 
// MAGIC Spark lazily executes a DAG of transformations in order to optimize the execution plan on Data Frames.  

// COMMAND ----------

// MAGIC %md 
// MAGIC ###Spark Types

// COMMAND ----------

//To work with correct Scala types

import org.apache.spark.sql.types

val b = types.ByteType
val c = types.ArrayType
val d = types.StructType
val e = types.DateType

// COMMAND ----------

// MAGIC %md 
// MAGIC ###Data Sources    --> default format is parquet
// MAGIC - CSV
// MAGIC - JSON
// MAGIC - Parquet
// MAGIC - ORC
// MAGIC - JDBC/ODBC connections
// MAGIC - Plain-text files
// MAGIC - various other data lakes , databricks delta lakes

// COMMAND ----------

// MAGIC %md
// MAGIC ### readmode   --> default is permissive
// MAGIC
// MAGIC 1. permissive:  *Sets all fields to null when it encounters a corrupted record and places all corrupted records in a string column called _corrupt_record*
// MAGIC 2. dropMalformed: drop the row that contains malformed records, like schema is not working, or row is malformed
// MAGIC 3. failFast: Faile immediately upon encoutering malformed records

// COMMAND ----------

//reading a file which has curropt records
dbutils.fs.head("/FileStore/tables/suankiData/permissive_test.csv")

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ####//Permissive

// COMMAND ----------

//Permissive
//have to write code explicitly to handle the corrupted record.


import org.apache.spark.sql.types
val schema = types.StructType(Seq(
    types.StructField("DEST_COUNTRY_NAME", types.StringType, true),
    types.StructField("ORIGIN_COUNTRY_NAME", types.StringType, true),
    types.StructField("NO_OF_FlIGHTS", types.IntegerType, true),
    types.StructField("_corrupt_record", types.StringType, true)
)
)

val df = spark
.read
.format("csv")
.option("mode","permissive")
.option("header",true)
.option("columnNameOfCorruptRecord", "_corrupt_record")
.schema(schema)
.load("/FileStore/tables/suankiData/permissive_test.csv")

// COMMAND ----------

import org.apache.spark.sql.{functions => f}

df.where(f.col("_corrupt_record").isNotNull).show()

// COMMAND ----------

df.count()

// COMMAND ----------

//Filter the cleaned records

val cleanedRecords = df.withColumn("test",f.col("_corrupt_record").isNull).drop(f.col("_corrupt_record"))

// COMMAND ----------

// MAGIC %md
// MAGIC ####failFast

// COMMAND ----------


val failFastDF = spark
.read
.format("csv")
.option("mode","failFast")
.option("header",true)
.load("/FileStore/tables/suankiData/permissive_test.csv")

// COMMAND ----------

failFastDF.show()  //we call an Action and job is failed

// COMMAND ----------

// MAGIC %md
// MAGIC ####// dropMalformed

// COMMAND ----------

val dropMalformedDF = spark
.read
.format("csv")
.option("mode","dropMalformed")
.option("header",true)
.schema(schema)
.load("/FileStore/tables/suankiData/permissive_test.csv")

// COMMAND ----------

cleanedRecords.count()

// COMMAND ----------

dropMalformedDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ####reading jdbc source

// COMMAND ----------

def readJdbc(spark: SparkSession): Unit = {
    
    val url = raw"jdbc:oracle:thin:suanki/testpass@//localhost:1521/PDBORCL"
    val driver = raw"oracle.jdbc.driver.OracleDriver"

    //parallel load
    val minMax = (spark.read.format("jdbc")
      .option("url", url)
      .option("user", "suanki")
      .option("driver", driver)
      .option("password", "Sr#123")
      .option("query",
        s"""select cast(0 as int) as minid
                    , max(cast(product_id as int)) as maxid from products""")
      .load())

    val minMaxCasted = minMax.select(col("minid").cast("int"), col("maxid").cast("int"))

    val collectMinMax = minMaxCasted.head()
    val lowerBound = collectMinMax.getAs[Int]("minid")
    val upperBound = collectMinMax.getAs[Int]("maxid")


    // println(collectMinMax, lowerBound, upperBound)

    val productDF1 = (spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", "suanki")
      .option("password", "S123")
      .option("dbtable", "products").load())

    productDF1.rdd.getNumPartitions //1

    val productDF = (spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", "suanki")
      .option("password", "Sp123")
      //      .option("dbtable",
      //        "(select product_id, product_name, description, standard_cost,list_price, CATEGORY_ID from products) as subq")
      .option("dbtable", "products")
      .option("partitionColumn", "product_id")
      .option("lowerBound", lowerBound)
      .option("upperBound", upperBound)
      .option("numPartitions", "8")
      //      .option("fetchsize","1")
      .load())

    productDF.where(col("product_id") === 1).show()
    productDF.rdd.getNumPartitions //8


    val productDF2 = (spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", "suanki")
      .option("password", "sp#123")
      .option("query",
        """select product_id, product_name, description, standard_cost,list_price,
                CATEGORY_ID from products""".stripMargin)
      .option("numPartitions", "10")
      .option("fetchsize", "20")
      .load())

    productDF2.where(col("product_id") === 1).show()
    productDF2.rdd.getNumPartitions //1

  }


// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ####SaveMode
// MAGIC **default is errorIfExists.**
// MAGIC
// MAGIC 1.  SaveMode.append  : *Appends the output files to the list of files that already exist at that location*
// MAGIC 2.  SaveMode.Overwrite:  *Will completely overwrite any data that already exists there*
// MAGIC 3.  saveMode.errorIfExists: *Throws an error and fails the write if data or files already exist at the specified location.* 
// MAGIC 4.  SaveMode.Ignore: *If data or files exist at the location, do nothing with the current DataFrame*

// COMMAND ----------

// MAGIC %md
// MAGIC ####basic operations

// COMMAND ----------

import org.apache.spark.sql.{types => t, functions => f}

// COMMAND ----------

//creating a manual Schema
import org.apache.spark.sql.types.{StructField,StructType,StringType}
val transactionSchema = StructType(
  Array(StructField("cust_id",StringType,true),
StructField("start_date",StringType,true),
StructField("end_date",StringType,true),
StructField("txn_id",StringType,true),
StructField("date",StringType,true),
StructField("year",StringType,true),
StructField("month",StringType,true),
StructField("day",StringType,true),
StructField("expense_type",StringType,true),
StructField("amt",StringType,true),
StructField("city",StringType,true)
)
)

// COMMAND ----------

//from ddl
//spark.read.parquet(s"${DataPath.basePath}/transaction.parquet").schema.toDDL
val transactionDF = spark.read.parquet(s"${DataPath.basePath}/transaction.parquet")

// COMMAND ----------


def getConf(value:String):String = spark.conf.get(value)

println(
  s"""|getNumPartitions: ${transactionDF.rdd.getNumPartitions}
      |defaultParallelism: ${spark.sparkContext.defaultParallelism}
      |spark.conf.adaptive: ${getConf("spark.sql.adaptive.enabled")}
      |spark.sql.files.maxPartitionBytes: ${getConf("spark.sql.files.maxPartitionBytes")} ==>  128mb
      |spark.sql.shuffle.partitoins: ${getConf("spark.sql.shuffle.partitions")}
      |spark.sql.autoBroadcastJoinThreshold: ${getConf("spark.sql.autoBroadcastJoinThreshold")}  ==> 10mb
""".stripMargin('|')
)

// COMMAND ----------

//creating Df 

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}


val myManualSchema = new StructType(Array(
new StructField("some", StringType, true),
new StructField("col", StringType, true),
new StructField("names", LongType, false)))


val myRows = Seq(Row("Hello", null, 1L))

val myDF = spark.createDataFrame(spark.sparkContext.parallelize(myRows), myManualSchema)

// COMMAND ----------

//we can also take advantage of Spark’s implicits in the console

import spark.implicits._

val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

myDF.show()


// COMMAND ----------

// MAGIC %md 
// MAGIC ####Column and Expression

// COMMAND ----------

transactionDF.show(5,false)

// COMMAND ----------

import org.apache.spark.sql.{functions => f, types}


transactionDF.select(
  f.col("cust_id"),
  f.col("cust_id").alias("customer_id"),
  f.expr("cust_id as customer_id")
).show(2,false)



// COMMAND ----------

/*We can treat selectExpr as a simple way to build up
complex expressions that create new DataFrames. In fact, we can add any valid non-aggregating
SQL statement, and as long as the columns resolve, it will be valid
*/

// COMMAND ----------

transactionDF.selectExpr(
  "*",
  "year = '2018' as is_2018",
).show(2, false)

// COMMAND ----------

transactionDF.selectExpr( "avg(amt) as avgAmt","count(expense_type) as expenseCount").show(2,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####//Converting to Spark Types (Literals)
// MAGIC
// MAGIC

// COMMAND ----------

transactionDF.select(f.col("*"), f.lit(2).alias("one")).show(2,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####//Adding columns and Renaming

// COMMAND ----------

transactionDF.withColumn("is_2018", f.col("year") === "2018")
.withColumnRenamed("cust_id","customer_id").show(2,false)

// COMMAND ----------

//set spark.sql.caseSensitive true
spark.conf.get("spark.sql.caseSensitive")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ####//droping column

// COMMAND ----------

transactionDF.withColumn("test",f.col("year").cast(types.IntegerType)).drop("cust_id", "day").columns

// COMMAND ----------

// MAGIC %md
// MAGIC ####//filtering column

// COMMAND ----------

transactionDF.filter(f.col("city") === "boston")

// COMMAND ----------

//random sample

val seed = 10000
val withReplacement = false
val fraction = 0.5

transactionDF.sample(withReplacement, fraction, seed).count()

// COMMAND ----------

// MAGIC %md
// MAGIC ####union 
// MAGIC
// MAGIC -  unionAll() is deprecated since Spark “2.0.0” version and replaced with union().
// MAGIC

// COMMAND ----------

import spark.implicits._

  val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000)
  )
  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")

  
  df.printSchema()
  
  df.show()

// COMMAND ----------

val simpleData2 = Seq(("James","Sales","NY",90000,34,10000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
  val df2 = simpleData2.toDF("employee_name","department","state","salary","age","bonus")
  df2.show()

// COMMAND ----------

df.union(df2).show()

// COMMAND ----------

//combine without duplicate

val df5 = df.union(df2).distinct()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Sorting Rows
// MAGIC
// MAGIC 1. **SortBy**
// MAGIC 2. **OrderBy**
// MAGIC
// MAGIC Note: For optimization purposes, it’s sometimes advisable to sort within each partition before another
// MAGIC set of transformations. You can use the sortWithinPartitions method to do this or  while loading you can use  spark.read.parquet(somefile).sortWithinPartitions("country")

// COMMAND ----------

transactionDF.sortWithinPartitions("city").explain

// COMMAND ----------

transactionDF.sort(f.col("city")).explain

// COMMAND ----------

// MAGIC %fs
// MAGIC
// MAGIC ls /FileStore/tables/suankiData/temp

// COMMAND ----------

transactionDF.select(f.col("city")).distinct().explain

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",true)
transactionDF.orderBy(f.col("city")).explain

// COMMAND ----------

import org.apache.spark.sql.SaveMode

transactionDF.orderBy(f.col("city")).write.format("noop").mode(SaveMode.Overwrite).save("/FileStore/tables/suankiData/temp/demo")

// COMMAND ----------

// MAGIC %md
// MAGIC ####coalesce and repartition
// MAGIC partition the data according to some frequently
// MAGIC filtered columns, which control the physical layout of data across the cluster including the
// MAGIC partitioning scheme and the number of partitions.
// MAGIC
// MAGIC #### **`repartition`**
// MAGIC Returns a new DataFrame that has exactly **`n`** partitions.
// MAGIC
// MAGIC - Wide transformation
// MAGIC - Pro: Evenly balances partition sizes  
// MAGIC - Con: Requires shuffling all data
// MAGIC -   Repartition: *will incur a full shuffle of the data,you should typically only repartition when the future number of partitions is greater
// MAGIC than your current number of partitions or when you are looking to partition by a set of columns*
// MAGIC
// MAGIC
// MAGIC #### **`coalesce`**
// MAGIC Returns a new DataFrame that has exactly **`n`** partitions, when fewer partitions are requested.
// MAGIC
// MAGIC If a larger number of partitions is requested, it will stay at the current number of partitions.
// MAGIC
// MAGIC - Narrow transformation, some partitions are effectively concatenated
// MAGIC - Pro: Requires no shuffling
// MAGIC - Cons:
// MAGIC   - Is not able to increase # partitions
// MAGIC   - Can result in uneven partition sizes
// MAGIC -   coalesce:  it will try to put the memory partition on the node only by merging/moving data from one partition to another, size may be uneven
// MAGIC
// MAGIC

// COMMAND ----------

transactionDF.rdd.getNumPartitions

// COMMAND ----------

var transactionDF1 = transactionDF.repartition(24)

// COMMAND ----------

transactionDF1.rdd.getNumPartitions  //action will trigger and will cause shuffle of memory partition data

// COMMAND ----------

// MAGIC %md
// MAGIC ######*`Which one is faster`*

// COMMAND ----------


transactionDF.where(f.col("city") !== "Boston").count()

// COMMAND ----------

transactionDF1.repartition(12, f.col("city")).where(f.col("city") =!= "Boston").write.format("noop").mode(org.apache.spark.sql.SaveMode.Overwrite).save("/FileStore/tables/suank")

// COMMAND ----------

println(
  spark.sparkContext.defaultParallelism,
  spark.conf.get("spark.sql.shuffle.partitions"),
  spark.conf.get("spark.sql.adaptive.enabled")
)

// COMMAND ----------

transactionDF1.repartition(8, f.col("city")).where(f.col("city") =!= "Boston").write.format("noop").mode(org.apache.spark.sql.SaveMode.Overwrite).save("/FileStore/tables/suank")

// COMMAND ----------

transactionDF1.repartition(24, f.col("city")).where(f.col("city") =!= "Boston").write.format("noop").mode(org.apache.spark.sql.SaveMode.Overwrite).save("/FileStore/tables/suank")

// COMMAND ----------

//coalesce
transactionDF.coalesce(24).rdd.getNumPartitions  //it's giving 12 b/c we are providing more than input memory partitions of transactionDF which is 12

// COMMAND ----------

transactionDF.coalesce(10).rdd.getNumPartitions  //will work

// COMMAND ----------

// MAGIC %md
// MAGIC ####Collecting to the driver
// MAGIC
// MAGIC -- don't collect large dataset. can make driver OOM

// COMMAND ----------

transactionDF.limit(2).collect()

// COMMAND ----------

transactionDF.limit(2).collect()(0).getAs[String](0)

// COMMAND ----------

// toLocalIterator collects partitions to the driver as an iterator. This
// method allows you to iterate over the entire dataset partition-by-partition in a serial manner

try{
 val d = transactionDF.limit(2).toLocalIterator()
 while(d.hasNext()){
  println(d.next())
 }
}
 catch {
  case a:RuntimeException => throw new RuntimeException
 }

// COMMAND ----------


