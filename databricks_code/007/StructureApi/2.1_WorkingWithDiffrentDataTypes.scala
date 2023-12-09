// Databricks notebook source
// MAGIC %run /007/CommonUtils/SetupData

// COMMAND ----------

// MAGIC %md
// MAGIC ### Working with null values
// MAGIC
// MAGIC 1. coalesce(col("Description"), col("CustomerId")) : select the first non-null value from a set of columns 
// MAGIC 2. ifnull, nullIf, nvl, and nvl2
// MAGIC 3. 
// MAGIC ```
// MAGIC always use nulls to represent missing or empty data in your
// MAGIC DataFrames. Spark can optimize working with null values more than it can if you use empty
// MAGIC strings or other values
// MAGIC
// MAGIC df.na._
// MAGIC 1. There are two things you can do with null values: you can explicitly drop nulls or you can fill
// MAGIC them with a value (globally or on a per-column basis)
// MAGIC ```

// COMMAND ----------

val timeData = Seq(
            ("2019-07-01 12:01:19",
            "07-01-2019 12:01:19", 
            "07-01-2019"))

val timeDf = timeData.toDF("dateOne","dateTwo","dateThree")

timeDf.show()

// COMMAND ----------

/*
SELECT
ifnull(null, 'return_value'),
nullif('value', 'value'),
nvl(null, 'return_value'),
nvl2('not_null', 'return_value', "else_value")
FROM dfTable LIMIT 1
*/


val data=Seq(
("101",   "sujeet",  null),
("102",   null,     "23"),
("103",   null,     null),
("104",   "Rajoo",  null)
)

val df = data.toDF("id","name","age")


df.select( f.col("id"), f.col("name"),f.col("age"),
f.coalesce(f.col("id"), f.col("age")),
f.expr("ifnull(name,age)").as("ifnull"),
f.expr("nullif(name,age)").as("nullif"),
f.expr("nvl(name,age)").as("nvl"),
f.expr("nvl2(name,age,'else_val')").as("nvl2")
)
.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###drop
// MAGIC `drop, which removes rows that contain nulls. `
// MAGIC
// MAGIC 1. df.na.drop("any")  : this is default behaviour, if any column has null value it will drop entire row
// MAGIC 2. df.na.drop("all"):  if all the column has null value then only drop the row
// MAGIC 3. df.na.drop("all", Seq("StockCode", "InvoiceNo")):  if selected columns has null value , then only drop the row
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ####fill
// MAGIC
// MAGIC `Using the fill function, you can fill one or more columns with a set of values`
// MAGIC
// MAGIC 1. df.na.fill("All Null values become this string")  :   all null vaues become passed string
// MAGIC 2. df.na.fill(5, Seq("StockCode", "InvoiceNo")) :  all the null value of for provided seq of columns would becom 5
// MAGIC 3. val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
// MAGIC
// MAGIC        df.na.fill(fillColValues)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###replace
// MAGIC
// MAGIC > this work with other values as well
// MAGIC `There are more flexible
// MAGIC options that you can use with more than just null values. Probably the most common use case is
// MAGIC to replace all values in a certain column according to their current value`
// MAGIC
// MAGIC 1. df.na.replace("Description", Map("" -> "UNKNOWN"))  :  replace empty string with uknown for column description
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###Working with Complex Types
// MAGIC
// MAGIC >There are three kinds of complex types: structs, arrays,
// MAGIC and maps.

// COMMAND ----------

// MAGIC %md
// MAGIC ###1. Struct
// MAGIC `You can think of structs as DataFrames within DataFrames`

// COMMAND ----------

val structDF = retailDF.select(f.struct("Description", "InvoiceNo").alias("complex"))

structDF.select(f.col("complex").getField("description"), f.col("complex")).show(5,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ###2. Array

// COMMAND ----------

//The first task is to turn our Description column into a complex type, an array
val splittedInArray = f.split(f.col("description")," ")

retailDF
.select(
  f.col("country"),f.col("description"),splittedInArray,
 f.size(splittedInArray),
 f.array_contains(splittedInArray, "WHITE").as("ifContainWhite")
 )
.show(5,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####explode
// MAGIC The explode function takes a column that consists of arrays and creates one row (with the rest of
// MAGIC the values duplicated) per value in the array

// COMMAND ----------

val explodedDF = retailDF
.withColumn("exploded",f.explode(splittedInArray))
.withColumn("splittedCol",splittedInArray)
explodedDF.show(5,false)

// COMMAND ----------

explodedDF.groupBy(splittedInArray,f.col("country")).agg(f.collect_set(f.col("exploded"))).show(5,false) //revert exploded value

// COMMAND ----------

explodedDF
.withColumn("array1", f.array_distinct(splittedInArray))
.select("description","array1","splittedCol")
.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Maps
// MAGIC Maps are created by using the map function and key-value pairs of columns. You then can select
// MAGIC them just like you might select from an array:
// MAGIC
// MAGIC >//You can also explode map types, which will turn key and value  into seprate column columns

// COMMAND ----------

val complexDF = retailDF.withColumn("complex_map",f.map(f.col("Description"), f.col("InvoiceNo")))
.select(f.col("Description"), f.col("invoiceno"), f.col("complex_map"),f.col("complex_map").getField("CITRONELLA CANDLE FLOWERPOT")
)
complexDF.show(2,false)

// COMMAND ----------

complexDF.select(f.explode(f.col("complex_map"))).show(5,false)

// COMMAND ----------

//You can also explode map types, which will turn them into columns
complexDF.select(f.col("description"),f.explode(f.col("complex_map"))).show(5,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ###User defined function
// MAGIC
// MAGIC >user-defined functions (UDFs) make it possible for you to write your own custom
// MAGIC transformations using Python or Scala and even use external libraries. 
// MAGIC
// MAGIC 1. create a function
// MAGIC 2. register with myudf = f.udf(f:Int=>Double)
// MAGIC 3. call on column  myudf(col("test"))
// MAGIC
// MAGIC `Note:
// MAGIC
// MAGIC if you want to use in expr or in sql  we can also register this UDF as a
// MAGIC Spark SQL function
// MAGIC
// MAGIC spark.udf.register("power3", power3(_:Double):Double)`

// COMMAND ----------

def power3(number:Double):Double = number * number * number
power3(2.0)

// COMMAND ----------

val power3udf = f.udf(power3(_:Double):Double)

// COMMAND ----------

spark.range(5).toDF("num")
.select(power3udf(f.col("num")))
.show()

// COMMAND ----------


