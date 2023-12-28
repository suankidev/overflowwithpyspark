// Databricks notebook source
spark

// COMMAND ----------

//dbutils.fs.rm("dbfs:/FileStore/tables",true)  //Seq[com.databricks.backend.daemon.dbutils.FileInfo] 

// COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/suankiData")
dbutils.fs.mkdirs("dbfs:/FileStore/tables/suankiData/flightData")
dbutils.fs.mkdirs("dbfs:/FileStore/tables/suankiData/retailData")


// COMMAND ----------

val path=raw"dbfs:/FileStore/tables/suankiData/"
println(s"upload files under $path")

// COMMAND ----------


dbutils.fs.ls("dbfs:/FileStore/tables/suankiData/flightData") foreach println


// COMMAND ----------


dbutils.fs.ls("dbfs:/FileStore/tables/suankiData/retailData") foreach println


// COMMAND ----------

val flightData2015 = spark
.read
.option("inferSchema", "true")
.option("header", "true")
.csv(s"$path/flightData/2015_summary.csv")

// COMMAND ----------



val retailData2010 = spark
.read
.option("inferSchema", "true")
.option("header", "true")
.csv(s"dbfs:/FileStore/tables/suankiData/retailData/2010_12_01.csv")

// COMMAND ----------

import org.apache.spark.sql.Row

// COMMAND ----------

val take5:Array[Row] = flightData2015.take(5)  //Array[org.apache.spark.sql.Row]

// COMMAND ----------

take5(0)

// COMMAND ----------

flightData2015.sort("count").explain

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",5)  //by default when there is shuffle will break data into 200 partitions 

// COMMAND ----------

flightData2015.sort("count").take(5)

// COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")

// COMMAND ----------

spark.sql("show tables in default").show()

// COMMAND ----------

spark.sql("desc flight_data_2015 ").show()

// COMMAND ----------

val sqlWay=spark.sql("select DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count from flight_data_2015 order by count desc ")
sqlWay.show()

// COMMAND ----------

import org.apache.spark.sql.functions.max,sum

flightData2015.select(max("count")).take(1)

// COMMAND ----------

val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

// COMMAND ----------

import org.apache.spark.sql.functions.{sum,max,desc,col}
flightData2015.groupBy("DEST_COUNTRY_NAME").agg(sum("count").alias("destination_total")).orderBy(col("destination_total").desc).show()

// COMMAND ----------

flightData2015.groupBy(col("DEST_COUNTRY_NAME")).agg(sum("count").alias("destination_total")).sort(desc("destination_total")).explain
//partial_sum ==> 

// COMMAND ----------

retailData2010.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._


// COMMAND ----------



val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")
val DOTCodeFilter = col("StockCode") === "DOT"

retailData2010.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
.show()

// COMMAND ----------

retailData2010.withColumn("isExpensive", col("unitPrice") > 600)
.filter("isExpensive")
.select("Description", "UnitPrice").show(5)

// COMMAND ----------

retailData2010.where(col("Description").eqNullSafe("hello").isin("DOT","TEST")).show()

// COMMAND ----------

//Working with Numbers

// COMMAND ----------

spark.range(5).select(round(lit("2.5")), bround(lit("2.5"))).show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{corr,initcap}

// COMMAND ----------

retailData2010.select(corr("Quantity", "UnitPrice")).show()

// COMMAND ----------

retailData2010.describe().show()

// COMMAND ----------

//working with String

// COMMAND ----------

val df=retailData2010

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

df.select(initcap(col("Description"))).show(2, false)

// COMMAND ----------

df.select(col("Description"),
lower(col("Description")),
upper(lower(col("Description")))).show(2)

// COMMAND ----------

val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")

println(regexString)

// COMMAND ----------

df.select(
regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
col("Description")).show(2,false)

// COMMAND ----------

df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
.show(2)

// COMMAND ----------

val regexString = simpleColors.map(_.toUpperCase).mkString("(","|",")")

println(regexString)

// COMMAND ----------

df.select(
regexp_extract(col("Description"), regexString, 1).alias("color_clean"),
col("Description")).show(2,false)

// COMMAND ----------

val containsBlack = col("Description").contains("BLACK")
val containsWhite = col("DESCRIPTION").contains("WHITE")
df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
.where("hasSimpleColor")
.select("Description").show(3, false)

// COMMAND ----------

val simpleColors = Seq("black", "white", "red", "green", "blue")

var selectedcolumns = simpleColors.map(color =>
col("description").contains(color).alias(s"is_$color")
)

selectedcolumns

// COMMAND ----------

selectedcolumns = selectedcolumns :+ col("description")

// COMMAND ----------

df.select(selectedcolumns:_*).show()

// COMMAND ----------

//doing the sum on multiple columns

val myData = Seq(
  ("sujeet",23,60),
  ("Ramesh",26,80),
  ("kamesh",27,70),

)
val myDataRdd = spark.sparkContext.parallelize(myData)
import spark.implicits._

val mydf = myDataRdd.toDF("name","age","marks")


// COMMAND ----------

mydf.show()

// COMMAND ----------

//Question: calculate sum on multiple columns

// COMMAND ----------

mydf.select(sum("age"), sum("marks"), min("marks"), max("marks")).show()

// COMMAND ----------

val colList = List("age","marks")

val selectedCols = colList.map(colName => sum(col(colName)).alias(s"sum_$colName"))

// COMMAND ----------

mydf.select(selectedCols:_*).show()

// COMMAND ----------

//working with date and time

// COMMAND ----------

val dateDF = spark.range(10)
.withColumn("today", current_date())
.withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")

// COMMAND ----------

dateDF.show(false)

// COMMAND ----------

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

// COMMAND ----------

dateDF.withColumn("week_ago", date_sub(col("today"), 7)).show()

// COMMAND ----------

dateDF.withColumn("week_ago", date_sub(col("today"), 7))
.select(datediff(col("week_ago"), col("today"))).show(1)


// COMMAND ----------

dateDF.select(
to_date(lit("2016-01-01")).alias("start"),
to_date(lit("2017-05-22")).alias("end")).schema

//.select(months_between(col("start"), col("end"))).

// COMMAND ----------

spark.range(5).withColumn("date", lit("2017-01-01"))
.select(to_date(col("date"))).show(1)

// COMMAND ----------

dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

// COMMAND ----------

val dateFormat = "yyyy-dd-MM"
val cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))

// COMMAND ----------

//to_timestamp, which always requires a format to be specified:

// COMMAND ----------

cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

// COMMAND ----------

//working with null data

// COMMAND ----------

df.select(coalesce(col("Description"), col("CustomerId"))).show()

// COMMAND ----------

df.na.drop("all", Seq("StockCode", "InvoiceNo"))
df.na.drop("all")
df.na.drop("any")

// COMMAND ----------

//fill null value 

// COMMAND ----------

df.na.fill("All Null values become this string")

df.na.fill(5, Seq("StockCode", "InvoiceNo"))

val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
df.na.fill(fillColValues)


// COMMAND ----------

df.na.replace("Description", Map("" -> "UNKNOWN"))
//df.na.replacedef replace[T](cols: Seq[String],replacement: Map[T,T]): org.apache.spark.sql.DataFrame

// COMMAND ----------

//working with complex type

// COMMAND ----------

val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))

// COMMAND ----------

complexDF.show(10,false)

// COMMAND ----------

complexDF.select(col("complex").getField("description")).show(5,false)
complexDF.select(col("complex.description")).show(5,false)

// COMMAND ----------

df.select(split(col("Description"), " ")).show(2,false)

// COMMAND ----------

df.select(split(col("Description"), " ").alias("array_col"))
.select(col("array_col")(0), size(col("array_col")), array_contains(col("array_col"),"WHITE")).show(2)

// COMMAND ----------

val explodedDF= df.withColumn("exploded_columns", explode(split(col("description"), " ")))
.select("description","invoiceNo","exploded_columns")


// COMMAND ----------

explodedDF.show()

// COMMAND ----------

explodedDF.groupBy(col("description")).agg(collect_set("exploded_columns").alias("reset")).show(5,false)

// COMMAND ----------

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
.select(col("complex_map"), col("complex_map")("WHITE METAL LANTERN"))
.show(2,false)

// COMMAND ----------

//Working with UDF

// COMMAND ----------

def power3(number:Double):Double = number * number * number
power3(2.0)

// COMMAND ----------

val udfExampleDF = spark.range(5).toDF("num")

// COMMAND ----------

val power3udf = udf(power3(_:Double):Double)
val power3udf1 = udf((x:Int)=>x*x*x)

// COMMAND ----------

udfExampleDF.select(power3udf1(col("num"))).show()

// COMMAND ----------

spark.udf.register("power3", power3(_:Double):Double)
udfExampleDF.selectExpr("power3(num)").show(2)

// COMMAND ----------


