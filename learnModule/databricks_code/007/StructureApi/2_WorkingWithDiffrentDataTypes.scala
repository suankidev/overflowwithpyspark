// Databricks notebook source
// MAGIC %md
// MAGIC **`variety of different kinds of data, including the following:`**
// MAGIC 1. Booleans
// MAGIC 2. Numbers
// MAGIC 3. Strings
// MAGIC 4. Dates and timestamps
// MAGIC 5. Handling null
// MAGIC 6. Complex types
// MAGIC 7. User-defined functions
// MAGIC
// MAGIC
// MAGIC *submodule of org.apache.spark.sql  has many functions to work with data sets*
// MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/index.html?search=functions">latest doc</a>
// MAGIC
// MAGIC 1. class DataFrameNaFunctions extends AnyRef
// MAGIC 2. class DataFrameStatFunctions extends AnyRef
// MAGIC 3. object Functions <a href="https://github.com/apache/spark/blob/v3.5.0/sql/core/src/main/scala/org/apache/spark/sql/functions.scala">org.apache.spark.sql.functions</a>
// MAGIC

// COMMAND ----------

// MAGIC %run ../CommonUtils/SetupData

// COMMAND ----------

customerDF.printSchema

// COMMAND ----------

transactionDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ####lit
// MAGIC
// MAGIC The lit functions. This functions converts a type in another language to its corresponding Spark representation
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.{functions => f,types, DataFrameReader, DataFrameNaFunctions => na, DataFrameStatFunctions => sf}

// COMMAND ----------

customerDF.select(f.lit(5), f.lit(5.0), f.lit("five"), f.lit(null)).show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Boolean

// COMMAND ----------

customerDF.show(5,false)

// COMMAND ----------

customerDF.where(f.col("gender").equalTo("Female")).select("name","cust_id","city","age").show(5,false)   //Scala uses  ===  for equality, =!=  for not equal , in case we don't wnat to use equalTo

// COMMAND ----------

val priceFilter = f.col("UnitPrice") > 600
val descripFilter = f.col("Description").contains("POSTAGE")

retailDF.where(f.col("stockcode").isin("DOT")).where(priceFilter || descripFilter).show(5,false)

// COMMAND ----------

//Notice how we did not need to specify our filter as an expression and how we could use a column name without any extra work
val dotCodFilter = f.col("stockCode") === "DOT"

retailDF.withColumn("isExpensive", dotCodFilter.and(priceFilter.or(descripFilter))).show(5, false)

// COMMAND ----------

retailDF
.withColumn("isExpensive", f.not( f.col("UnitPrice").leq(250)))
.filter("isExpensive")
.select("description","unitprice")
.show(5,false)

// COMMAND ----------

//above transformation can be written as below in using expression
//https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html#isInCollection(values:Iterable[_]):org.apache.spark.sql.Column
retailDF
.withColumn("isExpensive", !( f.col("UnitPrice") <= 250))
.filter("isExpensive")
.select("description","unitprice")
.show(5,false)

// COMMAND ----------

/*
If there is a null in your data, you’ll need to treat things a bit differently. Here’s how you can ensure
that you perform a null-safe equivalence test:
df.where(col("Description").eqNullSafe("hello")).show()
*/

// COMMAND ----------

retailDF
  .withColumn("nullSafeCustomerID",f.col("customerId").eqNullSafe("Hello"))
  .where("nullSafeCustomerID")  //select value which is null , working in java stype, but same can be handel by using isnull, isnotnull
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Working with Numbers

// COMMAND ----------

//imagin the true quantity = (quantity * unitproce)^2 + 5

val fabricatedQuantity = f.pow(f.col("quantity") * f.col("unitprice"), 2) + 5

retailDF
  .select(f.col("quantity"), fabricatedQuantity.alias("realQuantity"))
  .withColumn("round", f.round(f.col("realQuantity"),2))
  .show(2,false)

// COMMAND ----------

spark
.range(5)
.toDF("num")
.select(f.round(f.lit(2.5) , 0) ,  f.bround(f.lit(2.5), 0))
.show()

// COMMAND ----------

retailDF.describe().show() //summary of the df

// COMMAND ----------

retailDF.withColumn("increasingId", f.monotonically_increasing_id()).show(5,false)  
//The assumption is that the data frame has less than 1 billion partitions, and each partition has less than 8 billion records.

// COMMAND ----------

// MAGIC %md
// MAGIC ####Working with String

// COMMAND ----------

val initCap = f.initcap(f.col("description"))  //capatilize
val lowe = f.lower(f.col("description"))  //lower
val uppe = f.upper(f.col("description"))  //upper
val tr = f.trim(f.lit(" Hello "))  //trim spaces from left and right
val ltri = f.ltrim(f.lit(" Hello   "))  //trim spaces from left
val lpad = f.lpad(f.lit("Hello"), 3, "-")  //from left onl three char
val lpad1 = f.lpad(f.lit("He"), 3, "-")  //if there are less than three, will filled by '-' from left 


retailDF.select(
  initCap,
  lowe,
  uppe,
  tr,
  ltri,
  lpad,
  lpad1
).show(5, false)





// COMMAND ----------

// MAGIC %md
// MAGIC ####Regular Expression
// MAGIC `*There are two key functions in Spark that you’ll need in
// MAGIC order to perform regular expression tasks: regexp_extract and regexp_replace.
// MAGIC
// MAGIC 1. regexp_replace(col, regextString, replacement)`
// MAGIC 2. regexp_extract(col, regextString, index)
// MAGIC 3. translate(col, "oldValue:String", "newValue:String"):  replace fiven characters with other charecters
// MAGIC
// MAGIC *//Sometimes, rather than extracting values, we simply want to check for their existence*
// MAGIC
// MAGIC 1. col("description").contains("some value")
// MAGIC
// MAGIC     `we can do same in the sql  using:  instr(description, 'BLACK') >= 1`
// MAGIC
// MAGIC 2.

// COMMAND ----------

val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toString.toUpperCase).mkString("|")
val regexExtractString = simpleColors.map(_.toUpperCase).mkString("(","|",")")
println(regexString)

// COMMAND ----------

retailDF
.select(f.col("description"), f.regexp_replace(f.col("description"), regexString, "Sujeet").as("regexReplace"),
        f.translate(f.col("description"),"LEET","MFFU").alias("translate"),
        f.regexp_extract(f.col("description"),regexExtractString,1 ).as("regexExtract") ) //where(f.col("regexExtract") === "")-> if no value extracted will return empty 
.show(5,false)

// COMMAND ----------

retailDF.printSchema

// COMMAND ----------

//This is trivial with just two values, but it becomes more complicated when there are values
val containsBlack = f.col("Description").contains("BLACK")
val containsWhite = f.col("DESCRIPTION").contains("WHITE")

retailDF.withColumn("hasSimplecolor", containsBlack || containsWhite )
.filter(f.col("hasSimplecolor"))
.show(5, false)

// COMMAND ----------

var selectedColors = simpleColors.map( color => {
  f.col("description").contains(color).alias(s"is_$color")
})      //will generate a seq of columns

val finalSelect = f.col("*") +: selectedColors 
finalSelect

display(retailDF.select(selectedColors:_*))


// COMMAND ----------

display(retailDF.select(finalSelect:_*))

// COMMAND ----------

//isin
retailDF
.withColumn("test", f.col("stockCode").isin("20781","22052"))
.show(5,false)

// COMMAND ----------

// MAGIC
// MAGIC %md
// MAGIC #### Working with date and times
// MAGIC
// MAGIC `we often store our timestamps or dates as strings and convert them into date types at
// MAGIC runtime.`
// MAGIC
// MAGIC 1. current_date()
// MAGIC 2. current_timestamp()
// MAGIC 3. to_date(col(date),"yyyy-MM-dd")  or to_date(col(date))
// MAGIC 4. to_timestamp(col(date), "yyyy-mm-dd")  0r to_timestamp(col(date))
// MAGIC
// MAGIC [SET timzone](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-conf-mgmt-set-timezone.html)
// MAGIC
// MAGIC [Java simpledataformat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html)
// MAGIC ```
// MAGIC Note:
// MAGIC =>When it
// MAGIC comes to timezone handling. In version 2.1 and before, Spark parsed according to the machine’s
// MAGIC timezone if timezones are not explicitly specified in the value that you are parsing. You can set a
// MAGIC session local timezone if necessary by setting spark.conf.session.timeZone in the SQL
// MAGIC configurations. This should be set according to the Java TimeZone format
// MAGIC
// MAGIC => Spark will not throw an error if it cannot parse the date; rather, it will just return null
// MAGIC ```
// MAGIC
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC >Unix time is also known as Epoch time which specifies the moment in time since 1970-01-01 00:00:00 UTC
// MAGIC
// MAGIC
// MAGIC 5. unix_timestamp(): is used to get the current time and to convert the time string in a format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds) and
// MAGIC      1) def unix_timestamp()
// MAGIC      2) def unix_timestamp(s: Column)
// MAGIC      3) def unix_timestamp(s: Column, p: String) 
// MAGIC 6. from_unixtime():  Converts Unix Time Seconds to Date and Timestamp
// MAGIC     1) def from_unixtime(ut: Column): Column
// MAGIC     2) def from_unixtime(ut: Column, f: String): Column
// MAGIC
// MAGIC  
// MAGIC

// COMMAND ----------

spark.conf.get("spark.sql.session.timeZone")  //default TZ of the system
spark.sql("SET TIME ZONE 'America/Los_Angeles'")
spark.conf.get("spark.sql.session.timeZone")

// COMMAND ----------

/**
Spark’s TimestampType class supports only second-level precision, which means that if
you’re going to be working with milliseconds or microseconds, you’ll need to work around this
problem by potentially operating on them as longs
**/

// COMMAND ----------

val dateDF = spark
.range(5)
.withColumn("today", f.current_date())
.withColumn("now", f.current_timestamp())
dateDF.printSchema
dateDF.show()

// COMMAND ----------

// spark.sql("SET TIME ZONE 'IST'")  //changing to IST
spark.conf.get("spark.sql.session.timeZone","IST")
spark.conf.get("spark.sql.session.timeZone")
val dateDF = spark
.range(5)
.withColumn("today", f.current_date())
.withColumn("now", f.current_timestamp())
dateDF.printSchema
dateDF.show()

// COMMAND ----------


dateDF.select(f.col("today")
              ,f.date_add(f.col("today"), 7).as("date_add")
              ,f.date_sub(f.col("today"),7).as("date_sub")
              ,f.datediff(f.col("today"), f.to_date(f.lit("2022-12-30"))).as("datediff")
              ,f.months_between(f.col("today"), f.to_date(f.lit("2022-12-30"))).as("month_between")
              ,f.dayofyear(f.col("today")).as("dayofyear")
              ,f.dayofmonth(f.col("today")).as("dayofmonth")
              ,f.dayofweek(f.col("today")).as("dayofweek")
              )
              .show()




// COMMAND ----------

dateDF.select(f.to_date(f.lit("2016-20-12")),f.to_date(f.lit("2017-12-11"))).show(1)  //yyyy-mm-dd 

// COMMAND ----------

/**
Let’s fix this pipeline, step by step, and come up with a robust way to avoid these issues entirely.
The first step is to remember that we need to specify our date format according to the Java
SimpleDateFormat standard.
We will use two functions to fix this: to_date and to_timestamp. The former optionally
expects a format
**/

// COMMAND ----------

val dateFormat = "yyyy-dd-MM"
val dateFormatOne = "yyyy-MM-dd"
val startDate = f.lit("2022-12-30")
val endDate = f.lit("2022-30-12")
dateDF.select(
 f.to_date(startDate,dateFormatOne)
,f.to_date(endDate,dateFormat)
,f.to_date(endDate))
.show()

// COMMAND ----------

val cleanedDF = dateDF.select(
 f.col("*"),
 f.to_date(startDate,dateFormatOne).alias("todate")
)
cleanedDF.show(false)

// COMMAND ----------

cleanedDF
.withColumn("totimestamp", f.to_timestamp(f.col("today")))
.withColumn("totimestampWithDateFormate", f.to_timestamp(f.col("today"), "yyyy-MM-dd"))  //in case date are in some specific format
.withColumn("todateOnNow", f.to_date(f.col("now")))
.show(false)

// COMMAND ----------



// COMMAND ----------

val timeData = Seq(
            ("2019-07-01 12:01:19",
            "07-01-2019 12:01:19", 
            "07-01-2019"))

val timeDf = timeData.toDF("dateOne","dateTwo","dateThree")

timeDf.show()

// COMMAND ----------

val unixTime = f.unix_timestamp(f.col("dateTwo"), "MM-dd-yyyy HH:mm:ss")

timeDf.select(
  f.unix_timestamp(f.col("dateOne")).alias("epochTime"),
  f.unix_timestamp(f.col("dateTwo"), "MM-dd-yyyy HH:mm:ss").alias("epochByformate"),
  f.unix_timestamp(f.col("dateThree"), "MM-dd-yyyy"),
  f.from_unixtime(unixTime).as("fromEpoch"),
  f.from_unixtime(unixTime,"yyyy-dd-MM").as("fromEpochWithFormat")
)
.show()

// COMMAND ----------

/**
Implicit type casting is an easy way to shoot yourself in the foot, especially when dealing with null
values or dates in different timezones or formats. We recommend that you parse them explicitly
instead of relying on implicit conversions.

**/

// COMMAND ----------


