# Databricks notebook source
from pyspark.sql.session import SparkSession

flightDf=spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/data/fligt_data/*.csv")

retailDf=spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/data/retail-data/*.csv")



# COMMAND ----------

flightDf.take(3)

print(retailDf.rdd.getNumPartitions)

d=retailDf.repartition(5)

print(d.rdd.getNumPartitions)

# COMMAND ----------

flightDf.sort("count")

# COMMAND ----------

flightDf.createOrReplaceTempView("flightAsTable")

# COMMAND ----------


spark.conf.set("spark.sql.shuffle.partitions", 5)
sqlway=spark.sql("select  DEST_COUNTRY_NAME, count(1) from flightAsTable group by DEST_COUNTRY_NAME")

dfway=flightDf.groupBy("count").count()

# COMMAND ----------

sqlway.explain()

dfway.explain()

# COMMAND ----------

from pyspark.sql.functions import *
flightDf.groupBy("DEST_COUNTRY_NAME").agg(
sum("count").alias("sum")).orderBy(col("sum").desc()
                                  ).limit(5).show()

# COMMAND ----------

staticSchema=retailDf.schema


retailDf.show(5)



# COMMAND ----------

retailDf.select(expr("CustomerID"),expr("UnitPrice * Quantity").alias("totalCast"),expr("InvoiceDate"))\
                .groupBy(col("customerid"),window(col("invoicedate"),"1 day"))\
                .sum("totalCast").show(5,False)

# COMMAND ----------

from pyspark.sql import Row


spark.sparkContext.parallelize([Row(1,2),Row(2,3),Row(3,4)]).toDF().show(5)

"""
Spark uses an engine called
Catalyst that maintains its own type information through the planning and processing of work. In
doing so, this opens up a wide variety of execution optimizations that make significant
differences. 

"""



# COMMAND ----------

df=spark.range(500).toDF("number")
df.select(df["number"]).show()

# COMMAND ----------


"""usercode--->unresovled logical plan--->catalog(analysis)--->Resolved logical plan----->
logical optimization------->optimized logical plan---->physical plan-----cost model------>best physical plan------------->execution of rdd on cluster
"""

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,LongType

flightSchema=StructType(
[
  StructField("DEST_COUNTRY_NAME",StringType(),True),
  StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
  StructField("count",LongType(),False)
])

flightDfWithSchema=spark.read.schema(flightSchema).option("header","true").csv("/FileStore/tables/data/fligt_data/*.csv")




# COMMAND ----------

myRow=Row("hello",None,1,False)
print(myRow[0])

# COMMAND ----------

from pyspark.sql.types import FloatType
manualSchema=StructType([
StructField("name", StringType(), True),
StructField("sal", StringType(), True),
StructField("exp", FloatType(), False)
])
oneRow=Row("rohan",5600,5.8)
names=[Row("rohan",5600,5.8),Row("sujeet",38000,3.6)]

spark.createDataFrame(names,manualSchema).show()

spark.sparkContext.parallelize(names).toDF().show()


# COMMAND ----------

"""
select method when you’re working with columns or expressions,
and the selectExpr method when you’re working with expressions in strings. Naturally some
transformations are not specified as methods on columns; therefore, there exists a group of
functions found in the org.apache.spark.sql.functions package
"""

# COMMAND ----------

retailDf.printSchema()
retailDf.where(expr("CustomerID = '17850.0' and StockCode = '84406B' and Country = 'United Kingdom' ")).limit(5).show()



# COMMAND ----------

data1=[["Mahesh", 35636,"karvenagar"] ,["Rajoo",33789,"Rampoor"],["tesla",37937,"jamnagar"]]
myschema=["Name","sal","add"]
df1=spark.createDataFrame(data1,myschema)


data2=[["karim", 35636,"karvenagar"] ,["must",33789,"Rampoor"],["paresh",37937,"jamnagar"]]


df2=spark.createDataFrame(data2,myschema)





df1.show()
df2.show()


#or

spark.sparkContext.parallelize(data1).toDF(myschema).show()


df1.union(df2).show()


# COMMAND ----------

a=flightDf.select(col("dest_country_name").alias("dest"),col("origin_country_name").alias("origin"),col("count"))\
.orderBy(col("dest").asc(),col("origin").asc())

a.rdd.getNumPartitions()

b=a.repartition(5,col("dest"))

a.rdd.getNumPartitions()
b.rdd.getNumPartitions()


#any collection is expensive on dataframe

for i in a.collect():
  print(i[0],i[1])



# COMMAND ----------

retailDf.where(instr(col("description"), "WHITE" ) == 1).show(5,False)

# COMMAND ----------

 #instr return 1 if it contain it's like string.contains in scala
  #Returns 0 if the given substring could not be found.
  
priceFilter=col("Unitprice")>6
descripFilter=instr(col("Description") , "POSTAGE") >= 1

retailDf.where(col("StockCode").isin("DOT")).where( priceFilter | descripFilter).show()

# COMMAND ----------

DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1

retailDf.withColumn("isExpensive", col("unitprice") >= 600).where(col("isExpensive")).show()


# COMMAND ----------

#can be checked if it's null
retailDf.where(col("Description").isNull()).select(col("description").eqNullSafe("hi")).show(5)


# COMMAND ----------

total=pow(col("unitprice") * col("quantity") * 75, 2)
retailDf.select(col("customerid"), total.alias("totalIndianRupees")).show(5)


# COMMAND ----------

retailDf.select(corr("Quantity", "UnitPrice")).show()

retailDf.describe().show()

# COMMAND ----------

retailDf.select(monotonically_increasing_id()).show(10)

# COMMAND ----------

#working with string

retailDf.select(initcap(col("Description"))).show()

retailDf.select(col("Description"),
lower(col("Description")),
upper(lower(col("Description")))).show(2)


retailDf.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# COMMAND ----------

#regular Exxpression

from pyspark.sql.functions import regexp_extract,regexp_replace

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"

retailDf.select(
regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
col("Description")).show(2)

# COMMAND ----------

#Another task might be to replace given characters with other characters.

retailDf.select(translate(col("Description"), "LEET", "1337"),col("Description")).show(2)

# COMMAND ----------

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"

retailDf.select(regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),col("Description")).show(2,False)


retailDf.select(regexp_extract(col("Description"), "(WHITE)", 1).alias("color_clean"),col("Description")).show(2)



# COMMAND ----------

containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1

retailDf.withColumn("hasSimpleColor", containsBlack | containsWhite )\
.where("hasSimpleColor")\
.select("Description").show(3, False)

# COMMAND ----------

from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]

def color_locator(mycol, color_string):
    return locate(color_string.upper(), mycol).cast("boolean").alias("is_" + color_string)

selectedColumns = [color_locator(retailDf.Description, c) for c in simpleColors]

selectedColumns.append("*")

retailDf.select(*selectedColumns).show(5)

# COMMAND ----------

#working with data and time
from pyspark.sql.functions import current_date, current_timestamp

dateDF = spark.range(10).withColumn("today", current_date()).withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")


dateDF.printSchema()

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)


dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(datediff(col("week_ago"), col("today"))).show(1)



dateDF.select(to_date(lit("2016-01-01"),"yyyy-MM-dd").alias("start"),to_date(lit("2017-05-22")).alias("end")).select(months_between(col("start"), col("end"))).show(1)


#Spark will not throw an error if it cannot parse the date; rather, it will just return null.

dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)



dateDF.select(to_date(lit("2016-20-12"), "yyyy-dd-MM"),to_date(lit("2017-12-11"))).show(1)


dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")




from pyspark.sql.functions import to_timestamp


cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()


#comparing the date
"""After we have our date or timestamp in the correct format and type, comparing between them is
actually quite easy. We just need to be sure to either use a date/timestamp type or specify our
string according to the right format of yyyy-MM-dd if we’re comparing """

cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()

#or

cleanDateDF.filter(col("date2") > "'2017-12-12'").show()



# COMMAND ----------

dateDF.select().show(10,False)


# COMMAND ----------

#working with null data
# .na subpackage on a DataFrame

retailDf.select(coalesce(col("Description"), col("CustomerId"))).show(5)



#ifnull, nullIf, nvl, and nvl2
"""
There are several other SQL functions that you can use to achieve similar things. ifnull allows
you to select the second value if the first is null, and defaults to the first. Alternatively, 

you coulduse nullif, which returns null if the two values are equal or else returns the second if they are
not.

nvl   --> first is null   return second, but defaults to the first. Finally, 

nvl2  --> first is not null return second ; otherwise, it will return the last specified value


drop:

The simplest function is drop, which removes rows that contain nulls
"""


retailDf.na.drop()        #defaul any row that contains null      

retailDf.na.drop("any")

retailDf.na.drop("all")      #remove if all row is null

retailDf.na.drop("all", subset=["StockCode", "InvoiceNo"])



# COMMAND ----------

retailDf.na.fill("All Null values become this string")

retailDf.na.fill("all", subset=["StockCode", "InvoiceNo"])

retailDf.na.replace([""], ["UNKNOWN"], "Description")

# COMMAND ----------

#working with complex datatype

from pyspark.sql.functions import struct

complexDF = retailDf.select(struct("Description", "InvoiceNo").alias("complex"))

complexDF.createOrReplaceTempView("complexDF")

complexDF.select("complex.Description")

complexDF.select(col("complex").getField("Description"))


complexDF.select("complex.*")

# COMMAND ----------

arrayDf=retailDf.select(split(col("description")," ").alias("array_col"))

arrayDf


arrayDf.select(col("array_col")[0])


arrayDf.withColumn("checkcol",array_contains(col("array_col"), "WHITE")).where("checkcol")\
.select(explode(col("array_col")).alias("word")).where("word == 'WHITE'").groupBy(col("word")).count().orderBy(col("word").asc()).show(20,False)



# COMMAND ----------

from pyspark.sql.functions import create_map
retailDf.select(create_map(col("InvoiceNo"),col("Description")).alias("complex_map"))\
.show(2,False)

# COMMAND ----------

#udf Spark will serialize the function on the
#driver and transfer it over the network to all executor processes. 


def doublethenumber(num):
  if num % 2 == 0:
    return num*num
  else:
    return num*num*num

df=spark.range(10).toDF("num")

f=udf(doublethenumber)

df.select(f("num")).show()

#register for the spark

spark.udf.register("f1",doublethenumber,LongType())


df.select(expr("f1(num)")).show(5)


df.createOrReplaceTempView("dftab")


spark.sql("""select f1(num) from dftab""")

# COMMAND ----------

#Aggregations


df.select(count("num"),avg("num"),min("num"),max("num")).show()
df.describe().show()


# COMMAND ----------

finalRetailDf=spark.read.csv("/FileStore/tables/data/retail-data/*.csv",header=True,inferSchema=True).na.drop("any").coalesce(5)
finalRetailDf.rdd.getNumPartitions()
finalRetailDf.cache()
finalRetailDf.createOrReplaceTempView("finalRetailTab")

# COMMAND ----------

"""In addition to working with any type of values, Spark also allows us to create the following
groupings types:

The simplest grouping is to just summarize a complete DataFrame by performing an
aggregation in a select statement.

A “group by” allows you to specify one or more keys as well as one or more
aggregation functions to transform the value columns.

A “window” gives you the ability to specify one or more keys as well as one or more
aggregation functions to transform the value columns. However, the rows input to the
function are somehow related to the current row.

A “grouping set,” which you can use to aggregate at multiple different levels. Grouping
sets are available as a primitive in SQL and via rollups and cubes in DataFrames.
A “rollup” makes it possible for you to specify one or more keys as well as one or more
aggregation functions to transform the value columns, which will be summarized
hierarchically.

A “cube” allows you to specify one or more keys as well as one or more aggregation
functions to transform the value columns, which will be summarized across all
combinations of columns.
Each grouping returns a RelationalGroupedDataset on which we specify our aggregations.
"""
finalRetailDf.count()

# COMMAND ----------

finalRetailDf.groupBy("invoiceno").agg(count("CustomerID")).show()
                                      

# COMMAND ----------

finalRetailDf.groupBy("invoiceno","customerid")\
.agg(
count("customerid")).where("invoiceno == 537231").show(5)

# COMMAND ----------

#window
"""
A group-by takes data, and every row can go only into one grouping. A window function
calculates a return value for every input row of a table based on a group of rows, called a frame.
Each row can fall into one or more frames. A common use case is to take a look at a rolling
average of some value for which each row represents one day. If you were to do this, each row
would end up in seven different frames. We cover defining frames a little later, but for your
reference, Spark supports three kinds of window functions: ranking functions, analytic functions,
and aggregate functions
"""


#finalRetailDf.withColumn("date", to_date(col("InvoiceDate"), "yyyy-MM-dd H:mm")).show(2)

from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import dense_rank, rank
from pyspark.sql.window import Window

dfWithDate = finalRetailDf.withColumn("date", to_date(substring(col("InvoiceDate"),1,10), "yyyy-MM-dd"))
#dfWithDate.show(5)
dfWithDate.createOrReplaceTempView("dfWithDate")


windowSpec=Window.partitionBy("customerid","date").orderBy(desc("quantity"))

maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)



purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

# COMMAND ----------

finalRetailDf.show(5,False)

# COMMAND ----------

"""
Inner joins (keep rows with keys that exist in the left and right datasets)

Outer joins (keep rows with keys in either the left or right datasets)

Left outer joins (keep rows with keys in the left dataset)

Right outer joins (keep rows with keys in the right dataset)

Left semi joins (keep the rows in the left, and only the left, dataset where the key
appears in the right dataset)

Left anti joins (keep the rows in the left, and only the left, dataset where they do not
appear in the right dataset)

Natural joins (perform a join by implicitly matching the columns between the two
datasets with the same names)

Cross (or Cartesian) joins (match every row in the left dataset with every row in the
right dataset)

"""

# COMMAND ----------

person = spark.createDataFrame([
(0, "Bill Chambers", 0, [100]),
(1, "Matei Zaharia", 1, [500, 250, 100]),
(2, "Michael Armbrust", 1, [250, 100])]).toDF("id", "name", "graduate_program", "status")

graduateProgram = spark.createDataFrame([
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley")]).toDF("id", "degree", "department", "school")


sparkStatus = spark.createDataFrame([
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor")]).toDF("id", "status")

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

# COMMAND ----------

"""
person::
+---+----------------+----------------+---------------+
|id |name            |graduate_program|spark_status   |
+---+----------------+----------------+---------------+
|0  |Bill Chambers   |0               |[100]          |
|1  |Matei Zaharia   |1               |[500, 250, 100]|
|2  |Michael Armbrust|1               |[250, 100]     |
+---+----------------+----------------+---------------+

graduateProgram::
+---+-------+---------------------+-----------+
|id |degree |department           |school     |
+---+-------+---------------------+-----------+
|0  |Masters|School of Information|UC Berkeley|
|2  |Masters|EECS                 |UC Berkeley|
|1  |Ph.D.  |EECS                 |UC Berkeley|
+---+-------+---------------------+-----------+


sparkStatus::
+---+--------------+
|id |status        |
+---+--------------+
|500|Vice President|
|250|PMC Member    |
|100|Contributor   |
+---+--------------+

"""

# COMMAND ----------

# MAGIC %md https://www.oracletutorial.com/oracle-basics/oracle-joins/

# COMMAND ----------

joinExpression = person["graduate_program"] == graduateProgram['id']

joinType = "inner"
joinDF1=person.join(graduateProgram, joinExpression, joinType)

joinDF1.show()

joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()

joinType= "left_outer"
person.join(graduateProgram, joinExpression, joinType).show()


joinType = "right_outer"
person.join(graduateProgram, joinExpression, joinType).show()


# COMMAND ----------

joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

person.withColumnRenamed("id", "personId").join(sparkStatus, array_contains(col("spark_status"), sparkStatus["id"]), "inner").show()

# COMMAND ----------


