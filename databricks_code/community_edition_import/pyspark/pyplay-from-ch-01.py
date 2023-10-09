# Databricks notebook source


"""
 spark =SparkSession.builder().enableHiveSupport().AppName("test").getOrCreate()
 
 Spark Application:- Driver process and set of executors
 
 Driver process(user code and spark session): run main() function and resposible for three things
                   maintaining information about the Spark Application; responding to a user’s program or input;
                   and analyzing, distributing, and scheduling work across the executors 
                   
                   
                   Note: you control your Spark Application through a driver process called the SparkSession.
"""

spark



# COMMAND ----------

"""
partitions: To allow every executor to perform work in parallel, Spark breaks up the data into chunks called
partitions
Transformation:- you need to instruct Spark how you would like to
modify it to do what you want. These instructions are called transformations.Transformations are the core of how you express your business logic using Spark

   1. each input partition will contribute to only one output partition --Narrow


Note:
A wide dependency (or wide transformation) style transformation will have input partitions
contributing to many output partitions. You will often hear this referred to as a shuffle whereby
Spark will exchange partitions across the cluster. With narrow transformations, Spark will
automatically perform an operation called pipelining, meaning that if we specify multiple filters
on DataFrames, they’ll all be performed in-memory. The same cannot be said for shuffles. When
we perform a shuffle, Spark writes the results to disk

"""

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/data/fligt_data/

# COMMAND ----------

"""SparkApplication-->Dataframe-->partitions--->Transforamtion-->lazy Evaluation--->Action"""

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/data/fligt_data/2010_summary.csv

# COMMAND ----------

myRange=spark.range(100).toDF("number")
myRange.show()

# COMMAND ----------

aRange=spark.range(100).toDF("number")
#or 
print(type(spark.range(10)))

# COMMAND ----------

b=aRange.where("number % 2 == 0")

# COMMAND ----------

b.count()

# COMMAND ----------

from pyspark.sql.functions import col
df=spark.range(500).toDF("number")
df.select(df["number"],col("number")).show(10)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StringType,StructType, StructField

mycol=[Row(1,2,3),Row(4,5,6),Row(7,8,9)]
print(type(mycol))
spark.sparkContext.parallelize(mycol).toDF().show()
spark.createDataFrame(mycol,StructType([StructField("A",StringType(),True),StructField("B",StringType(),True),StructField("C",StringType(),True)])).show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

field=[StructField("name",StringType(),True,{'description':"unique  name"}),StructField("age",LongType(),True)]
schema1=StructType(field)
df = spark.createDataFrame([("joe", 34), ("luisa", 22)], ["first_name", "age"])
df.show()
df1 = spark.createDataFrame([("joe", 34), ("luisa", 22)], schema=schema1)
df1.show()

# COMMAND ----------

import pandas as pd
ps = spark

d = [(1,2),(3,4)]

df = ps.createDataFrame(d,['col1','col2'])

df.show()


pandadf = df.toPandas()

print(type(pandadf))

print(pandadf['col1'])


pddf= pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

print(pddf)



# COMMAND ----------



# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/data/fligt_data/2015_summary.csv

# COMMAND ----------

flightDF2015=spark.read.format("csv").option("inferSchema","true") \
.option("header","true") \
.load("dbfs:/FileStore/tables/data/fligt_data/2015_summary.csv")

# COMMAND ----------

flightSchema=flightDF2015.schema
spark.read.format("csv").schema(flightSchema).option("mode","dropmalformed").option("header","true").load("dbfs:/FileStore/tables/data/fligt_data/2015_summary.csv")

# COMMAND ----------

flightDF2015.sort("count").explain()

# COMMAND ----------

"""
We do not manipulate the physical data; instead, we configure physical execution characteristics
through things like the shuffle partitions parameter that we set
"""
spark.conf.set("spark.sql.shuffle.partitions","5")


flightDF2015.sort("count").explain()

# COMMAND ----------

#find top travel destination country
from pyspark.sql.functions import desc

flightDF2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)","destcount").sort(desc("destcount"))\
          .limit(5)\
          .show()

# COMMAND ----------

"""This execution plan is a directed acyclic graph (DAG) of transformations"""

# COMMAND ----------

from pyspark.sql.types import ByteType

b=ByteType()

print(type(b))



# COMMAND ----------

from pyspark.sql.functions import col,concat,expr,lit
flightDF2015.select(col("*"),expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME as tf")).show(5)

# COMMAND ----------

""" columns are expressions, but what is an expression? An expression is
a set of transformations on one or more values in a record in a DataFrame"""



# COMMAND ----------

#provide aggregarion with select expression
flightDF.selectExpr("avg(count)","count(count)").show()

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","false")
flightDF.selectExpr("dest_country_name").show()

# COMMAND ----------

flightDF.select(expr("count").cast("long"))

# COMMAND ----------

flightDF.where(col("dest_country_name") == "United States").show(5)

# COMMAND ----------

# in Python
seed = 5
withReplacement = False
fraction = 0.5
sampleData=flightDF.sample(withReplacement, fraction, seed)

# COMMAND ----------

#concating and appending
rdd1=spark.sparkContext.parallelize([Row("sujeet",38378,3),Row("rohan",34374,5)])
rdd2=spark.sparkContext.parallelize([Row("rohit",5555,1.3),Row("amit",34374,4.2)])

df1=spark.createDataFrame(rdd1,["name","sal","exp"])
df2=spark.createDataFrame(rdd2,["name","sal","exp"])

df3=df1.union(df2)

# COMMAND ----------

from pyspark.sql.functions import desc,asc
df3.printSchema()
df3.orderBy(col("exp").desc()).show()

# COMMAND ----------

flightDF.sort("count").explain()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions","5")
flightDF.sort("count").explain()

# COMMAND ----------

flightDF.createOrReplaceTempView("VFLIGHTDF")

# COMMAND ----------

spark.sql("""
select * from VFLIGHTDF limit 10""").show()

# COMMAND ----------

flightDF.take(5)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col,desc

flightDF.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)","total_count").sort(col("total_count").desc()).limit(5).show()

# COMMAND ----------

flightSchema1=StructType([
  StructField("dest",StringType(),True),
  StructField("origin",StringType(),True),
  StructField("count",LongType(),True)
])
flightDFSortedWithPart=spark.read.schema(flightSchema1).option("header","true").csv("/FileStore/tables/data/fligt_data/2011_summary.csv").sortWithinPartitions("count")
print(flightDFSortedWithPart.rdd.getNumPartitions())
flightDFSortedWithPart.show(5)

# COMMAND ----------

from pyspark.sql.functions import col
flightDfpartioned=flightDF.repartition(5,col("DEST_COUNTRY_NAME"))
print(flightDfpartioned.rdd.getNumPartitions())

# COMMAND ----------

#flightDfpartioned.write.parquet("/FileStore/tables/data/fligt_data/part_data/")

# COMMAND ----------

collectDF=flightDfpartioned.coalesce(2)

# COMMAND ----------

collectDF1 = collectDF.limit(10)
collectDF1.take(5) # take works with an Integer count
collectDF1.show() # this prints it out nicely
collectDF1.show(5, False)
collectDF1.collect()

# COMMAND ----------

#workin with different type of data
"""
Booleans
Numbers
Strings
Dates and timestamps
Handling null
Complex types
User-defined functions
"""

# COMMAND ----------

spark.sql("""select * from VFLIGHTDF""").show(5)

# COMMAND ----------

byDayDF=spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/data/fligt_data/2010_12_01.csv")

# COMMAND ----------

from pyspark.sql.functions import *
bydaydf=byDayDF.withColumn("date",substring(col("InvoiceDate"),1,10))
bydaydf.createOrReplaceTempView("VBYDAYDFTAB")

# COMMAND ----------

from pyspark.sql.functions import *
bydaydf.where(col("InvoiceNo")==536365).select("*").show(5,False)

# COMMAND ----------

# |  &
bydaydf.where(bydaydf.StockCode.isin("DOT")).where((col("UnitPrice") >= 600) | (col("Quantity") > 5) ).show(5)

# COMMAND ----------

from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
bydaydf.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)

# COMMAND ----------

from pyspark.sql.functions import expr
bydaydf.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
.where("isExpensive")\
.select("Description", "UnitPrice").show(5)

# COMMAND ----------

bydaydf.describe().show()

# COMMAND ----------

bydaydf.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
.where("isExpensive")\
.select("Description", "UnitPrice").show(5)

# COMMAND ----------

bydaydf.where(col("CustomerID").eqNullSafe(None) == False).show(5)

#bydaydf.withColumn("test",col("customerID").eqNullSafe(None)).show(5)

# COMMAND ----------

bydaydf.count()

# COMMAND ----------

dfmobile=spark.createDataFrame([Row("99191186"),Row("7355609142")],["mobile"])
dfmobile.show()
df.show()

# COMMAND ----------

from pyspark.sql.functions import lower,upper,ltrim,rtrim,rpad,lpad,trim,lit,col
m=dfmobile.mobile
finalDf=df.withColumn("msg",lit("   hi   ")).withColumn("greeting",lit("lower case letter")).withColumn("mobile",lit("7355609142"))\
.withColumn("istrue",length(col("mobile")) == 10).where("istrue")\
.select("*",trim(col("msg")), initcap(col("greeting")), lpad(col("mobile"),12,"91"))

# COMMAND ----------

#regex
my_regex="RED|BLAC|GREEN|WHITE"
bydaydf.select(
        (regexp_replace(col("Description"),my_regex,"COLOR")).alias("cleaned"),
        col("Description"),
        (regexp_extract(col("Description"),my_regex,0)).alias("extracted"),
        (col("description").contains("WHITE")).alias("desc"),
        ((instr(col("description"),"WHITE") >= 1 ) | (instr(col("description"),"BLACK") >= 1)).alias("isther")
).show(5)

# COMMAND ----------

from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]

def color_locator(column, color_string):
    return locate(color_string.upper(), column)\
    .cast("boolean")\
    .alias("is_" + color_string)

selectedColumns = [color_locator(bydaydf.Description, c) for c in simpleColors]

selectedColumns.append(expr("*")) # has to a be Column type

bydaydf.select(*selectedColumns).where(expr("is_white OR is_red")).select("Description").show(3, False)


# COMMAND ----------

#Working with Dates and Timestamps

bydaydf.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp

dateDf=spark.range(10).withColumn("today",current_date()).withColumn("now",current_timestamp())

# COMMAND ----------

dateDf.printSchema()

dateDf.show(5,False)

# COMMAND ----------

from pyspark.sql.functions import datediff,months_between,date_sub,date_add,current_timestamp,current_date

finaldateDf=dateDf.withColumn("weekAgo",date_sub(col("today"),7)).\
select(col("*"),date_sub(col("today"),5),date_add(col("today"),5),datediff(col("today"),col("weekAgo"))).withColumn("olddate",to_date(lit("2021-01-02")))

# COMMAND ----------

finaldateDf.select(months_between(col("olddate"),col("weekAgo"),True)).show()

# COMMAND ----------

finaldateDf.select(to_timestamp(col("olddate"),"yyyy-dd-MM HH:mm:ss")).show()


# COMMAND ----------

#Working with Nulls in Data

moviesDF=spark.read.options(header=True,inferSchema=True,mod="dropmalformed").json("/FileStore/tables/spark_input/movies.json")

# COMMAND ----------

moviesDF.printSchema()
#return first column if not null otherwise return null only if both column are null
movies=moviesDF.select(coalesce(col("Director"),col("source"))).show(20,False)

# COMMAND ----------

moviesDF.na.drop() #by defualt any

finalmovieDF=moviesDF.select("Title","Director","Major_Genre","Release_Date","IMDB_Rating")
finalmovieDF.na.drop("all",subset=["director","Major_Genre"]).show(5,False)
finalmovieDF.na.drop("all").show(5)  #if all column are null
finalmovieDF.na.drop("any").show(5) #if any column are null

# COMMAND ----------

finalmovieDF.na.fill("null is replaced with this").show(10,False)    #imdb_rating which is other than stringtype is not replaced

# COMMAND ----------

fill_col_value={"Director":"Uknown","Major_Genre":"not defined","imdb_rating":0.0}
finalmovieDF.na.fill(fill_col_value).show(5,False)

# COMMAND ----------

finalmovieswithoutnullDF=finalmovieDF.na.fill(fill_col_value).na.replace("Uknown"," ","Director").orderBy(col("IMDB_Rating").desc_nulls_first())
#replaced uknown with space
finalmovieswithoutnullDF.show(5,False)

# COMMAND ----------

#Working with complex data type

"""
There are three kinds of complex types: struct, array and map.
1. You can think of structs as DataFrames within DataFrames.
"""

complexDF=finalmovieswithoutnullDF.select(struct("Title","Director").alias("complexType"))

complexDF.createOrReplaceTempView("complexDFV")

# COMMAND ----------

complexDF.select("complexType.Title","complexType.director").show(5)

complexDF.select(col("complexType").getField("title")).show(5)

spark.sql("select  complexType.Title , complexType.* from  complexDFV limit 10").show(5,False)

# COMMAND ----------

"""
split
We do this by using the split function and specify the delimiter:
return #DataFrame[split(Director,  , -1): array<string>]

SELECT split(Description, ' ')[0] FROM dfTable
"""

from pyspark.sql.functions import split

finalmovieswithoutnullDF.select(split(col("Director")," ").alias("arraycol")).select(expr("arraycol[0]")).show(5,False)
finalmovieswithoutnullDF.select(size(split(col("Director")," "))).show()

# COMMAND ----------

#finalmovieswithoutnullDF

finalmovieswithoutnullDF.select(col("title"),array_contains(split("Director"," "),"Steven")).show(5,False)

# COMMAND ----------

finalmovieswithoutnullDF.withColumn("splitted",split("Director"," ")).withColumn("exploded",explode("splitted")).select("exploded").show(5)

finalmovieswithoutnullDF.withColumn("splitted",split("Director"," ")).withColumn("exploded",explode("splitted")).select("*").show(5,False)


# COMMAND ----------

from pyspark.sql.functions import create_map
#require even no of col i.g. argument means we can't select 3 column or 5 or so
finalmovieswithoutnullDF.select(create_map(col("title"),col("imdb_rating")).alias("mapofcol"))\
.select(expr("mapofcol['The Godfather']")).show(5)


# COMMAND ----------

finalmovieswithoutnullDF.na.replace(" ","Uknown",subset=["director"]).withColumn("maped",create_map(col("director"),col("title")))\
.select(explode("maped")).show(5,False)
                                                                                                

# COMMAND ----------


