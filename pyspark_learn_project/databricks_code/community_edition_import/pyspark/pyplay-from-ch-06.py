# Databricks notebook source
moviesDF=spark.read.options(inferSchema=True,header=True).csv("/FileStore/tables/moviesdata/movies.csv")

# COMMAND ----------

moviesDF.schema
moviesDF.show(5)

# COMMAND ----------

from pyspark.sql.functions import *
jsonDF = spark.range(5).selectExpr("""'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

# COMMAND ----------

jsonDF.select(get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]").alias("column"),\
              json_tuple(col("jsonString"), "myJSONKey")).show(5,False)

# COMMAND ----------

#convering struct type to json
from pyspark.sql.functions import to_json,col

moviesDF.selectExpr("struct(title,genres) as myStruct")\
.select(to_json(col("myStruct"))).show(5,False)

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import *

parseSchema = StructType((
StructField("title",StringType(),True),
StructField("genres",StringType(),True)))

moviesDF.select(expr("struct(title, genres) as myStruct"))\
.select(to_json(col("myStruct")).alias("newJSON"))\
.select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2,False)

# COMMAND ----------

"""One of the most powerful things that you can do in Spark is define your own functions. These
user-defined functions (UDFs) make it possible for you to write your own custom
transformations using Python or Scala and even use external libraries.By default, these functions are registered as temporary functions to be used in that specific
SparkSession or Context.Thus far, our
expectations for the input are high:"""

# COMMAND ----------

from pyspark.sql.functions import col
def pow3(num):
  return num**3

udfEx=spark.range(5).toDF("number")

udfEx.select(col("number")).show()
print(pow3(10))

from pyspark.sql.functions import udf
powerto3=udf(pow3)
udfEx.select(powerto3(col("number"))).show()


#register to use in sql
"""this is how it will be available to spark.sql When you want to optionally return a value from a UDF, you should return None in Python and
an Option type in Scala:"""
from pyspark.sql.types import IntegerType, DoubleType
spark.udf.register("power3py", pow3, LongType())
udfEx.select(expr("power3py(number)")).show()


# COMMAND ----------

moviesDF.printSchema()

# COMMAND ----------

moviesDF.count()

#
moviesDF.na.drop("any").show()

# COMMAND ----------

from pyspark.sql.functions import split,col,explode

moviesDF.withColumn("splitted",split(col("genres"),"\|")).show(6,False)
d=moviesDF.withColumn("splitted",split(col("genres"),"\|")).withColumn("a",array_contains(col("splitted"),"Children")).where(col("a"))


# COMMAND ----------

#find movies which are Action and Adventure only
from pyspark.sql.functions import array_contains
moviesDF.withColumn("splitted",split(col("genres"),"\|")).where(array_contains(col("splitted"),"Adventure") & array_contains(col("splitted"),"Action")).show(5,False)

# COMMAND ----------

"""
Spark Cache and persist are optimization techniques for iterative and interactive Spark applications to improve the performance of the jobs or applications

"""

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct

retailDF=spark.read.options(inferSchema=True,header=True,mode="dropmalformed").csv("dbfs:/FileStore/tables/data/retail-data/*.csv").coalesce(2)
retailDF.na.drop("any").count()
retailDF.cache()
retailDF.rdd.getNumPartitions()
retailDF.select(count("*")).show()

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct
from pyspark.sql.functions import first,last
from pyspark import StorageLevel

retailDF=spark.read.options(inferSchema=True,header=True,mode="dropmalformed").csv("dbfs:/FileStore/tables/data/retail-data/*.csv").coalesce(2)
retailDF.na.drop("any").count()
retailDF.persist(StorageLevel.MEMORY_ONLY)
retailDF.rdd.getNumPartitions()
retailDF.select(count("*")).show()
retailDF.select(
first("StockCode"),last("StockCode"),
).show(5,False)


# COMMAND ----------

from pyspark.sql.functions import collect_set,collect_list,expr,col,avg,sum
from pyspark.sql.types import StringType,BooleanType

def printCountry(country):
   return country
  
spark.udf.register("printCountry", printCountry, BooleanType())
  
retailDF.agg(collect_list("Country"),collect_set("Country"))

retailDF.select(count("*"),count("stockcode"),avg("Unitprice"),sum("quantity"),).show(5)



# COMMAND ----------

from pyspark.sql.functions import col
retailDF.select(col("stockcode")).distinct().count()

retailDF.groupBy(col("stockcode"),col("invoiceno")).count().show(5)

retailDF.groupBy(col("stockcode")).agg(
  count("stockcode"),
  count("CustomerID")
).show(5)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/data/productrevenu.csv

# COMMAND ----------

#window function
prodrev=spark.read.csv("/FileStore/tables/data/productrevenu.csv",header=True)
prodrev.show(10)
prodrev.createOrReplaceTempView("prodrevtab")

""" want to answer two questions:
    1. What are the best-selling and the second best-selling products in every category?
    2. What is the difference between the revenue of each product and the revenue of the best-selling product in the same category of that product?"""

# COMMAND ----------

prodrev.groupBy("category").count().show()

# COMMAND ----------

spark.sql("""
         with temp as( select product,category,revenue, dense_rank(revenue) over(partition by category order by revenue desc) as rnk from prodrevtab)
         select * from temp 
          """).show()

# COMMAND ----------

"""
    First, the PARTITION BY clause divided the products into multiple partitions by category.
    Then, the ORDER BY clause sorted the rows in each partition by list price in descending order.
    Finally, the RANK() function calculated the rank for each row in each partition. It re-initialized the rank for each partition."""

# COMMAND ----------

from pyspark.sql.window import Window

retailDF.show(5,False)

retailDF.na.drop("any").createOrReplaceTempView("retailDFtab")

# COMMAND ----------

spark.sql("""
select InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country, dense_rank() over(partition by CustomerID,InvoiceDate order by Quantity desc nulls last) as pricerank  from retailDFtab 
""").show(20)

# COMMAND ----------

retailDF.where(expr("country == 'United Kingdom'")).select(count("country")).show()

# COMMAND ----------


