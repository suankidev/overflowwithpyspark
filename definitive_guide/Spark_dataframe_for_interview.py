# Databricks notebook source
flightdf = spark.read.format("csv").option('header',True).load('dbfs:/public/flight_data/*.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Working with Different Types 

# COMMAND ----------

retail_schema = """InvoiceNo string,
 StockCode string,
 Description string,
 Quantity int,
 InvoiceDate timestamp,
 UnitPrice double,
 CustomerID double,
 Country string"""

# COMMAND ----------

df = spark.read.format('csv').option('header',True).schema(retail_schema).load("dbfs:/public/retail_data/2010-12-01.csv")

# COMMAND ----------



# COMMAND ----------

df.dtypes

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###Working with boolean

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.where(col('invoiceno') != 536365).select('invoiceno','description').show(5,False)

# COMMAND ----------

from pyspark.sql.functions import instr
pricefilter = col('UnitPrice') > 600
descriptfilter = instr(col('description'),'POSTAGE') > 1

df.where(pricefilter | descriptfilter).where(col('stockcode').isin('DOT')).show()

# COMMAND ----------

    dotcode = col('stockcode') == 'DOT'
    unitprice = col('unitprice') > 600
    descriptfilter = instr(col('description'),'POSTAGE') >= 1

    df.withColumn('isExpensive', dotcode & (unitprice | descriptfilter)).where('isExpensive').show()

# COMMAND ----------

df.where(col('InvoiceNo') == None).count()

# COMMAND ----------

df.withColumn('isExpensive',col('stockcode') == '22632').where('isExpensive').show(10, False)

# COMMAND ----------

from pyspark.sql.functions import round, bround,lit
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

# COMMAND ----------

df.select(col('stockcode'),col('description'),col('description').eqNullSafe('NULL')).where(col('stockcode') == '22139').show(truncate=False)

# COMMAND ----------

display(df.describe())

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df.select(monotonically_increasing_id()).show()

# COMMAND ----------

from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()

# COMMAND ----------

from pyspark.sql.functions import lower, upper
df.select(col("Description"),
lower(col("Description")),
upper(lower(col("Description")))).show(2)

# COMMAND ----------

from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 6, "*").alias("lp"),
rpad(lit("HELLO"), 8, "*").alias("rp")).show(2)
