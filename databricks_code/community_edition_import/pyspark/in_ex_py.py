# Databricks notebook source


"""
#first part:
spark:
    rdd
    structtured api
    theory of spark


#second part:
sql:


#third part:
project explaination:

#group by https://www.youtube.com/watch?v=Nk2I5zgEeHc

"""


# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('param_name',"defaullt_val")
v_data_source = dbutils.widgets.get('param_name')

# COMMAND ----------

v_data_source

# COMMAND ----------



spark.conf.set("spark.sql.autoBroadcastJoinThreshold",10240)
spark.conf.set("spark.sql.shuffle.partition",200)


partitions
transformation --> narrow wide
    narrow:  where  one-->one   -->(With narrow transformations, Spark will
                                        automatically perform an operation called pipelining ,
                                         in memory processing)
    wide:    groupBy  one==>many 
    
    
Action: An action instructs Spark to compute a result from a series of transformations.


physical plan:  flightData2015.sort("count").explain()


The logical plan of transformations that we build up defines a lineage for the DataFrame so that
at any given point in time, Spark knows how to recompute any partition by performing all of the
operations it had before on the same input data.


This execution plan is a directed acyclic graph (DAG) of transformations,
each resulting in a new immutable DataFrame, on which we call an action to generate a result

Spark lazily executes a DAG of transformations in order to optimize the
execution plan on DataFrames

---->
1. Write DataFrame/Dataset/SQL Code.
2. If valid code, Spark converts this to a Logical Plan.
3. Spark transforms this Logical Plan to a Physical Plan, checking for optimizations along
the way.
4. Spark then executes this Physical Plan (RDD manipulations) on the cluster.


logical plan:  (usercode -> unresolved logical plan --catalog(analyser)-->rsolved logicl-->optimized plan)    ===> physical plan  based on cast model   
====> execute on cluster(on low level RDD)





 


parquet:  open source,  self describing(metadata, shcema), columnur storage


spark.sql.parquet.mergeSchema


spark2-submit \
--master yarn \
--deploy-mode cluster \
--conf "spark.sql.shuffle.partitions=20000" \
--conf "spark.executor.memoryOverhead=5244" \
--conf "spark.memory.fraction=0.8" \
--conf "spark.memory.storageFraction=0.2" \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.files.maxPartitionBytes=168435456" \
--conf "spark.dynamicAllocation.minExecutors=1" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.dynamicAllocation.enabled=true" \
--files /path/log4j.properties,/path/file2.conf,/path/file3.json \
--py-files file1.py,file2.py
spark-application.py

-->

--conf "spark.sql.files.maxPartitionBytes="
--conf "spark.executor.instances=4"
--conf "spark.executor.memory=2GB"
--conf "spark.driver.memory=20GB"




df.persist(StorageLevel.<level_value>)



spark.executor.memoryOverhead





# COMMAND ----------

spark

# COMMAND ----------

df = spark.range(100)
from pyspark.sql.types import StructType, StructField, StringType, Row, IntegerType
from pyspark.sql.functions import col

sch = StructType([StructField('num',IntegerType(),False)])
def basic_rdd(): 
    myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple Spark".split(" ")    
    #print(myCollection)
    
    words = spark.sparkContext.parallelize(myCollection)
    
    print(words.collect())
    
    #find if it's start with S
    d = words.filter(lambda x: x.startswith('S'))
    
    #map the input according to  the o/p
    d = words.map(lambda x: x) ##['Spark', 'The', 'Definitive', 'Guide', ':', 'Big', 'Data', 'Processing', 'Made', 'Simple']
    d = words.map(lambda x: x.split(' '))   ###[['Spark'], ['The'], ['Definitive'], ['Guide'], [':'], ['Big'], ['Data'], ['Processing'], ['Made'], ['Simple']]
    #print(d.collect())
    #print(d.toDF().show())

    d = words.flatMap(lambda x: x)
    #print(d.collect())
    
    d = words.flatMap(lambda x: x).map(lambda x: x.split(' '))
    #print(d.collect())
    
    
    #short the words according to letter
    d = words.sortBy(lambda x: len(x))
    #print(d.collect())
    
    d = words.map(lambda x: (len(x), x))
    d = d.sortByKey()
    #print(d.collect())
    
    #find largest word
    d = words.reduce(lambda x,y: x if len(x)>len(y) else y)
    #print(d)
    
    #find how many times Spark came
    d=words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
    print(d.collect())

#basic_rdd()


def basic_one():
    myschema = StructType([StructField("name", StringType(), False), StructField("Age", StringType(), False)])
    names = [Row("sujeet", "30"), Row("Ankita", "24")]
    spark.createDataFrame(names, myschema).show()
    spark.createDataFrame(names, ["name", "age"]).show()
    spark.sparkContext.parallelize(names).toDF(["name", "age"]).show()  

basic_one()


# COMMAND ----------

file_location = "/FileStore/tables/retail-data/2010_12_08.csv"
random_file = "dbfs:/FileStore/tables/tstfile.csv"


#display(dbutils.fs.ls("/FileStore/tables/"))
 
#comma seprated file

def get_rdd(file):
  return spark.sparkContext.textFile(file)



#it will load the file in list, based on new line character
retail_rdd = get_rdd(file_location)
random_rdd = get_rdd(random_file)

#print(retail_rdd.take(5))
#print(random_rdd.collect())


#remove the header from file
retail_header = retail_rdd.first()
final_retail_file = retail_rdd.filter(lambda x: x != retail_header)
#print(final_retail_file.first())

#find records where country is Norway
d = final_retail_file.map(lambda x: x.split(","))
#print(d.collect())
d = d.map(lambda x: (x[0], x[7],x[7] == 'Norway'))
d = d.filter(lambda x: x[2] != False)

#print(d.take(5))

from pyspark.sql.functions import trim
#calculate no of times Kharadi came in the random_rdd file

#print(random_rdd.collect())

#split records
d = random_rdd.flatMap(lambda x: x.split(","))  #['val|name', '1257.577|A', '3683648.3837|B', '', '', 'EON Free Zone', ' Phase 2', ' Kharadi' ....
d = d.map(lambda x: (x.strip(),1))
d = d.reduceByKey(lambda x,y:x+y)

#print(d.collect())

#print(d.filter(lambda x: x[0] == 'Kharadi').collect())


#d.saveAsTextFile("dbfs:/FileStore/tables/test_file.csv")

# COMMAND ----------

"""Checkpoint is a mechanism where every so often Spark streaming application stores data and metadata in the fault-tolerant file system.
So Checkpoint stores the Spark application lineage graph as metadata and saves the application state in a timely to a file system. 
"""

def caching_and_storage_level():
        my_collection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
        coll_rdd = spark.sparkContext.parallelize(my_collection, 2)

        print(f"checkpoint: {coll_rdd.getCheckpointFile()},"
              f" storage_level:{coll_rdd.getStorageLevel()},"
              f"num_of_partition:{coll_rdd.getNumPartitions()}")
        """org.apache.spark.storage.StorageLevel"""
        coll_rdd.cache()
        spark.sparkContext.setCheckpointDir("dbfs:/FileStore/tables/checkping_dir")
        coll_rdd = coll_rdd.repartition(4)
        coll_rdd.checkpoint()

        print(f"checkpoint: {coll_rdd.getCheckpointFile()},"
              f" storage_level:{coll_rdd.getStorageLevel()},"
              f"num_of_partition:{coll_rdd.getNumPartitions()}")
  
caching_and_storage_level()



# COMMAND ----------

def key_value_based_rdd():
    my_coll = "Spark The Definitive Guide : Big Data Processing Made Simple" \
        .split(" ")
    words = spark.sparkContext.parallelize(my_coll, 2)

    pair_rdd_word = words.map(lambda x: (x.lower(), 1))
    words_key_by = words.keyBy(lambda x: x.lower()[0])

    #print(words_key_by.collect())
    #print(pair_rdd_word.collect())

    words_key_by = words_key_by.mapValues(lambda x: x.upper())

    #print(words_key_by.collect())

    flat_map_value = words_key_by.flatMapValues(lambda x: x.upper())
    #print(flat_map_value.collect())

    #print(words_key_by.keys().collect())
    #print(words_key_by.values().collect())

    #print(words_key_by.lookup('S')
                    
key_value_based_rdd()

# COMMAND ----------

file_location = "/FileStore/tables/retail-data/2010_12_08.csv"

d = spark.sparkContext.textFile(file_location)

h = d.first()

d = d.filter(lambda x: x != h)
d = d.map(lambda x: x.split(","))
#display(d.take(5))
temp = d

#d = d.map(lambda x: (x[7],x[:7]))
d = d.map(lambda x: (x[7],x[0]))
grp = d.groupBy(lambda x: x[0]).mapValues(list)

#print(grp.collect())


grp = temp.map(lambda x: (x[7], x[0])).groupByKey()

print(grp.collect())
#print(grp.mapValues(min).collect())
# d = grp.map(lambda x: (x[0], list(x[1])))


# #print(d.collect())

# d = d.groupByKey()
# d = d.map(lambda x: (x[0],list(x[1])))

# #print(d.collect())





# COMMAND ----------

"spark datastructure API"

from  pyspark.sql.functions import *
from pyspark.sql.types import *

dfschema = StructType([StructField('origin', StringType(), False),
                     StructField('destination', StringType(), False),
                     StructField('total', IntegerType(), True)])

data_one = [Row('India', 'Bhutan', 3), Row('India', 'Canada', 4)]
data_two = [Row('Canada', 'India', 5), Row('Bhutan', 'Nepal', 3), Row('India', 'Canada', 6)]

data_three = [Row('India', 'Canada', 4)]

df_one = spark.createDataFrame(data_one, dfschema)
df_two = spark.createDataFrame(data_two, dfschema)
df_three = spark.createDataFrame(data_three, dfschema)

# df_one.show()
# df_two.show()

uniondf = df_one.union(df_two)

uniondf.union(df_three).show()  #in spark union does not remove duplicate column like other framework

uniondf.union(df_three).dropDuplicates().show()

uniondf.union(df_three).distinct().show()





# COMMAND ----------

df = uniondf.union(df_three)

df.sort('total').show()  #within partiton not guarantee of order

df.orderBy(col('total')).show()

# COMMAND ----------

# The method toLocalIterator collects partitions to the driver as an iterator
test = uniondf.toLocalIterator()

for i, j in enumerate(test):
   print(i,j, j[0])

# COMMAND ----------

""" flightData2015 = spark.read.option("header", "true").schema(flight_schema).format("csv").load(file_path) \
        .sortWithinPartitions(col('totalflight'))
        """ #to spark tuning




# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/retail-data/*.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","
schema_csv ="invoiceno int, stockcode string , description string, quantity int, invoicedate timestamp, unitprice float, customerid string , country string"
# The applied options are for CSV files. For other file types, these will be ignored.
retail_df = spark.read.format(file_type) \
  .option("inferSchema", False) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .schema(schema_csv)\
  .load(file_location)

retail_df.show(5, truncate=False)

retail_df.dtypes

# COMMAND ----------

from pyspark.sql.functions import lit
df = retail_df.select(lit(5), lit('five'), lit(5.0))
df.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import instr,col

'''Working with Booleans'''
df = retail_df.where(col('InvoiceNo') == 536365).select(col('InvoiceNo'), col('Description'))

price_filter = col('UnitPrice') > 600
descriptor_filter = instr(col('description'), 'POSTAGE') >= 1

#retail_df.show(5)
df = retail_df.where(col('StockCode').isin('DOT')).where(price_filter | descriptor_filter)

#df.show(5, False)

dot_code_filter = col("StockCode") == 'DOT'

retail_df.withColumn('isExpensive', dot_code_filter & (price_filter | descriptor_filter)) \
   .where(col('isExpensive'))#.show(5, False)

stock_code = {'DOT', '71053', 'test'}

retail_df.where(col('StockCode').isin(*stock_code)).show(5, False)

retail_df.where(~ (dot_code_filter & (price_filter | descriptor_filter))).\
 select('description', 'unitprice').show()



# COMMAND ----------

#working with string

'''initcap to capatalise '''

from pyspark.sql.functions import initcap, lower, upper
df = retail_df.select(initcap(col('Description')), lower(col('description')), upper(col('description')))
df.show(2, False)

from pyspark.sql.functions import ltrim, rtrim, trim, rpad, lpad

'''Note that if lpad or rpad takes a number less than the length of the string, 
it will always remove 
values from the right side of the string'''

df = retail_df.select(ltrim(lit('    hello    ')), rpad(lit('Hello'), 8, '--'))
df.show(2, False)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, regexp_extract, translate
regex_string = 'BLACK|WHITE|RED|GREEN|BLUE'

extract_string_one = '(^WHITE)'   #grouping can be  form as re module in python

retail_df.show(2,truncate=False)
# find regex_string  in description column and replace it with 'COLOR' value
df = retail_df.withColumn('test',regexp_replace(col('Description'), regex_string, 'COLOR').alias('color_clean'))

df.show(5, False)

# replace given characters with other characters

#df = retail_df.select(translate(col('description'), 'LEET', '1337'))

#df.show(5, False)



# COMMAND ----------

# pulling out first mention color
'''Extract a specific group matched by a Java regex, from the specified string column. 
If the regex did not match, or the specified group did not match, an empty string is returned'''

extract_string = '(BLACK|WHITE|RED|GREEN|BLUE)'

df = retail_df.select(regexp_extract(col('description'), extract_string, 1).alias('extract_column'),
                      col('description'))

df.show(5, False)

df = retail_df.select(regexp_extract(col('description'), extract_string_one, 1).alias('extract_column'),
                      col('description'))

#df.show(5, False)

'''check if the description contain the value BLACK or WHITE'''

filter_black = instr(col('description'), 'BLACK') >= 1
filter_white = instr(col('description'), 'RED') >= 1

df = retail_df.withColumn('has_simple_color', (filter_black | filter_white)).where('has_simple_color') \
    .select('description')

#df.show(5, False)



# COMMAND ----------

'''what if we have multiple value to find in a column , if exist or not'''
from pyspark.sql.functions import locate,expr, regexp_extract
simple_color = ['black', 'red', 'white', 'green', 'blue']

def color_locator(column, color_string):
    return locate(color_string.upper(), column) \
        .cast("boolean") \
        .alias("is_" + color_string)

selected_columns = [color_locator(retail_df.Description, c) for c in simple_color]

selected_columns.append(expr("*"))  # has to a be Column type

df = retail_df.select(*selected_columns).where(expr("is_white OR is_red")) \
    .select("Description")

#df.show(3, False)

df = retail_df.select(locate('RED', col('description')).alias('description_located'), col('description'))
#df.show(5, False)

simple_color = ['black', 'red', 'white', 'green', 'blue']

pattern =  "("+'|'.join(simple_color).upper()+")"
print(pattern)

#or
df = retail_df.withColumn('check', regexp_extract(col('description'), pattern, 0)).filter(col('check') != '')
df.show(5, truncate=False)

#df.dtypes


# COMMAND ----------

#working with date and time
from pyspark.sql.functions import unix_timestamp, to_date, current_date, current_timestamp, date_sub, date_add , months_between
from pyspark.sql.functions import col, datediff, lit         

date_df = spark.range(10) \
  .withColumn("today", current_date()) \
  .withColumn("now", current_timestamp())

#date_df.printSchema()
#date_df.show(truncate=False)

date_df.select(date_sub(col("today"), 5), date_add(col("today"), 5))

date_df.withColumn("week_ago", date_sub(col("today"), 7)) \
  .select(datediff(col("week_ago"), col("today")))

date_df.select(
  to_date(lit("2016-01-01")).alias("start"),
  to_date(lit("2017-05-22")).alias("end")
).select(col('start'), col('end'), months_between(col("start"), col("end")))#.show(1)

"""Unix time is also known as Epoch time which specifies the moment in time since 1970-01-01 00:00:00 UTC
unix_timestamp() – Converts Date and Timestamp Column to Unix
https://sparkbyexamples.com/pyspark/pyspark-sql-working-with-unix-time-timestamp/#:~:text=In%20PySpark%20SQL%2C%20unix_timestamp%20%28%29%20is%20used%20to,UTC%29%20to%20a%20string%20representation%20of%20the%20timestamp.

yyyy-mm-dd
"""
inputData = [("2019-07-01 12:01:19",
            "07-01-2019 12:01:19",
            "07-01-2019")]
columns = ["timestamp_1", "timestamp_2", "timestamp_3"]
df = spark.createDataFrame(
  data=inputData,
  schema=columns)
# df.printSchema()
df.show(truncate=False)

"""convert timestamp to unix timestamp"""
df2 = df.select(
  unix_timestamp(col("timestamp_1")).alias("timestamp_1"),
  unix_timestamp(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
  unix_timestamp(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
  unix_timestamp().alias("timestamp_4")
)
#df2.printSchema()
df2.show(truncate=False)



# COMMAND ----------

# Convert; Unix; timestamp; to; timestamp

df3 = df2.select(
  from_unixtime(col("timestamp_1")).alias("timestamp_1"),
  from_unixtime(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
  from_unixtime(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
  from_unixtime(col("timestamp_4")).alias("timestamp_4")
)
# df3.printSchema()
df3.show(truncate=False)

# Spark will not throw an error if it cannot parse the date; rather, it will just return null

dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
  to_date(lit("2017-12-11"), dateFormat).alias("date"),
  to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.show(5, False)

cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
"""After we have our date or timestamp in the correct format and type, comparing between them is
actually quite easy. We just need to be sure to either use a date/timestamp type or specify our
string according to the right format of yyyy-MM-dd if we’re comparing a date"""



# COMMAND ----------


"""always use nulls to represent missing or empty data in your
DataFrames. Spark can optimize working with null values more than it can if you use empty
strings or other values. """


retail_df.select(coalesce(col('description'), col('customerid')))
# .show(5, False)

# ifnull, nullIf, nvl, and nvl2

spark = get_spark_session()

# drop function to drop value

retail_df.na.drop()
retail_df.na.drop("any")  # drop if any value is null in a row
retail_df.na.drop("all")  # drop if all values in a row in null

retail_df.na.drop("all", subset=['description', 'customerid'])

# fill all value to a string if it's a null
retail_df.na.fill("replace value")
fill_cols_value = {"StockCode": 5, "Description": "No Value"}
retail_df.na.fill(fill_cols_value)

# replace value with other
retail_df.na.replace([""], ["UNKNOWN"], "Description")

# Ordering
# asc_nulls_first, desc_nulls_first, asc_nulls_last, or desc_nulls_last t

# COMMAND ----------



# COMMAND ----------


"""There are three kinds of complex types: structs, arrays,
and maps."""


#retail_df.show(5)

from pyspark.sql.functions import struct, array_contains, create_map, expr, split, size, explode,concat_ws, when

#retail_df.show(5, False)
complex_df = retail_df.select(struct(col('description'), col('customerid')).alias('complex'), col('*'))
#complex_df.show(5, truncate=False)
complex_df.select(col('complex').getField('customerid'))  # .show(5, False)
complex_df.select(expr("complex.*"))
# .show(5, False)


"""our objective is to take
every single word in our Description column and convert that into a row in our DataFrame"""

complex_ar = retail_df.select(split(col('description'), " ").alias('splitcol'), col('description'),
                            col('customerid')).withColumn('exploded',explode(col('splitcol')))\


complex_ar.show(5, truncate=False)


#how to unexplod
complex_ar.groupBy(col('splitcol')).agg(collect_set(col('exploded'))).show(5,truncate=False)

# complex_ar.withColumn('test', dense_rank().over(Window.partitionBy('splitcol').orderBy('exploaded')))

# \.show()

                      
#.groupBy('description')

#complex_ar.agg(col('description'), col('')).show()

#complex_ar.select(expr("splitcol[0]"), size(col('splitcol'))).show(1)

# # array contain
#complex_ar.select(array_contains(col('splitcol'), 'WHITE').alias('arrycontain')).show(5, truncate=False)

#explode work like flatMapValues of rdd
#complex_ar.select(col("*"), explode(col('splitcol'))).show(10, False)

# retail_df.select(create_map(col('description'), col('invoiceno')).alias('complex_map')) \
#   .select(col('complex_map')['WHITE METAL LANTERN']).show()

# COMMAND ----------

"""it support
ranking function --> row_number(), rank(), dense_rank(), percent_rank(), 
analytic function
aggregate function
"""
simple_data = [("James", "Sales", 3000),
 ("Michael", "Sales", 4600),
 ("Robert", "Sales", 4100),
 ("Maria", "Finance", 3000),
 ("James", "Sales", 3000),
 ("Scott", "Finance", 3300),
 ("Jen", "Finance", 3900),
 ("Jeff", "Marketing", 3000),
 ("Kumar", "Marketing", 2000),
 ("Saif", "Sales", 4100)
 ]

columns = ["employee_name", "department", "salary"]


df = spark.createDataFrame(simple_data, columns)

df.show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

window_spec = Window.partitionBy('department').orderBy(col('salary').desc())
window_spec1 = Window.orderBy(col('salary').desc())

df.withColumn('rowrank', row_number().over(window_spec)).show(10, False)  #in series
df.withColumn('rank', rank().over(window_spec)).show(10, False)    #same value for same num abut keep adding when gap
df.withColumn('denserank', dense_rank().over(window_spec)).show(10, False) #same value for same num but don't keep adding 

df.withColumn('salrank', dense_rank().over(window_spec1)).show(truncate=False)

# COMMAND ----------



# COMMAND ----------

#find avg salary for each dept

win = Window.partitionBy(col('department'))

df.withColumn('avgSal', avg(col('salary')).over(win))\
.withColumn('rank', dense_rank().over(win.orderBy('avgSal'))).show()


# COMMAND ----------

#join:

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)

# COMMAND ----------

leftanti -: only column from right table where doen't not match with left
leftsemi --: only columns from left table which match with right 

# COMMAND ----------

#self join

emp = [(1,"John Smith",3000, 3), \
    (2,"Jane Anderson",4000,3), \
    (3,"Tom Lanon",1000,4), \
    (4,"Anne Connor",2000, None), \
    (5,"Jeremy York",3000, 1)
  ]
empColumns = ["emp_id","name","salary","manager_id"]

df = spark.createDataFrame(emp, empColumns)

df.show()

# COMMAND ----------

#find manager for each employee

df.alias("df1").join(df.alias('df2'), col("df1.manager_id") == col("df2.emp_id"), 'inner')\
.select(col("df1.emp_id"), col("df1.name"), col("df1.manager_id"), col("df2.name").alias('manager')).show()



# COMMAND ----------


""" 
table_name, cob_Date,run_id, count
  A, 2022-03-21 , 3 4000
  A, 2022-03-22 , 3 4050
  B, 2022-03-21 , 3 4000
  B, 2022-03-22 , 3 4050
"""

columns = [ 'table_name', 'cob_date','run_id', 'count']

data =[('A', '2022-03-21' , '3', '4000'), ('A', '2022-03-22' , '3', '4050'),
       ('B', '2022-03-21' , '3', '4000'), ('B', '2022-03-22' , '3', '4050'),
      ('B', '2022-03-21' , '3', '7000'), ('B', '2022-03-22' , '3', '7050')
      ]
       


df = spark.createDataFrame(data, columns)

df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import col,dense_rank

winspec = Window.partitionBy(col('cob_date'),col('table_name')).orderBy(col('count').desc())

df_1 = df.withColumn('rank', dense_rank().over(winspec)).where(col('rank') == 1)

df_1.show()




# COMMAND ----------

sample_dict = {
    "name": "Kelly",
    "age": 25,
    "salary": 8000,
    "city": "New york"}

# Keys to extract
keys = ["name", "salary"]


d = dict()


d.update({i:sample_dict[i]} for i in keys)

print(i)

# COMMAND ----------

line = "Return a list of the lines in the string, breaking at line boundaries list"

r = spark.sparkContext.parallelize(line.split(" "))

a = r.map(lambda x: (x,1))

#print(a.collect())


#how many time 'list' word is in line

rbyk = a.reduceByKey(lambda x,y: x+y)
#print(rbyk.collect())

gbyk = a.groupByKey()

         #or
#print(gbyk.map(lambda x: (x[0], sum(list(x[1])))).collect())
         #or
#data   =  r.map(lambda x: (x, 1))
kby = r.keyBy(lambda x: x[0])

#print(kby.collect())

t = kby.mapValues(lambda x: x.upper())
#print(t.collect())


#count of each word
#print(r.collect())

a = r.flatMap(lambda x:x)
#print(a.collect())
#a.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y).collect()


#count of 'b' char in string


sc = spark.sparkContext

print(sc)
l = [1,2,3,5,6]
d = sc.broadcast(l)


print(d.value[2])






# COMMAND ----------

#expload and unexpload

from pyspark.sql.functions import split, collect_set, explode

#collect_set to remove duplicates

file_location = "/FileStore/tables/retail-data/2010_12_08.csv"

dict = {"header":True, "inferSchema": True}

retail_df =  spark.read.format('csv').options(**dict).load(file_location)



splitted_retail_df = retail_df.select(['stockcode', 'description','country'])

exploaded_df = splitted_retail_df.withColumn('splited_desc',split('description'," ")).withColumn('exploaded_desc', explode( split('description'," ")))

exploaded_df.show(5, truncate=False)

# COMMAND ----------

exploaded_df.groupBy(['description']).agg(collect_set('exploaded_desc')).show(5, truncate=False)

# COMMAND ----------

#group by

import pyspark.sql.functions as f
df.groupBy(f.year('invoicedate')).agg(f.count('quantity'), f.sum('quantity')).show(5, truncate=False)

# COMMAND ----------

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


d_test = empDF



# COMMAND ----------

joindf = empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id, 'inner')

joindf.show()

joindf.explain()


# COMMAND ----------

#empDF.write.format('parquet').mode('overwrite').partitionBy('emp_dept_id').save("dbfs:/FileStore/tables/output/emp_table_stg")
empDF = spark.read.format('parquet').load('dbfs:/FileStore/tables/output/emp_table_stg')
joindf = empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id, 'inner').where('emp_dept_id == 10')

joindf.show()

joindf.explain()


# COMMAND ----------

spark.conf.set('spark.sql.optimizer.dynamicPartitionPruning.enabled',True)

empDF.where('emp_dept_id == 10').explain()
d_test.where('emp_dept_id == 10').explain()
#joindf = empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id, 'inner').where('emp_dept_id == 10')

#joindf.show()

#joindf.explain()

d_test.where(f.col('name') == 'jones').explain()

import pyspark.sql.functions as f

empDF.where(f.col('name') == 'jones').explain()


empDF.join(d_test, empDF.emp_dept_id == d_test.emp_dept_id ,'inner').explain()


# COMMAND ----------

empDF.where('emp_dept_id == 10').show()
#d_test.where('emp_dept_id == 10').show()

# COMMAND ----------

#join:

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
#empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
#deptDF.printSchema()
deptDF.show(truncate=False)





# COMMAND ----------

deptDF.crossJoin(empDF).count()

# COMMAND ----------

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'right').show()

# COMMAND ----------

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'right').show()

# COMMAND ----------

spark.sql('show databases').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists demo

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database extended demo

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls dbfs:/FileStore/tables/processed

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;

# COMMAND ----------

races_df = spark.read.parquet("dbfs:/FileStore/tables/processed/races/")
races_df.show()

# COMMAND ----------

spark.sql('use demo')

races_df.createOrReplaceTempView('races_df_sql')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in demo

# COMMAND ----------

#created managed table
races_df.write.format('parquet').saveAsTable('races_df_data')


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended races_df_data

# COMMAND ----------

#externale table

races_df.write.format('parquet').option('path','dbfs:/FileStore/tables/processed/race_df_sql').saveAsTable('demo.race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended demo.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create external table demo.race_df_external_table(
# MAGIC test String,
# MAGIC test1 String
# MAGIC )
# MAGIC using parquet
# MAGIC location 'dbfs:/FileStore/tables/processed/race_result_external_sql'

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables in demo
