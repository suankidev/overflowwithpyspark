# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/data/fligt_data/2015_summary.csv

# COMMAND ----------

#%fs ls dbfs:/FileStore/tables/data/retail-data/2010_12_01.csv
dbutils.fs.ls("/FileStore/tables/")


test_file ="dbfs:/FileStore/tables/tstfile.csv"
data = "Spark  is not enough to work with all the stuff in BigData , we need addtion tech to learn BigData"


flight_rdd = spark.sparkContext.textFile("dbfs:/FileStore/tables/data/fligt_data/2015_summary.csv")


test_file_rdd = spark.sparkContext.textFile(test_file)

line_rdd = spark.sparkContext.parallelize(data.split(" "))



# COMMAND ----------

#sort by len

line_rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y : x+y).collect()



# COMMAND ----------




# COMMAND ----------

txt = spark.sparkContext.textFile("dbfs:/FileStore/tables/data/fligt_data/2015_summary.csv")

header = txt.first()
print(header)

txt = txt.filter(lambda line: line != header)

print(txt.take(10))

print(txt.map(lambda l:l.split(',')).take(5))

#remove footer
txt = txt.map(lambda line: line.split(",")).filter(lambda line: line[0] != 'United States' and line[1] != 'India')

txt.count()

#convert to dataframe
#df=txt.toDF(header.split("|"))
#df.show()
  #          source_df = spark.read.option("header","true").option("delimiter",field_delimiter).csv(path)
  source_df = spark.sparkContext.textFile(path)
  source_header = source_df.first()
  source_df = source_df.filter(lambda line: line != source_header)
  source_df = source_df.map(lambda line : line.split(field_delimiter)).filter(lambda line: line[0] != 'T' and line[2] != 'EOF')
  source_df = source_df.toDF(source_header.split(field_delimiter))
  
            source_df = source_df.where(col(source_df.columns[0]) != 'T')
            source_df_stg = spark.read.option("header","true").option("delimiter",field_delimiter).schema(stg_table_schema).csv(path)
            #source_df_stg = source_df_stg.limit(source_df_stg.count()-1)
            source_df_stg = source_df_stg.where(col(source_df_stg.columns[0]) != 'T')
            source_df_final = spark.read.option("header","true").option("delimiter",field_delimiter).schema(fds_table_schema).csv(path)
            #source_df_final = source_df_final.limit(source_df_final.count()-1)
            source_df_final = source_df_final.where(col(source_df_final.columns[0]) != 'T')
            print("Footer removed from source file ...")


# COMMAND ----------

source_d

# COMMAND ----------

def test(**option):
  return option

test(**{"header":"True","inferSchema":"True"})



# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,LongType

flightSchema=StructType([StructField("destination_country",StringType(),True),StructField("source_country",StringType(),True),StructField("count_of_travel",LongType(),True)])

flight2015=spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/FileStore/tables/data/fligt_data/2015_summary.csv")



# COMMAND ----------

flight2015.show()

# COMMAND ----------

flight2015.select("dest_country_name","origin_country_name","count").show()

# COMMAND ----------

from pyspark.sql.functions import lit,expr,col
testDf=flight2015.select("*",lit("2021-12-31").alias("Sample_date"))\
.withColumn("destination",col("DEST_COUNTRY_NAME") == 'United States').withColumnRenamed("destination","dest")


# COMMAND ----------

testDf.show(5)

testDf.drop("dest").show(5)

# COMMAND ----------

testDf.withColumn("countAslongtype",col("count").cast("decimal(22,15)")).withColumn("test",col("countAslongtype")\
                                                                                    .cast("Float")).where(col("dest")).show()

# COMMAND ----------

#flight2015.where(col("count")>5).where(col("origin_country_name") != "United States").show()

#flight2015.distinct().show()

flight2015.sample(True,0.5,5).count()


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType
from pyspark.sql import Row

def createDataFrameWithSchema(data,schema):
  return spark.createDataFrame(data,schema)

#student data and schema
student_data=[Row("Sujeet kumar",29,"Gorakhpur",9), Row("Baradi Vanktesh",27,"Hyderabad",2),Row("Vibha Bhosker",24,"Nagpur,Maharashtra",1),\
             Row("footer",None,None,None)]
not_tech_student_data=[Row("Ramesh",30,"Mumbai",51), Row("Ram",23,"Solapur,Maharashtra",57),Row("Tiger",34,"Patana,Bihar",54)
                      ,Row("Bahadur",23,"Madurai,Chennai",54)]
student_schema=StructType([StructField("Name",StringType(),True), StructField("Age",LongType(),True),StructField("Address",StringType(),True)
                          ,StructField("CourseID",LongType(),True)])

#course data and schema
course_data=[Row(9,"Computer Science"), Row(2,"BioTechnology"),Row(1,"LifeScience"),Row(3,"Space Tech"),Row(3,"Ethical Hacking"),Row(4,"Data Science")\
            ,Row(6,None),Row(None,None)]
cource_schema=StructType([StructField("CourseID", LongType(),True),StructField("CourseName",StringType(),True)])

course_detail_data=[Row(1,140000,"Yealry",2.0),Row(2,190000,"Yealry",4.0),Row(3,120000,"Half Yearly",5.0),Row(3,17000,"Yearly",0.5),Row(4,200000,"Yearly",2.0),Row(9,190000,"Yearly",4.0)]

course_detail_schema=StructType([StructField("CourseID", LongType(),False),StructField("CourseFee", LongType(),False),StructField("Feestructure",StringType(),False),StructField("CourseDuration", DoubleType(),False)])

#Creating the datafram from data and schema
studentDF=createDataFrameWithSchema(student_data,student_schema)
student_non_tech_DF=createDataFrameWithSchema(not_tech_student_data,student_schema)
courseDF=createDataFrameWithSchema(course_data,cource_schema)
courseDetailDF=createDataFrameWithSchema(course_detail_data,course_detail_schema)



studentDF.show()
courseDF.sort("CourseID").show()
courseDetailDF.show()
student_non_tech_DF.show()

total_studentDF=studentDF.union(student_non_tech_DF)


# COMMAND ----------

#removing footer logic
from pyspark.sql.functions import avg,max,round, bround,monotonically_increasing_id
studentDF.show()

print(studentDF.columns[0])

studentDF.where(col(studentDF.columns[0]) != 'T').show()


# COMMAND ----------

"""To union two DataFrames, you must be sure that they have the same schema and
number of columns; otherwise, the union will fail.
"""
studentDF.union(student_non_tech_DF).where(col("Age")>25).show(10,False)

# COMMAND ----------

"""sort() is more efficient compared to orderBy() because the data is sorted on each partition individually and this is why the order in the output data is not guaranteed. On the other hand, orderBy() collects all the data into a single executor and then sorts them"""

from pyspark.sql.functions import desc,asc,desc_nulls_first,asc_nulls_first

#total_studentDF.sort("Age","CourseID").desc().show()

sortedStudentDF=total_studentDF.orderBy(col("Age").desc_nulls_first())


#sortWithinPartion

flightDF2015=spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/FileStore/tables/data/fligt_data/2015_summary.csv").sortWithinPartitions("count")




# COMMAND ----------

#partitions and Coalesce
"""                      ##OPTIMIZATION##
partition based optimization: sorWithinPartition while reading the df
                           : repartition the data based on most frequentyl used filter column
"""



# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",5)  #decide shuffle partition
flightDF2015.orderBy(col("count").desc()).explain()


flightDF2015.repartition(10).rdd.getNumPartitions()

"""If you know that you’re going to be filtering by a certain column often, it can be worth
repartitioning based on that column:
"""


#10 partition based on 2015
finalFlightDF2015=flightDF2015.repartition(10,col("dest_country_name"))


# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive", False)

#total_studentDF.select("age").distinct().show()
total_studentDF.where(col("age") == 23).show()



# COMMAND ----------

"""Working with Booleans------------------->"""
from pyspark.sql.functions import instr,col

agefilt = col("age") == 23

agefilt3 = col("age").eqNullSafe(23)

ageFilt1 = col("age")  != 23

addrfilt1 = col("address").isin("Mumbai")

addrfilt2 = ((instr(col("address"),"Maharashtra") >= 1 ) & agefilt)


#it will search for the substring in string  and ret
addrfilt = instr(col("address") , "rashtra") >= 1

total_studentDF.withColumn("filterCol",addrfilt).where(agefilt).where(addrfilt).show()

total_studentDF.withColumn("filtCol",agefilt | addrfilt).show()

total_studentDF.withColumn("filtcol",col("Address").isin("Mumbai")).show()


"""or --> |  in python 
and  ---> &  in python"""

# COMMAND ----------

total_studentDF.where(col("age") !=  23).show()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/data/retail-data/2010_12_01.csv

# COMMAND ----------

df=spark.read.format("csv").option("header","true").option("sep",",").load("dbfs:/FileStore/tables/data/retail-data/2010_12_01.csv")
retailDF=spark.read.format("csv").option("header","false").option("sep",",").load("dbfs:/FileStore/tables/data/retail-data/2010_12_01.csv")
retailDF1=spark.read.format("csv").option("header","false").option("inferSchema",True).option("sep",",").load("dbfs:/FileStore/tables/data/retail-data/2010_12_01.csv")
print(retailDF.first()[0])

#removing the first column
finalRetailDF=retailDF.where(col("_c0") != retailDF.first()[0]).rdd.toDF(df.schema)

finalRetailDF.show()

sch = retailDF.schema
retailRdd=retailDF.rdd
header=retailRdd.first()
finalDF=retailRdd.filter(lambda x: x != header).toDF(sch)

#finalDF.show()


# COMMAND ----------

from pyspark.sql.functions import expr, pow

fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5

finalRetailDF.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)


# COMMAND ----------

from pyspark.sql.functions import avg,max,round, bround,monotonically_increasing_id

finalRetailDF.select(round(lit("2.5")), bround(lit("2.5")))

df=finalRetailDF.select("StockCode").withColumn("test",col("stockcode").isin(['85123A','84406B','22632'])).withColumn("srno",monotonically_increasing_id())

#remove footer
# file_df=file_df.withColumn("sr_number",monotonically_increasing_id())
# line_to_remove=file_df(max("sr_number")+1)
# file_df=file_df.where("sr_number" != line_to_remove)


df.show(10)
#df.select(max("srno")+1).show()


# COMMAND ----------

from pyspark.sql.functions import initcap,lit, ltrim, rtrim, rpad, lpad, trim

df=finalRetailDF
#df.select(initcap(col("description"))).show()

df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)



# COMMAND ----------

from pyspark.sql.functions import regexp_replace,translate,regexp_extract

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"

df.select(
regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
col("Description"))#.show(2,False)

#replace char by char
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))



extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),col("Description")).show(10,False)





# COMMAND ----------

containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1

df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
.where("hasSimpleColor")\
.select("Description").show(3, False)

# COMMAND ----------

from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]

def color_locator(column, color_string):
   return locate(color_string.upper(), column).cast("boolean").alias("is_"+color_string)

selectedColumns = [color_locator(col("description"),c) for c in simpleColors]

selectedColumns.append(expr("*")) # has to a be Column type

df.select(*selectedColumns).where(expr("is_white OR is_red")).select("Description").show(3, False)


# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp

dateDF = spark.range(10)\
.withColumn("today", current_date())\
.withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")

dateDF.printSchema()



# COMMAND ----------

from pyspark.sql.functions import date_sub,date_add,months_between,datediff

dateDF.withColumn("minusfive",date_sub(col("today"),5)).withColumn("minux5dayfromnow",date_sub(col("now"),5))\
.withColumn("date1",lit("2021-12-31")).withColumn("date2",lit("2020-05-13"))\
.withColumn("daysdiff",datediff(col("date1"),col("date2"))).show(5, False)


# COMMAND ----------

from pyspark.sql.functions import expr,col,to_date,from_unixtime,unix_timestamp

dateDF.withColumn("test",to_date(col("now"))).show(2,False)

dateDF.withColumn("testdate",lit("12/04/2021 12:12:32"))\
                  .select(from_unixtime(unix_timestamp(col('testdate'),'MM/dd/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')).show()
                 
                  #.select(unix_timestamp(col('testdate'),'MM/dd/yyyy HH:mm:ss')).show()
                 

# COMMAND ----------

from pyspark.sql.functions import to_date, to_timestamp

spark.range(1).select(to_date(lit("2017-12-31"),'yyyy-MM-dd').alias("testdate")).withColumn("cleanDate",to_timestamp("testdate")).show(10)


#cleanDateDF.filter(col("date2") > "'2017-12-12'").show()


# COMMAND ----------

from pyspark.sql.functions import coalesce,asc_nulls_first

#df.show(3,False)

df.orderBy(col("description").asc_nulls_first()).show(5)



df.select(coalesce(col("description"), col("customerid")).alias("testdata"))




#ifnull, nullIf, nvl, and nvl2



# COMMAND ----------

df.createOrReplaceTempView("dftable")

spark.sql("SELECT decode(1,1,'one')").show()

spark.sql("SELECT decode(1,2,'one')").show()

spark.sql("SELECT decode(1,2,'one','not one')").show()  # if 1 == 1 then one else not one

spark.sql(" select DECODE(2, 1, 'One',  2, 'Two')").show() # if 2 == 1 then 'one' elif 2 == 2 then 'two'

spark.sql("select DECODE(3, 1, 'One',  2, 'Two', 'Not one or two')").show() # default value


# COMMAND ----------

#spark.sql("SELECT decode(description,'WHITE METAL LANTERN','testdata',description) from dftable").show()

from pyspark.sql.functions import decode

df.selectExpr("decode(description,'WHITE METAL LANTERN','testdata',description)").show(5)




# COMMAND ----------

#handling nulll
"""
ifnull --?    if null then "firstval"
nullif --?     if both are same then null else first
nvl    --?    if first is not null then first else second val
"""

spark.sql("SELECT ifnull(null, 'return_value'),\
nullif('value', 'value'),\
nvl(null, 'return_value'),\
nvl2('not_null', 'return_value', 'else_value')\
FROM dfTable LIMIT 1").show()


# COMMAND ----------

#dropin null col

df.na.drop()
df.na.drop("any")
df.na.drop("all")

df.na.drop("all", subset=["StockCode", "InvoiceNo"])





# COMMAND ----------

#to fill all null values in columns of type String, you might specify the following


# COMMAND ----------

courseDF.show()

#to fill all null values in columns of type String
courseDF.na.fill("testval").show()

fill_cols_vals = {"courseid": 100, "coursename" : "NOT ASSIGNED"}

courseDF.na.fill(fill_cols_vals).show()


# COMMAND ----------

courseDF.na.replace(["Data Science"], ["Science"], "courseid").show()


# COMMAND ----------

#Working with complex type:

"""
There are three kinds of complex types: structs, arrays,
and maps.


"""
from pyspark.sql.functions import struct

#form struct
tempDF=df.select(struct(col("description"), col("invoiceno")).alias("complex"))

#various way of access
tempDF.select(col("complex").getField("description"))

tempDF.selectExpr("complex.description")

tempDF.select("complex.*")


# COMMAND ----------

#Array


"""To define arrays, let’s work through a use case. With our current data, our objective is to take
every single word in our Description column and convert that into a row in our DataFrame.
The first task is to turn our Description column into a complex type, an array."""


from pyspark.sql.functions import split,create_map

tempdf=df.select(col("description"), split(col("description"), " ").alias("arrcol"), create_map(col("description"), col("InvoiceNo")).alias("complex_map"))

df.createOrReplaceTempView("tabsql")

sql("select split(description, ' ') from tabsql").show(5,False)



# COMMAND ----------

from pyspark.sql.functions import size,array_contains,explode

tempdf.withColumn('size',size(col('arrcol'))).withColumn('firstindex',col('arrcol')[0])\
.withColumn('arrcontain',array_contains(col('arrcol'),'WHITE'))\
.withColumn('exploaded',explode(col('arrcol')))\
.select('*')\
.show(5,False)

#dateDF.select(timestamp(col("now"))).show(5,False)


# COMMAND ----------

"""You can query them by using the proper key. A missing key returns null"""

tempdf.select("complex_map").select(col('complex_map')['WHITE METAL LANTERN']).show(10,False)

tempdf.select("complex_map").show(5,False)


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

udfExample=spark.range(5).toDF("num")

def power3(number):
  return number ** 3

power3(2.0)

pw=udf(power3)


udfExample.select(pw(col("num"))).show(5)
#or

spark.udf.register("pw1",power3,DoubleType())

udfExample.createOrReplaceTempView("ex")


spark.sql("select pw1(num) from ex").show(5)



# COMMAND ----------

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

# COMMAND ----------

from pyspark.sql.functions import col,rank, dense_rank, row_number
df.show

from pyspark.sql.window import Window

winspec = Window.orderBy(col('salary').desc())


#second highest sal

df_filt = df.withColumn('sal_rank', rank().over(winspec))\
            .withColumn('sal_dense', dense_rank().over(winspec))\
            .withColumn('sal_row_numk', row_number().over(winspec))


#df_filt.show()

df.createOrReplaceTempView('temp')

spark.sql("""select temp.*, dense_rank() over(order by salary) as rank from temp""").show()


# COMMAND ----------

#join

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


