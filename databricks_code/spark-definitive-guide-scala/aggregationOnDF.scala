// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val df = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("dbfs:/FileStore/tables/suankiData/retailData/*.csv")
.coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")

// COMMAND ----------

df.count()

// COMMAND ----------

df.select(count("StockCode")).show()

// COMMAND ----------

df.select(countDistinct(col("stockCode"))).show()

// COMMAND ----------

df.select(last("stockCode")).show()

// COMMAND ----------

df.select(sum_distinct(col("Quantity"))).show()

// COMMAND ----------

df.select(
count("Quantity").alias("total_transactions"),
sum("Quantity").alias("total_purchases"),
avg("Quantity").alias("avg_purchases"),
expr("mean(Quantity)").alias("mean_purchases"))
.selectExpr(
"total_purchases/total_transactions",
"avg_purchases",
"mean_purchases").show()

// COMMAND ----------

df.select(skewness("Quantity"), kurtosis("Quantity")).show()

// COMMAND ----------

df.agg(collect_set("Country"), collect_list("Country")).show()

// COMMAND ----------

df.groupBy("InvoiceNo", "CustomerId").count().show()

// COMMAND ----------

df.groupBy("InvoiceNo").agg(
count("Quantity").alias("quan"),
expr("count(Quantity)")).show()

// COMMAND ----------

//window functions: ranking functions, analytic functions,
//and aggregate functions.

// COMMAND ----------

import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")
  df.show()

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val windowSpecification = Window.partitionBy("department").orderBy(col("salary").desc)


// COMMAND ----------

df.withColumn("row_number", row_number.over(windowSpecification)).show()

// COMMAND ----------

df.withColumn("row_number", rank.over(windowSpecification)).show()  //add the gap

// COMMAND ----------

df.withColumn("row_number", dense_rank.over(windowSpecification)).show() 

// COMMAND ----------

val windowSpecAgg  = Window.partitionBy("department")

val aggDF = df.withColumn("row",row_number.over(windowSpecification))
    .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
    .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
    .withColumn("min", min(col("salary")).over(windowSpecAgg))
    .withColumn("max", max(col("salary")).over(windowSpecAgg))
    .where(col("row")===1).select("department","avg","sum","min","max")
    .show()

// COMMAND ----------

//pivote
val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

import spark.sqlContext.implicits._
val df = data.toDF("Product","Amount","Country")
df.show()

// COMMAND ----------

val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.show()

// COMMAND ----------

df.groupBy("Product").pivot("Country").agg(sum("amount")).show()

// COMMAND ----------

// Unpivot
val unPivotDF = pivotDF.select($"Product",
expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"))
.where("Total is not null")
unPivotDF.show()

// COMMAND ----------

val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
  )
  val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary")
  import spark.sqlContext.implicits._
  val empDF = emp.toDF(empColumns:_*)
  empDF.show(false)

  val dept = Seq(("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  )

  val deptColumns = Seq("dept_name","dept_id")
  val deptDF = dept.toDF(deptColumns:_*)
  deptDF.show(false)

// COMMAND ----------

empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"inner")
    .show(false)

// COMMAND ----------

 empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"outer").show()

// COMMAND ----------

 empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"left")
    .show(false)

// COMMAND ----------

 empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"right")
   .show(false)

// COMMAND ----------

empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi")
    .show(false)

// COMMAND ----------

 empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti")
    .show(false)
