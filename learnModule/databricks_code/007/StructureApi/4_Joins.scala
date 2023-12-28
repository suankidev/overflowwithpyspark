// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC The list of joins provided by Spark SQL is:
// MAGIC
// MAGIC 1. Inner Join
// MAGIC 2. Left / Left Outer Join
// MAGIC 3. Right / Right Outer Join
// MAGIC 4. Outer / Full Join
// MAGIC 5. Cross Join
// MAGIC 6. Left Anti Join
// MAGIC 7. Left Semi Join
// MAGIC 8. Self Join
// MAGIC
// MAGIC ```Syntax
// MAGIC
// MAGIC 1) join(right: Dataset[_]): DataFrame
// MAGIC 2) join(right: Dataset[_], usingColumn: String): DataFrame
// MAGIC 3) join(right: Dataset[_], usingColumns: Seq[String]): DataFrame
// MAGIC 4) join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame
// MAGIC 5) join(right: Dataset[_], joinExprs: Column): DataFrame
// MAGIC 6) join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame
// MAGIC
// MAGIC ```

// COMMAND ----------

// MAGIC %run /007/CommonUtils/SetupData

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

// COMMAND ----------


val dept = Seq(("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  )

  val deptColumns = Seq("dept_name","dept_id")
  val deptDF = dept.toDF(deptColumns:_*)
  deptDF.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Inner join
// MAGIC >Spark Inner join is the default join and it’s mostly used, It is used to join two DataFrames/Datasets on key columns, and where keys don’t match the rows get dropped from both datasets (emp & dept).

// COMMAND ----------

  //It drops “emp_dept_id” 50 from “emp” and “dept_id” 30 from “dept” datasets
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"inner")
    .show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####   Outer Join a.k.a. full, fullouter, full_outer
// MAGIC >returns all rows from both Spark DataFrame/Datasets, where join expression doesn’t match it returns null on respective record columns.

// COMMAND ----------

  //“emp” dataset’s “emp_dept_id” with value 50 doesn’t have a record on “dept” hence dept columns have null and “dept_id” 30 doesn’t have a record in “emp” hence you see null’s on emp columns
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"full")
    .show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Left Outer Join
// MAGIC >Spark Left a.k.a Left Outer join returns all rows from the left DataFrame/Dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for that record and drops records from right where match not found.

// COMMAND ----------

 
 //“emp_dept_id” 5o doesn’t have a record on “dept” dataset hence, this record contains null on “dept” columns (dept_name & dept_id). and “dept_id” 30 from “dept” dataset dropped from the results
 empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"left")
    .show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Right Outer Join
// MAGIC >Spark Right a.k.a Right Outer join is opposite of left join, here it returns all rows from the right DataFrame/Dataset regardless of math found on the left dataset, when join expression doesn’t match, it assigns null for that record and drops records from left where match not found.

// COMMAND ----------

//The right dataset “dept_id” 30 doesn’t have it on the left dataset “emp” hence, this record contains null on “emp” columns. and “emp_dept_id” 50 dropped as a match not found on left. Below is the result
empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"right")
   .show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Left Semi join
// MAGIC >Spark Left Semi join is similar to inner join difference being leftsemi join returns all columns from the left DataFrame/Dataset and ignores all columns from the right dataset
// MAGIC >this join returns columns from the only left dataset for the records match in the right dataset on join expression, records not matched on join expression are ignored from both left and right datasets.
// MAGIC

// COMMAND ----------

 empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi")
    .show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Left Anti Join
// MAGIC >Left Anti join does the exact opposite of the Spark leftsemi join, leftanti join returns only columns from the left DataFrame/Dataset for non-matched records.

// COMMAND ----------

  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti")
    .show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####  `Self Join`
// MAGIC
// MAGIC ```
// MAGIC below dataset
// MAGIC On this dataset we have an employee id column `emp_id and superior_emp_id`
// MAGIC , we use these two columns to do a self-join and find’s out a superior name for all employee’s
// MAGIC ```

// COMMAND ----------

  val emp = Seq((1,"Smith",1,"10",3000),
    (2,"Rose",1,"20",4000),
    (3,"Williams",1,"10",1000),
    (4,"Jones",2,"10",2000),
    (5,"Brown",2,"40",-1),
    (6,"Brown",2,"50",-1)
  )
  val empColumns = Seq("emp_id","name","superior_emp_id","emp_dept_id","salary")
  import spark.implicits._
  val empDF = emp.toDF(empColumns:_*)
  empDF.show(false)

// COMMAND ----------

empDF.alias("emp1").join(empDF.alias("emp2"), f.col("emp1.emp_id") === f.col("emp2.superior_emp_id"), "inner")  //inner
.select(col("emp1.emp_id"),col("emp1.name"),
      col("emp2.emp_id").as("superior_emp_id"),
      col("emp2.name").as("superior_emp_name"))
    .show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Cross (Cartesian) Joins
// MAGIC >The last of our joins are cross-joins or cartesian products. Cross-joins in simplest terms are inner
// MAGIC joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame
// MAGIC to ever single row in the right DataFrame. This will cause an absolute explosion in the number of
// MAGIC rows contained in the resulting DataFrame. If you have 1,000 rows in each DataFrame, the crossjoin of these will result in 1,000,000 (1,000 x 1,000) rows. For this reason, you must very
// MAGIC explicitly state that you want a cross-join by using the cross join keyword:
// MAGIC
// MAGIC ```Note:
// MAGIC
// MAGIC They’re dangerous!
// MAGIC Advanced users can set the session-level configuration spark.sql.crossJoin.enable to true in
// MAGIC order to allow cross-joins without warnings or without Spark trying to perform another join for you.
// MAGIC ```

// COMMAND ----------

empDF.crossJoin(deptDF)
    .show()

// COMMAND ----------

// MAGIC %md
// MAGIC #Challenges When Using Joins

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1. complex data types

// COMMAND ----------

val person = Seq(
(0, "Bill Chambers", 0, Seq(100)),
(1, "Matei Zaharia", 1, Seq(500, 250, 100)),
(2, "Michael Armbrust", 1, Seq(250, 100)))
.toDF("id", "name", "graduate_program", "spark_status")

val graduateProgram = Seq(
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley"))
.toDF("id", "degree", "department", "school")

val sparkStatus = Seq(
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor"))
.toDF("id", "status")

// COMMAND ----------

person.printSchema

// COMMAND ----------

person.withColumnRenamed("id", "personId")
.join(sparkStatus, expr("array_contains(spark_status, id)")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2. Handling Duplicate Column Names
// MAGIC >dealing with duplicate column names in your
// MAGIC results DataFrame. In a DataFrame, each column has a unique ID within Spark’s SQL Engine,
// MAGIC Catalyst. This unique ID is purely internal and not something that you can directly reference.
// MAGIC This makes it quite difficult to refer to a specific column when you have a DataFrame with
// MAGIC duplicate column names.
// MAGIC This can occur in two distinct situations:
// MAGIC
// MAGIC 1. The join expression that you specify does not remove one key from one of the input
// MAGIC DataFrames and the keys have the same column name
// MAGIC 2. Two columns on which you are not performing the join have the same name
// MAGIC Let’s create a problem dataset that we can use to illustrate these problems

// COMMAND ----------

val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")

person.join(gradProgramDupe, joinExpr).show()

//The challenge arises when we refer to one of these columns:

person.join(gradProgramDupe, joinExpr).select("graduate_program").show()  //`graduate_program` is ambiguous, could be: [`graduate_program`, `graduate_program`].

// COMMAND ----------

// MAGIC %md
// MAGIC # Solution to duplicate col

// COMMAND ----------

// MAGIC %md
// MAGIC #### Approach 1: Different join expression

// COMMAND ----------


//This automatically removes one of the columns for you during the join:
person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Approach 2: Dropping the column after the join

// COMMAND ----------

person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program")).select("graduate_program").show()

val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram, joinExpr).drop(graduateProgram.col("id")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ####  Approach 3: Renaming a column before the join

// COMMAND ----------

val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
val joinExpr = person.col("graduate_program") === gradProgram3.col("grad_id")
person.join(gradProgram3, joinExpr).show()
