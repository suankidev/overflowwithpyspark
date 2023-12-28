// Databricks notebook source
val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1),
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

// MAGIC %md 
// MAGIC ### Remove duplicate from emp datasets

// COMMAND ----------

empDF.createOrReplaceTempView("empView")

// COMMAND ----------

spark.sql("show tables").show()

// COMMAND ----------

spark.sql(
"""
select distinct * from  empView
"""
).show()

// COMMAND ----------

//will assign 2 row number if it falls in same parition with same id and name
spark.sql("""
select *, row_number() over (partition by name, emp_id order by emp_id desc) as row_num from empview
""").show()

// COMMAND ----------

//will give the duplicate
spark.sql("""
with temp as (select *, row_number() over (partition by name, emp_id order by emp_id desc) as row_num from empview )
select * from temp where row_num > 1
""").show()

// COMMAND ----------

//will neglect duplicate
spark.sql("""
with temp as (select *, row_number() over (partition by name, emp_id order by emp_id desc) as row_num from empview )
select * from temp where row_num = 1
""").show()

// COMMAND ----------

//groupBy to remove duplicate
spark.sql("""
select emp_id,name,superior_emp_id,year_joined,emp_dept_id,gender,salary from empview  group by  emp_id,name,superior_emp_id,year_joined,emp_dept_id,gender,salary
""").show()

// COMMAND ----------


