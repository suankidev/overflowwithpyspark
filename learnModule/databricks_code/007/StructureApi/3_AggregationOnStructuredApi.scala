// Databricks notebook source
// MAGIC %run /007/CommonUtils/SetupData

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC **In addition to working with any type of values, Spark also allows us to create the following
// MAGIC groupings types:**
// MAGIC
// MAGIC 1. The simplest grouping is to just summarize a complete DataFrame by performing an
// MAGIC aggregation in a select statement.
// MAGIC
// MAGIC 2. A “group by” allows you to specify one or more keys as well as one or more
// MAGIC aggregation functions to transform the value columns.
// MAGIC
// MAGIC 3. A “window” gives you the ability to specify one or more keys as well as one or more
// MAGIC aggregation functions to transform the value columns. However, the rows input to the
// MAGIC function are somehow related to the current row.
// MAGIC
// MAGIC 4. A “grouping set,” which you can use to aggregate at multiple different levels. Grouping
// MAGIC sets are available as a primitive in SQL and via rollups and cubes in DataFrames.
// MAGIC
// MAGIC 5. A “rollup” makes it possible for you to specify one or more keys as well as one or more
// MAGIC aggregation functions to transform the value columns, which will be summarized
// MAGIC hierarchically.
// MAGIC
// MAGIC 6. A “cube” allows you to specify one or more keys as well as one or more aggregation
// MAGIC functions to transform the value columns, which will be summarized across all
// MAGIC combinations of columns.
// MAGIC
// MAGIC
// MAGIC >Each grouping returns a RelationalGroupedDataset on which we specify our aggregations.

// COMMAND ----------

val df = retailDF
df.cache()
df.createOrReplaceTempView("retailDF")

// COMMAND ----------

// MAGIC %md
// MAGIC ###Count
// MAGIC 1. when
// MAGIC performing a count(*), Spark will count null values (including rows containing all nulls). However,
// MAGIC when counting an individual column, Spark will not count the null values.
// MAGIC
// MAGIC 2. counting on column is a transformation not a action like df.count()
// MAGIC
// MAGIC       - count(*) : count on all column
// MAGIC       - count(1) : count all row as literal 1
// MAGIC
// MAGIC ```
// MAGIC df.select(f.count("*"), f.count("description")).show()
// MAGIC +--------+------------------+
// MAGIC |count(1)|count(description)|
// MAGIC +--------+------------------+
// MAGIC |   19632|             19563|
// MAGIC +--------+------------------+
// MAGIC ```

// COMMAND ----------


df.count() //action

// COMMAND ----------

df.select(f.count("*"), f.count("description")).show() //count is a transformation

// COMMAND ----------

df.select(f.col("StockCode")).distinct.show(5,false) 

// COMMAND ----------

df.select(f.countDistinct("StockCode")).show() //see spark optimizaiton file for how spark does plan count distinct in backgroup

// COMMAND ----------

// MAGIC %md
// MAGIC ####1. The simplest grouping is to just summarize a complete DataFrame by performing an aggregation in a select statement.

// COMMAND ----------

df.select(
          f.first("description"), 
          f.last("description") , 
          f.min("quantity"),
          f.max("quantity"),
          f.sum("quantity"), 
          f.sum_distinct(f.col("quantity")),
          f.avg(f.col("quantity")),
          f.mean(f.col("quantity"))
         )
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC ####Variance and Standard Deviation
// MAGIC   These are both measures of the spread of the data around the mean. 
// MAGIC #### skewness and kurtosis
// MAGIC The skewness is a measure of symmetry or asymmetry of data distribution,
// MAGIC  and kurtosis measures whether data is heavy-tailed or light-tailed in a distribution
// MAGIC
// MAGIC #### Covariance and Correlation
// MAGIC Two of these functions are cov and corr, for
// MAGIC covariance and correlation, respectively. Correlation measures the Pearson correlation
// MAGIC coefficient, which is scaled between –1 and +1. The covariance is scaled according to the inputs
// MAGIC in the data.
// MAGIC
// MAGIC [DataSkewness](https://community.cloud.databricks.com/?o=2941430867364584#notebook/2227913417139108/command/2227913417139127)

// COMMAND ----------

retailDF.select(
  f.var_pop(f.col("Quantity")),
   f.var_samp(f.col("Quantity")),
f.stddev_pop(f.col("Quantity")), 
f.stddev_samp("Quantity"),
f.corr(f.col("InvoiceNo"), f.col("Quantity")), 
f.covar_samp(f.col("InvoiceNo"), f.col("Quantity") ),
f.covar_pop(f.col("InvoiceNo"), f.col("Quantity") )
).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Aggregating to Complex Types

// COMMAND ----------

//retailDF.select(f.collect_set(f.col("Country")).as("setOfCountry"), f.collect_list(f.col("Country")).as("listOfCountry"))

retailDF.agg(f.collect_set(f.col("Country")).as("setOfCountry"), f.collect_list(f.col("Country")).as("listOfCountry"))
.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### Grouping
// MAGIC
// MAGIC >First we specify the column(s) on which we would like to
// MAGIC group, and then we specify the aggregation(s). The first step returns a
// MAGIC RelationalGroupedDataset, and the second step returns a DataFrame

// COMMAND ----------

retailDF.groupBy("invoiceno","customerid").count().show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC #agg

// COMMAND ----------

retailDF.groupBy("invoiceno","customerid").agg(f.count(f.col("quantity")), f.min(f.col("quantity"))).show(5,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Grouping with Maps

// COMMAND ----------

retailDF.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ####Window Functions
// MAGIC [spark.apache.org](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
// MAGIC
// MAGIC [medium.com](https://medium.com/@erkansirin/spark-sql-window-functions-303702c194a8)
// MAGIC >A group-by takes data, and every row can go only into one grouping. 
// MAGIC Window functions operate on a group of rows, referred to as a window, and calculate a return value for each row based on the group of rows. Window functions are useful for processing tasks such as calculating a moving average, computing a cumulative statistic, or accessing the value of rows given the relative position of the current row.
// MAGIC
// MAGIC ```
// MAGIC
// MAGIC 1. Ranking Functions
// MAGIC
// MAGIC Syntax: RANK | DENSE_RANK | PERCENT_RANK | NTILE | ROW_NUMBER
// MAGIC 2. Analytic Functions
// MAGIC
// MAGIC Syntax: CUME_DIST | LAG | LEAD | NTH_VALUE | FIRST_VALUE | LAST_VALUE
// MAGIC 3. Aggregate Functions
// MAGIC
// MAGIC Syntax: MAX | MIN | COUNT | SUM | AVG | .
// MAGIC Basic syntax
// MAGIC
// MAGIC MAX(salary) over(partition by dept) as dept_max_salary
// MAGIC
// MAGIC ```

// COMMAND ----------

val df = departmentDF

// COMMAND ----------

val windowSpec  = Window.partitionBy("department").orderBy("salary")


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC 1. Row number(rn) sorts all rows in each window sequentially.
// MAGIC
// MAGIC 2.  rank, on the other hand, gives the same sequence number to employees with the same salary, but picks up where the row number(rn) left off for the next employee.
// MAGIC
// MAGIC 3. dense_rank, unlike rank, gives the consecutive number,not the row number, to those who follow the ones with the same salary
// MAGIC
// MAGIC 4. ntile : ntile is a function that takes integer arguments. According to these n integers determined by the user, it divides the window into n and assigns numbers to each group in a sequential manner.
// MAGIC  

// COMMAND ----------


//ranking function
df.withColumn("row_number",row_number.over(windowSpec))
.withColumn("rank",rank.over(windowSpec))
.withColumn("dense_rank",dense_rank.over(windowSpec))
.withColumn("percent_rank",percent_rank.over(windowSpec))
.withColumn("ntileOfTwo",ntile(2).over(windowSpec))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### LAG ve LEAD
// MAGIC
// MAGIC It can be helpful to compare rows with previous or next rows, especially if you have the data in a meaningful order. You can use LAG or LEAD to create columns that pull values from other rows; All you have to do is specify which column you want to pull from and how many rows away you want to pull from. LAG pulls from the top rows, LEAD from the bottom rows.
// MAGIC
// MAGIC Now let’s see how different the salary differs from the previous and next employee in order of id (probably entry order).

// COMMAND ----------

df.withColumn("lag", lag("salary", 1,0).over(Window.partitionBy("department").orderBy(col("salary"))))
 .withColumn("lead", lead("salary", 1, 0).over(Window.partitionBy("department").orderBy(col("salary"))))
 .withColumn("lag_diff", col("salary") - col("lag"))
 .withColumn("lead_diff", col("salary") - col("lead"))
 .show(false)

 /*
 F.lag("salary", 1, 0) ve LAG(salary, 1, 0): 1 indicates how many rows to look up or down, and 0 the default value. If we don’t use default value, null value comes when there is no top or bottom row.
 */

// COMMAND ----------

// MAGIC %md
// MAGIC ####Window Aggregate Functions
// MAGIC >how to calculate sum, min, max for each department using Spark SQL Aggregate window functions and WindowSpec. When working with Aggregate functions, we don’t need to use order by clause.

// COMMAND ----------

val windowSpecAgg  = Window.partitionBy("department")

val aggDF = df.withColumn("row",row_number.over(windowSpec))
    .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
    .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
    .withColumn("min", min(col("salary")).over(windowSpecAgg))
    .withColumn("max", max(col("salary")).over(windowSpecAgg))
    // .where(col("row")===1).select("department","avg","sum","min","max")
    .show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Top 2 salaries for each department

// COMMAND ----------

df.withColumn("salaryRank", rank.over(Window.partitionBy("department").orderBy(col("salary").desc) ) )
.filter(col("salaryRank") === 2).drop("salaryRank")
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #Grouping sets, rollup, cube
// MAGIC [ref link](https://learnsql.com/blog/sql-grouping-sets-clause/)
// MAGIC
// MAGIC **NOTE**
// MAGIC ```
// MAGIC Grouping sets depend on null values for aggregation levels. If you do not filter-out null values, you
// MAGIC will get incorrect results. This applies to cubes, rollups, and grouping sets
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ####Grouping sets
// MAGIC >only available in sql, 
// MAGIC >GROUPING SETS are groups, or sets, of columns by which rows can be grouped together. Instead of writing multiple queries and combining the results with a UNION, you can simply use GROUPING SETS.
// MAGIC
// MAGIC GROUPING SETS in SQL can be considered an extension of the GROUP BY clause. It allows you to define multiple grouping sets in the same query.
// MAGIC
// MAGIC ```
// MAGIC SELECT
// MAGIC     aggregate_function(column_1)
// MAGIC     column_2,
// MAGIC     column_3,
// MAGIC FROM
// MAGIC     table_name
// MAGIC GROUP BY
// MAGIC     GROUPING SETS (
// MAGIC         (column_2, column_3),
// MAGIC         (column_2),
// MAGIC         (column_3),
// MAGIC         ()
// MAGIC );
// MAGIC ```

// COMMAND ----------

spark.sql("SELECT * FROM payment ORDER BY payment_date;").show()

// COMMAND ----------

//Imagine we are preparing a report and we want to see one total for each store. The SUM() aggregate function can help us with this. 
//The results are aggregated by each unique combination of year and store.
spark.sql("""
select 
sum(payment_amount) as totalPayment,
year(payment_date) as payment_year,
store_id as store
from payment
group by year(payment_date), store_id
order by year(payment_date),store_id
""").show()

/*
However, we can’t see the total payments by year: the total payments for 2018, 2019, 2020, or 2021. We cannot see the totals by store either, which would be a useful metric to have. Using GROUPING SETS allows us to view these totals.
*/

// COMMAND ----------

/**
If you use the GROUP BY like this, you need multiple UNION ALL clauses to combine the data from different sources. UNION ALL also requires all result sets to have the same number of columns with compatible data types, so you need to adjust the queries by adding a NULL value where required
*/
spark.sql("""
--calculate sum of total payemnt , for each year and storeid
with temp as(select payment_amount as paymentAmount, year(payment_date) as payementYear, store_id as storeId from payment)
select sum(paymentAmount), payementYear, null as storeid from temp  group by  payementYear  
union
select sum(paymentAmount), null as payementYear, storeid from temp  group by  storeid  
""").show()

// COMMAND ----------

spark.sql("""
with temp as(select payment_amount as paymentAmount, year(payment_date) as payementYear, store_id as storeId from payment)
SELECT
  SUM(paymentAmount),
  payementYear,
  storeId
FROM temp
GROUP BY  grouping sets(payementYear, storeId) 
ORDER BY payementYear, storeId;
""").show()
//so, it's running two query ..as above cell

// COMMAND ----------

spark.sql("""
with temp as(select payment_amount as paymentAmount, year(payment_date) as payementYear, store_id as storeId from payment)
SELECT
  SUM(paymentAmount),
  payementYear,
  storeId
FROM temp
GROUP BY  grouping sets(
(payementYear, storeId),
(payementYear),
(storeId))
ORDER BY payementYear, storeId;
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Rollups
// MAGIC >Similar to GROUPING SETS, you can use the ROLLUP option in a single query to generate multiple grouping sets.
// MAGIC >ROLLUP assumes a hierarchy among the input columns. For example, if the input columns are:
// MAGIC GROUP BY ROLLUP(column_1,column_2)
// MAGIC
// MAGIC ```the hierarchy for this is column_1 > column_2, and ROLLUP generates the following grouping sets:
// MAGIC (column_1, column_2)
// MAGIC (column_1)
// MAGIC ()
// MAGIC ```
// MAGIC
// MAGIC >ROLLUP generates all grouping sets that make sense in this hierarchy. It generates a subtotal row every time the value of column_1 changes; this is the hierarchy we have provided. For this reason, we often use ROLLUP to generate subtotals and grand totals in reporting. The ordering of your columns in ROLLUP is very important.
// MAGIC
// MAGIC Let's look at a query that uses ROLLUP

// COMMAND ----------

val rolupdf = spark.sql("""
with temp as(select payment_amount as paymentAmount, year(payment_date) as payementYear, store_id as storeId from payment)
SELECT
sum(paymentAmount),
payementYear,
storeid
from temp
group by rollup(payementyear, storeid)
order by payementyear, storeid
""")
rolupdf.show()

// COMMAND ----------

rolupdf.where(col("payementyear").isNull).show() // total payment 

// COMMAND ----------

rolupdf.where(col("storeid").isNull).show() //year wise total payment

// COMMAND ----------

rolupdf.where(col("storeid").isNotNull).show() //year wise,store wise total payment

// COMMAND ----------

/**
1. total payment all year
2. total payemnt for each year
3. total payment for each  store year wise
**/

// COMMAND ----------

// MAGIC %md
// MAGIC #### cube
// MAGIC
// MAGIC >The CUBE is like combining GROUPING SETS and ROLLUP. It shows the detailed output of both.

// COMMAND ----------

val cubedf = spark.sql("""
with temp as(select payment_amount as paymentAmount, year(payment_date) as payementYear, store_id as storeId from payment)
SELECT
sum(paymentAmount),
payementYear,
storeid
from temp
group by cube(payementyear, storeid)
order by payementyear, storeid
""")

cubedf.show()

// COMMAND ----------

/**
1. total payment all year
2. total payemnt for each year
3. total payment for each  store year wise
4. cube df also include total payement by each store on all the data
**/


// COMMAND ----------

// MAGIC %md
// MAGIC #### Grouping Metadata
// MAGIC
// MAGIC ~
// MAGIC 1. total payment all year, irespecive of year and storeid  --> gid => 3
// MAGIC 2. total payemnt for each year, irrespecive of storeid  --> gid => 1
// MAGIC 3. total payment for each  store year wise --> gid => 0
// MAGIC 4. cube df also include total payement by each store, irrespecive of years  ==>  gid ==> 2
// MAGIC
// MAGIC

// COMMAND ----------

val paydf = spark.sql("select payment_amount as paymentAmount, year(payment_date) as payementYear, store_id as storeId from payment")

// COMMAND ----------

val finaldf = paydf.cube(col("payementYear"), col("storeId"))
.agg(f.grouping_id().as("gid"), f.sum(f.col("paymentAmount")).as("sumAmount"))
.orderBy(col("payementYear").desc, col("storeId").desc)

finaldf.show()

// COMMAND ----------

finaldf.where(col("gid") === 2).show()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###Pivot
// MAGIC
// MAGIC >Pivots make it possible for you to convert a row into a column. 
// MAGIC >transpose a dataset from rows to columns and columns to rows. Pivot is one of the techniques that allows transposing of rows to columns and performs possible aggregations along the way

// COMMAND ----------

df.show()

// COMMAND ----------

val pivotedDF = df.groupBy("department").pivot("employee_name").agg(sum(col("salary")))
pivotedDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC unpivote df
// MAGIC
// MAGIC ```
// MAGIC df.selectExpr(“row_label_column“, “stack(, , , , …)”)
// MAGIC
// MAGIC     row_label_column: Here in our case Country is row_label_column.
// MAGIC     n_columns: No of Columns. Here it is 4
// MAGIC     col0_label: Column Name
// MAGIC     col0_value: Column Value
// MAGIC ```
// MAGIC

// COMMAND ----------

val name = df.select("employee_name").collect.toList.map(row => row.getAs[String](0)).mkString("\"","','","\"")

// COMMAND ----------

// pivotedDF.selectExpr(
//   "department", "stack(9, 'James',James, 'Michael',Michael,'Robert',Robert,'Maria',Maria,'James',James,'Scott',Scott,'Jen',Jen,'Jeff',Jeff,'Kumar',Kumar,'Saif',Saif) as (employee_name,salary)"
// )

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ####User-Defined Aggregation Functions (UDAF)
