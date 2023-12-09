// Databricks notebook source
case class Flight(DEST_COUNTRY_NAME: String,
ORIGIN_COUNTRY_NAME: String, count: BigInt)

//dataset
val flights = spark.read
.format("csv")
.option("header",true).option("inferSchema",true).load("dbfs:/FileStore/tables/suankiData/flight_data")
.as[Flight]

//rdd
val flightRdd = spark.sparkContext.textFile("dbfs:/FileStore/tables/suankiData/flight_data/2011_summary.csv")
val firstRow = flightRdd.first
val finalflightRdd = flightRdd.filter(row => row !=firstRow && !row.trim.contains("Eustatius")).map(row => row.split(","))


//Interoperating Between DataFrames, Datasets, and RDDs
// in Scala: converts a Dataset[Long] to RDD[Long]
//Because Python doesn’t have Datasets—it has only DataFrames—you will get an RDD of type Row:
val rangeOfNumbers = spark.range(500).rdd


val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple Using Spark Diffrent Library for Big Data Technology"
.split(" ")

val words = spark.sparkContext.parallelize(myCollection,2)

// COMMAND ----------

println(s"collectoin of rdd of words: $words")
println(s"data sets of flight as Rdd: $finalflightRdd")
println(s"data sets of flight as CaseClass:Dataset: $flights")


