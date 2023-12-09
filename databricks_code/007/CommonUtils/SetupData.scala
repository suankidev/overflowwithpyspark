// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window 

// COMMAND ----------

object CreateTables{

    def cleanupdbfs:Unit={

      dbutils.fs.rm("/user/hive/warehouse/payment",true)

    }

    def createPaymnetTable(spark:org.apache.spark.sql.SparkSession):Unit={
      spark.sql("""create database if not exists sk""") 
      spark.sql("""use sk""")
      spark.sql("""drop table if exists payment""")
      cleanupdbfs
      spark.sql("""  
                           CREATE TABLE if not exists  payment (payment_amount decimal(8,2), payment_date date, store_id int)
                           USING DELTA;
               """)

      spark.sql(
                      """
                      INSERT INTO payment
                        VALUES
                        (1200.99, '2018-01-18', 1),
                        (189.23, '2018-02-15', 1),
                        (33.43, '2018-03-03', 3),
                        (7382.10, '2019-01-11', 2),
                        (382.92, '2019-02-18', 1),
                        (322.34, '2019-03-29', 2),
                        (2929.14, '2020-01-03', 2),
                        (499.02, '2020-02-19', 3),
                        (994.11, '2020-03-14', 1),
                        (394.93, '2021-01-22', 2),
                        (3332.23, '2021-02-23', 3),
                        (9499.49, '2021-03-10', 3),
                        (3002.43, '2018-02-25', 2),
                        (100.99, '2019-03-07', 1),
                        (211.65, '2020-02-02', 1),
                        (500.73, '2021-01-06', 3);
                          """
                        )


    }


  
}



// COMMAND ----------


object Setup{
    lazy val transactionDF = loadParquetDataSet("transaction.parquet")
    lazy val customerDF = loadParquetDataSet("customer.parquet")
    lazy val retailDF =   spark.read.option("inferSchema",true).option("header",true).csv(Setup.retailDataPath)
    lazy val payementDF = CreateTables.createPaymnetTable(spark)
    
    def departmentDF(spark:org.apache.spark.sql.SparkSession):org.apache.spark.sql.DataFrame={


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
            df

    }
   
   def printPath={
          println(s"DataPath.basePath: ${Setup.basePath}")
          println(s"DataPath.filghtData: ${Setup.flightDataPath}")
          println(s"DataPath.retailData: ${Setup.retailDataPath}")
          println()
          println("Method Available as per below")
          println(" setup.loadParquetDataSet(name:String):DataFrame=spark.read.parquet(name)")
          println("transactionDF:DataFrame")
          println("customerDF:DataFrame")
          println("retailDF:DataFrame")

   }

   def loadParquetDataSet(name:String):org.apache.spark.sql.DataFrame=spark.read.parquet(s"${Setup.basePath}/${name}")

println("Creating payment table")

val basePath = "dbfs:/FileStore/tables/suankiData"
val flightDataPath = s"$basePath/flightData"
val retailDataPath = s"$basePath/retailData"

}



// COMMAND ----------

//creating tables

lazy val transactionDF = Setup.loadParquetDataSet("transaction.parquet")
lazy val customerDF = Setup.loadParquetDataSet("customer.parquet")
lazy val retailDF =   spark.read.option("inferSchema",true).option("header",true).csv(Setup.retailDataPath)
lazy val departmentDF = Setup.departmentDF(spark)
Setup.payementDF


// COMMAND ----------


