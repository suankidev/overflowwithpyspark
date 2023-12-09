// Databricks notebook source
// MAGIC %md
// MAGIC 1. [What is mean , median, mode](https://www.khanacademy.org/math/cc-sixth-grade-math/cc-6th-data-statistics/mean-and-median/v/mean-median-and-mode)
// MAGIC 2. [What is skewness and kurtosis of data](https://www.youtube.com/watch?v=deoph-Rfc24&list=PLRNL7AjA6rjx0L7i1QRCJXNhdujE0mkDr)
// MAGIC 3. [solution to skewness](https://medium.com/@diehardankush/how-to-understanding-data-skewness-in-apache-spark-9e93b9a68f46#id_token=eyJhbGciOiJSUzI1NiIsImtpZCI6ImU0YWRmYjQzNmI5ZTE5N2UyZTExMDZhZjJjODQyMjg0ZTQ5ODZhZmYiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiIyMTYyOTYwMzU4MzQtazFrNnFlMDYwczJ0cDJhMmphbTRsamRjbXMwMHN0dGcuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiIyMTYyOTYwMzU4MzQtazFrNnFlMDYwczJ0cDJhMmphbTRsamRjbXMwMHN0dGcuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMDEyMzk4ODY1NTMzMTYxNjY3NzciLCJlbWFpbCI6ImplZXQuaWRlYUBnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmJmIjoxNzAxNTY2ODk4LCJuYW1lIjoiU3VqZWV0IEt1bWFyIiwicGljdHVyZSI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0wwSTFIaTlibnc3YzZXUFFIb2dtbVdhUDRCYlZFa1ctMTB4NGdXcElTa2FFTT1zOTYtYyIsImdpdmVuX25hbWUiOiJTdWplZXQiLCJmYW1pbHlfbmFtZSI6Ikt1bWFyIiwibG9jYWxlIjoiZW4tR0IiLCJpYXQiOjE3MDE1NjcxOTgsImV4cCI6MTcwMTU3MDc5OCwianRpIjoiNzU0ZTUxM2M3MzI3YjhiY2U2MzlmOGJhMzNkMmM5ZjkyNzk5OTY2YyJ9.ns7Ple09BIqAyir-DEWTni6hqra62-WTSYfXV7cka51uIJ7xvtb_VTqdRxUC4RzZyE9bf946k77lzoI5BBd7VInji-WL-TunYBLtYkMA_sN9NLtSj-tqgDWQqP-BVef1fNFLanG4Y3sb0XOoeAWEfdTVHHctSWkwD9gz88o5nFKiv44V3rzaLvGbeiE6TFPafAkrRDnQWr-y2EPKqt07VgNR7S5HYUNexsHqPFVCqhNQRStYykHc-umxD3NbfJ5vCQ9ndDDbPQCRAuvqL2r36hTgM5OFs9E864DoYufY9fE1F2HhtHRo-zWjvh2aUqK_BdK7-HKa2xs5sKM5M4xW9Q)
// MAGIC
// MAGIC
// MAGIC Statics : mean , meadian, mode
// MAGIC
// MAGIC example1:  4, 3, 1, 6, 1,7
// MAGIC
// MAGIC 1. Arithmetic mean 
// MAGIC *It is a measure of the center of a data set*
// MAGIC   - => (4 +3 +3 +1 +6 + 1 + 7) /6 = 22/6 = 3.6
// MAGIC
// MAGIC 2. median (central tendency)->  1 1 **3 4** 6 7   => 3.5   //mean of middle number is median =>3 + 4/2  => 3.5 
// MAGIC    - 0, 7, 50, 100000, 100000   //median is 50  can be calucalte 
// MAGIC
// MAGIC 3. mode(most common number in the dataset)  -> so answer is 1 for above datasets  
// MAGIC
// MAGIC
// MAGIC example:  we have different transaction for a diffrent cutomer
// MAGIC
// MAGIC ```
// MAGIC     cust    transaction   typeOfTransaction  transactionAmount
// MAGIC      a      4567          neft                +200
// MAGIC      a      6868          imps                -300
// MAGIC      a      3833           balance inquirey    0
// MAGIC      b      3434          imps                 +2000  
// MAGIC      b      2342           balance inquirey    0
// MAGIC      c      6546868        imps                +456546
// MAGIC      c      3824333        balance inquirey    0
// MAGIC ```
// MAGIC df.groupBy(cust).agg( mean(transactionAmount), mod(transactionAmount), midian(transactionAmount))
// MAGIC

// COMMAND ----------

// MAGIC %run /007/CommonUtils/SetupData

// COMMAND ----------

transactionDF.show(5,false)

// COMMAND ----------

transactionDF.groupBy(f.col("cust_id")).agg(f.count("txn_id")).show() 

// COMMAND ----------

retailDF.groupBy("country")
.agg(f.mean(f.col("quantity")), f.median(f.col("quantity")),f.mode(f.col("quantity")))
.show()

// COMMAND ----------

val data = Seq(
  (101, "imps", 10),
  (103, "neft", 50),
  (201, "imps", 40),
  (101, "upi",  20)
)


val df = data.toDF("trid","type","amnt")

df.show()



// COMMAND ----------

df.select(  f.mean( f.col("amnt") )  ,
            f.median(f.col("amnt")) , 
            f.mode(f.col("amnt")) ,
            f.skewness(f.col("amnt")),
            f.kurtosis(f.col("amnt"))          
          )
          .show(false)

//positive skewness  :  
//negative skeweness:  
/*
skewenes  => mean - mode /standard deviation
*/

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

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
