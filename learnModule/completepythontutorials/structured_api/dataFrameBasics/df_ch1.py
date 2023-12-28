from pyspark.sql.session import SparkSession
import logging
from pyspark.sql.functions import max, desc

spark = (SparkSession.builder.master("local[2]")
         .appName("dataFrameBasic").getOrCreate())
spark.sparkContext.setLogLevel(logLevel="ERROR")


def ch1():
    name = ["sujeet", "ramesh", "shyam"]
    numdf = spark.range(1000).toDF("num")
    numdf.where("num % 2 == 0").show()


# END to end example

def ch1_a():
    """
    The process of logical and physical DataFrame manipulation:
    The logical plan of transformations that we build up defines a lineage for the DataFrame so that
at any given point in time, Spark knows how to recompute any partition by performing all of the
operations it had before on the same input data

We do not manipulate the physical data; instead, we configure physical execution characteristics
through things like the shuffle partitions parameter that we set a few moments ago. We ended up
with five output partitions because that’s the value we specified in the shuffle partition. You can
change this to help control the physical execution characteristics of your Spark job
    """
    file_path = "C:\\Users\\sujee\\OneDrive\\Documents\\bigdata_and_hadoop\\databricks_udemy" \
                "\\data\\fligt_data\\2015-summary.csv"
    flightData2015 = (spark.read
                      .option("inferSchema", "true").option("header", "true")
                      .csv(file_path)
                      )
    # print(flightData2015.take(5))  #List of Row(col1,col2,col3),Row(col1,col2,col3)]
    # flightData2015.show()
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    sorted_df = flightData2015.sort("count")
    # sorted_df.explain()
    # sorted_df.show()
    sorted_df.createOrReplaceTempView("flightdata")
    sqlWay = spark.sql("select DEST_COUNTRY_NAME,count(*) as count from flightdata group by DEST_COUNTRY_NAME")
    dfway = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
    sqlWay.show()
    dfway.show()
    flightData2015.select(max("count")).show()

    # fine top 5 destination country
    """
    This execution plan is a directed acyclic graph (DAG) of transformations,
each resulting in a new immutable DataFrame, on which we call an action to generate a result
"""
    spark.sql(
        "select DEST_COUNTRY_NAME , sum(count) as numbers from flightdata group by DEST_COUNTRY_NAME order by numbers desc").show(
        5, truncate=False)
    flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)", "dest_total").sort(
        desc("dest_total")).limit(5).show()


# struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:int


"""
Transformation: instrunction to spark 
narrow : each input partition will contribute to one output partition
wide :A wide dependency (or wide transformation) style transformation will have input partitions
contributing to many output partition
You will often hear this referred to as a shuffle whereby
Spark will exchange partitions across the cluster. With narrow transformations, Spark will
automatically perform an operation called pipelining,s
"""

"""
LAZY Evaluation:
Spark will wait until the very last moment to execute the graph of
computation instructions. 

ACTION:
An action instructs Spark to compute a result from a series of transformations.
The simplest action is count
  three type action:
    --action to collect
    --action to view
    --action to write
"""

"""
spark job: 
all you need to understand is that a Spark job represents a set of
transformations triggered by an individual action
"""

"""
Spark lazily executes a DAG of transformations in order to optimize the
execution plan on DataFrames. """
