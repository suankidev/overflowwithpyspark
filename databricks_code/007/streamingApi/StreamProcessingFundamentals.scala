// Databricks notebook source
// MAGIC %md
// MAGIC #### What is Streaming ?
// MAGIC Stream processing is the act of continuously incorporating new data to compute a result. In
// MAGIC stream processing, the input data is unbounded and has no predetermined beginning or end
// MAGIC simply forms a series of events that arrive at the stream processing system (e.g., credit card
// MAGIC transactions, clicks on a website, or sensor readings from Internet of Things [IoT] devices)
// MAGIC
// MAGIC 1. DStreams API : mr rdd
// MAGIC 2. Structure Api: super set fo DStreams,will often perform better due to code generation and the Catalyst
// MAGIC optimizer
// MAGIC
// MAGIC
// MAGIC #### Stream vs Batch
// MAGIC 0. Batch processing computation runs on a
// MAGIC fixed-input dataset. Oftentimes, this might be a large-scale dataset in a data warehouse that
// MAGIC contains all the historical events from an application (e.g., all website visits or sensor readings
// MAGIC for the past month). Batch processing also takes a query to compute, similar to stream
// MAGIC processing, but only computes the result once
// MAGIC
// MAGIC 0. they often need to work
// MAGIC together. For example, streaming applications often need to join input data against a dataset
// MAGIC written periodically by a batch job

// COMMAND ----------

// MAGIC %md
// MAGIC ####Stream Processing Use Cases
// MAGIC
// MAGIC 0. Notifications and alerting
// MAGIC 0. Relea-time reporting
// MAGIC 0. Incremental ETL
// MAGIC 0. update data to serve in real time:  captuing click on web
// MAGIC 0. Rel-time decision making: 
// MAGIC 0. Online machine learning
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ####Challenges of Stream Processing
// MAGIC 0. Processing out-of-order data based on application timestamps (also called event time)
// MAGIC 0. Maintaining large amounts of state
// MAGIC 0. Supporting high-data throughput
// MAGIC 0. Processing each event exactly once despite machine failures
// MAGIC 0. Handling load imbalance and stragglers
// MAGIC 0. Responding to events at low latency
// MAGIC 0. Joining with external data in other storage systems
// MAGIC 0. Determining how to update output sinks as new events arrive
// MAGIC 0. Writing data transactionally to output systems
// MAGIC 0. Updating your application’s business logic at runtime

// COMMAND ----------

// MAGIC %md
// MAGIC #### Stream processing design points
// MAGIC there are multiple ways to design a streaming system
// MAGIC
// MAGIC 1. **Record-at-a-Time Versus Declarative APIs**:
// MAGIC     *The simplest way to design a streaming API would be to just pass each event to the application
// MAGIC     and let it react using custom code. This is the approach that many early streaming systems, such
// MAGIC     as **Apache Storm**, this require deep knowledge on low level api
// MAGIC     As a result, many newer streaming systems provide declarative APIs
// MAGIC     Spark’s original **DStreams API**, for example, offered functional API based
// MAGIC     on operations like map, reduce and filter on streams. Internally, the DStream API automatically
// MAGIC     tracked how much data each operator had processed, saved any relevant state reliably, and
// MAGIC     recovered the computation from failure when needed., System such as **google dataflow and apache kafka** provide similar function*
// MAGIC
// MAGIC 2. **Event Time Versus Processing Time:** 
// MAGIC  *Event time is the idea of processing data based on timestamps inserted into each
// MAGIC record at the source, as opposed to the time when the record is received at the streaming application (which is called processing time)*
// MAGIC     - when using event time, records may
// MAGIC arrive to the system out of order  due to network path or delay from remote device, but if data is likely to receive from local data center then don't need to worry
// MAGIC     When using event-time, several issues become common concerns across applications, including
// MAGIC tracking state in a manner that allows the system to incorporate late events, and determining
// MAGIC when it is safe to output a result for a given time window in event time (i.e., when the system is
// MAGIC likely to have received all the input up to that point).
// MAGIC
// MAGIC
// MAGIC 3. **Continuous Versus Micro-Batch Execution:**
// MAGIC - In continuous processing-based systems, each node in the system is continually
// MAGIC listening to messages from other nodes and outputting new updates to its child nodes.For
// MAGIC example, suppose that your application implements a map-reduce computation over several input
// MAGIC streams. In a continuous processing system, each of the nodes implementing map would read
// MAGIC records one by one from an input source, compute its function on them, and send them to the
// MAGIC appropriate reducer. The reducer would then update its state whenever it gets a new record. The
// MAGIC key idea is that this happens on each individual record
// MAGIC
// MAGIC -  micro-batch systems wait to accumulate small batches of input data (say, 500 ms’
// MAGIC worth), then process each batch in parallel using a distributed collection of tasks, similar to the
// MAGIC execution of a batch job in Spark. Micro-batch systems can often achieve high throughput per
// MAGIC node because they leverage the same optimizations as batch systems (e.g., vectorized
// MAGIC processing), and do not incur any extra per-record overhead,
// MAGIC
// MAGIC Thus, they need fewer nodes to process the same rate of data. Micro-batch systems can also use
// MAGIC dynamic load balancing techniques to handle changing workloads (e.g., increasing or decreasing
// MAGIC the number of tasks). The downside, however, is a higher base latency due to waiting to
// MAGIC accumulate a micro-batch. In practice, the streaming applications that are large-scale enough to
// MAGIC need to distribute their computation tend to prioritize throughput, so Spark has traditionally
// MAGIC implemented micro-batch processing. In Structured Streaming, however, there is an active
// MAGIC development effort to also support a continuous processing mode beneath the same API.
// MAGIC When choosing between these two execution modes, the main factors you should keep in mind
// MAGIC are your desired latency and total cost of operation (TCO). Micro-batch systems can comfortably
// MAGIC deliver latencies from 100 ms to a second, depending on the application. Within this regime, they
// MAGIC will generally require fewer nodes to achieve the same throughput, and hence lower operational
// MAGIC cost (including lower maintenance cost due to less frequent node failures). For much lower
// MAGIC latencies, you should consider a continuous processing system, or using a micro-batch system in
// MAGIC conjunction with a fast serving layer to provide low-latency queries (e.g., outputting data into
// MAGIC MySQL or Apache Cassandra, where it can be served to clients in milliseconds).
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC 1. **The DStream API**:  DStreams API has several limitations
// MAGIC - it is based purely on Java/Python objec and functions
// MAGIC - api is purely based on processing time - to hanle event-time operation, applicaiton needs to implement their own customer hanleing
// MAGIC - DStreams can only operate in a micro-batch fashion
// MAGIC
// MAGIC 2. **Strutured streaming**: Structured Streaming is a higher-level streaming API built from the ground up on Spark’s
// MAGIC Structured APIs. It is available in all the environments where structured processing runs,
// MAGIC including Scala, Java, Python, R, and SQL
// MAGIC
// MAGIC - As of Apache Spark 2.2, the system only
// MAGIC runs in a micro-batch model, but the Spark team at Databricks has announced an effort called
// MAGIC Continuous Processing to add a continuous execution mode.
// MAGIC - Structured Streaming ensures end-to-end, exactly-once
// MAGIC processing as well as fault-tolerance through checkpointing and write-ahead logs.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC **Input Sources:**  
// MAGIC - Apache Kafka
// MAGIC - File on distributed file system like hdfs, s3, deta lake
// MAGIC - a socket source for testing
// MAGIC **Output Sources:**
// MAGIC -  apache kafka
// MAGIC - almost any file format
// MAGIC - a foreach singk for running arbitary computaiton on th output records
// MAGIC - a console sink for testing
// MAGIC - a memory sink fo rdbugging
// MAGIC
// MAGIC **Output Modes**
// MAGIC - Append : only add new records to the output sink
// MAGIC - update: update changed records in place
// MAGIC - complete : rewrite the full output
// MAGIC
// MAGIC **Triggers**:*Whereas output modes define how data is output, triggers define when data is output*
// MAGIC - look for new data at fixed interval,save us hitting sink many times
// MAGIC
// MAGIC **Event-Time Processing**
// MAGIC - Structured Streaming also has support for event-time processing (i.e., processing data based on
// MAGIC timestamps included in the record that may arrive out of order)
// MAGIC
// MAGIC **Watermarks**
// MAGIC - Watermarks are a feature of streaming systems that allow you to specify how late they expect to
// MAGIC see data in event time. For example, in an application that processes logs from mobile devices,
// MAGIC one might expect logs to be up to 30 minutes late due to upload delays

// COMMAND ----------

dbutils.fs.ls("FileStore/tables/suankiData/activity_data") foreach(x => println(x.path))

// COMMAND ----------

//just to check the data type and few data
val staticDF = spark.read.json("/FileStore/tables/suankiData/activity_data")
//val staticDF = spark.read.json(raw"C:\Users\sujee\OneDrive\Documents\bigdata_and_hadoop\scala\Spark-The-Definitive-Guide\data\activity-data")
val staticSchema = staticDF.schema
staticDF.show(5,false)

// COMMAND ----------

val streaming = spark.readStream.schema(staticSchema).option("maxFilesPerTrigger", 1).json("/FileStore/tables/suankiData/activity_data")
//val streaming = spark.readStream.schema(staticSchema).option("maxFilesPerTrigger", 1).json(raw"C:\Users\sujee\OneDrive\Documents\bigdata_and_hadoop\scala\Spark-The-Definitive-Guide\data\activity-data")

// COMMAND ----------

/**Just like with other Spark APIs, streaming DataFrame creation and execution is lazy. In
particular, we can now specify transformations on our streaming DataFrame before finally
calling an action to start the stream*/
spark.conf.set("spark.sql.shuffle.partitions", 5)  //to avoid creating too many paritions
val activityCounts = streaming.groupBy("gt").count()

// COMMAND ----------

//Now that we set up our transformation, we need only to specify our action to start the query
//output sink
//we use the complete output mode. This mode rewrites all of the keys along with their counts after every trigger in form of memory table, so we can query it below
val activityQuery = activityCounts.writeStream.queryName("activity_counts")
.format("memory").outputMode("complete")
.start()

//You’ll notice that we set a unique query name to representthis stream, in this case activity_counts

// COMMAND ----------

/*
after this code executed, the streaming computation will have started in the background
activityQuery.awaitTermination() to prevent
the driver process from exiting while the query is active. We will omit this from our future parts
of the book for readability, but it must be included in your production applications; otherwise,
your stream won’t be able to run
*/
activityQuery.awaitTermination()


// COMMAND ----------

for( i <- 1 to 5 ) {
spark.streams.active foreach println   //n this case, we assigned it to a variable, so that’s not necessary.
spark.sql("SELECT * FROM activity_counts").show()
Thread.sleep(10000)
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Selections and Filtering

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

streaming.withColumn("stairs", expr("gt like '%stairs%'"))
.where("stairs")
.where("gt is not null")
.select("gt", "model", "arrival_time", "creation_time")
.writeStream
.queryName("simple_transform")
.format("memory")
.outputMode("append")
.start()

// COMMAND ----------

spark.streams.active

// COMMAND ----------


for(i <-  1 to 10 ) 
 {
  spark.sql("select * from simple_transform").show()
  Thread.sleep(10000)
 }
 


// COMMAND ----------

// MAGIC %md
// MAGIC #### Aggregations

// COMMAND ----------

val deviceModelStats = streaming.cube("gt", "model").avg()
.drop("avg(Arrival_time)")
.drop("avg(Creation_Time)")
.drop("avg(Index)")
.writeStream.queryName("device_counts").format("memory").outputMode("complete")
.start()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from device_counts;

// COMMAND ----------

val historicalAgg = static.groupBy("gt", "model").avg()
val deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")
.cube("gt", "model").avg()
.join(historicalAgg, Seq("gt", "model"))
.writeStream.queryName("device_counts").format("memory").outputMode("complete")
.start()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Where Data Is Read and Written (Sources and Sinks)
// MAGIC [https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
// MAGIC
// MAGIC 1. local, csv,parqu,json files:  we can control the number of files that we read in during each trigger via the
// MAGIC maxFilesPerTrigger option that we saw earlier.
// MAGIC  Note:  Spark will process partially written files before you have finished. On
// MAGIC file systems that show partial writes, such as local files or HDFS, this is best done by writing the
// MAGIC file in an external directory and moving it into the input directory when finished. On Amazon S3,
// MAGIC objects normally only appear once fully written
// MAGIC
// MAGIC 2. Kfka source and sink:
// MAGIC - Apache Kafka is a distributed publish-and-subscribe system for streams of data
// MAGIC - Kafka lets you
// MAGIC publish and subscribe to streams of records like you might do with a message queue—these are
// MAGIC stored as streams of records in a fault-tolerant way
// MAGIC - Think of Kafka like a distributed buffer.
// MAGIC Kafka lets you store streams of records in categories that are referred to as topics. Each record in
// MAGIC Kafka consists of a key, a value, and a timestamp
// MAGIC - Topics consist of immutable sequences of
// MAGIC records for which the position of a record in a sequence is called an offset. 
// MAGIC - Reading data is called subscribing to a topic 
// MAGIC - and writing data is as simple as publishing to a topic
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC #### Reading from the Kafka Source
// MAGIC ```
// MAGIC To read from Kafka, do the following in Structured Streaming:
// MAGIC // in Scala
// MAGIC
// MAGIC // Subscribe to 1 topic
// MAGIC val ds1 = spark.readStream.format("kafka")
// MAGIC .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
// MAGIC .option("subscribe", "topic1")
// MAGIC .load()
// MAGIC
// MAGIC // Subscribe to multiple topics
// MAGIC val ds2 = spark.readStream.format("kafka")
// MAGIC .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
// MAGIC .option("subscribe", "topic1,topic2")
// MAGIC .load()
// MAGIC
// MAGIC // Subscribe to a pattern of topics
// MAGIC val ds3 = spark.readStream.format("kafka")
// MAGIC .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
// MAGIC .option("subscribePattern", "topic.*")
// MAGIC .load()
// MAGIC
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ### Each row in the source will have the following schema:
// MAGIC - key: binary
// MAGIC - value: binary
// MAGIC - topic: string
// MAGIC - partition: int
// MAGIC - offset: long
// MAGIC - timestamp: long
