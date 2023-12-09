// Databricks notebook source
// DBTITLE 1,Joins and optimization
// MAGIC %md
// MAGIC
// MAGIC ####How Spark Performs Joins

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC >you need to understand the two core resources at play:
// MAGIC the node-to-node communication strategy and per node computation strategy
// MAGIC
// MAGIC >Spark approaches cluster communication in two different ways during joins. 
// MAGIC It either incurs a  shuffle join, which results in an **all-to-all communication or a broadcast join(node to node)**. 
// MAGIC
// MAGIC
// MAGIC 1.  **Big table–to–big table**
// MAGIC
// MAGIC In a shuffle join, every node talks to every other node and they share data according to which
// MAGIC node has a certain key or set of keys (on which you are joining). These joins are expensive
// MAGIC because the network can become congested with traffic, especially if your data is not partitioned
// MAGIC well
// MAGIC
// MAGIC 2. **Big table–to–small table**
// MAGIC When the table is small enough to fit into the memory of a single worker node, with some
// MAGIC breathing room of course, we can optimize our join. Although we can use a big table–to–big
// MAGIC table communication strategy, it can often be more efficient to use a broadcast join
// MAGIC what this does is
// MAGIC prevent us from performing the all-to-all communication during the entire join process. Instead,
// MAGIC we perform it only once at the beginning and then let each individual worker node perform the
// MAGIC work without having to wait or communicate with any other worker node, 

// COMMAND ----------

// MAGIC %run /007/CommonUtils/SetupData

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC transaction df
// MAGIC total: 39790092
// MAGIC +----------+----------+----------+---------------+----------+----+-----+---+-------------+------+-----------+
// MAGIC |cust_id   |start_date|end_date  |txn_id         |date      |year|month|day|expense_type |amt   |city       |
// MAGIC +----------+----------+----------+---------------+----------+----+-----+---+-------------+------+-----------+
// MAGIC |C0YDPQWPBJ|2010-07-01|2018-12-01|TZ5SMKZY9S03OQJ|2018-10-07|2018|10   |7  |Entertainment|10.42 |boston     |
// MAGIC |C0YDPQWPBJ|2010-07-01|2018-12-01|TYIAPPNU066CJ5R|2016-03-27|2016|3    |27 |Motor/Travel |44.34 |portland   |
// MAGIC |C0YDPQWPBJ|2010-07-01|2018-12-01|TETSXIK4BLXHJ6W|2011-04-11|2011|4    |11 |Entertainment|3.18  |chicago    |
// MAGIC |C0YDPQWPBJ|2010-07-01|2018-12-01|TQKL1QFJY3EM8LO|2018-02-22|2018|2    |22 |Groceries    |268.97|los_angeles|
// MAGIC |C0YDPQWPBJ|2010-07-01|2018-12-01|TYL6DFP09PPXMVB|2010-10-16|2010|10   |16 |Entertainment|2.66  |chicago    |
// MAGIC +----------+----------+----------+---------------+----------+----+-----+---+-------------+------+-----------+
// MAGIC
// MAGIC
// MAGIC customer df
// MAGIC total: 5000
// MAGIC +----------+-------------+---+------+----------+-----+-----------+
// MAGIC |cust_id   |name         |age|gender|birthday  |zip  |city       |
// MAGIC +----------+-------------+---+------+----------+-----+-----------+
// MAGIC |C007YEYTX9|Aaron Abbott |34 |Female|7/13/1991 |97823|boston     |
// MAGIC |C00B971T1J|Aaron Austin |37 |Female|12/16/2004|30332|chicago    |
// MAGIC |C00WRSJF1Q|Aaron Barnes |29 |Female|3/11/1977 |23451|denver     |
// MAGIC |C01AZWQMF3|Aaron Barrett|31 |Male  |7/9/1998  |46613|los_angeles|
// MAGIC |C01BKUFRHA|Aaron Becker |54 |Male  |11/24/1979|40284|san_diego  |
// MAGIC +----------+-------------+---+------+----------+-----+-----------+
// MAGIC
// MAGIC ```
// MAGIC
// MAGIC

// COMMAND ----------

transactionDF.join(customerDF, transactionDF.col("cust_id") ===  customerDF.col("cust_id")).explain

// COMMAND ----------

// MAGIC %md
// MAGIC Note:
// MAGIC
