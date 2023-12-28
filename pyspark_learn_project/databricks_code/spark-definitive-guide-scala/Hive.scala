// Databricks notebook source
        
javatpoint.com/hive-architecure



//get the help
hive --help

--hivevar  <key=value>
-i <file_name>   initialization sql file
-S  silent mode in interactive shell>
-v   verbose mode
--database $dbName
--hiveconf <property=value>


//hive   Or hive --serice-cli

hive --service cli --help



//to redirect all the properties  -e is to use to exute the command

hive -e "SET;" > /tmp/hive_properties.txt


//few important properties
hive.execution.engine=mr
hive.exec.max.created.files=100000                 
hive.exec.max.dynamic.partitions=5000          
hive.exec.max.dynamic.partitions.pernode=2000     
hive.exec.dynamic.partition=true                   
hive.exec.dynamic.partition.mode=nonstrict         
hive.exec.dynamic.partition.type.conversion=true 

hive.exec.parallel=false
hive.exec.parallel.thread.number=8

hive.metastore.warehouse.external.dir=/warehouse/tablespace/external/hive
hive.metastore.wareshouse.dir=/usr/hive/warehouse/tablespace/

//we can set the properties  with set or using --hiveconf


hive --hiveconf hive.exution.engine=tez

     //or after connecting to hive
     set hive.exution.engine=tez



//important files
ls -lrt /etc/hive/conf

mapred-site.xml

hdfs-site.xml

hive-site.xml --> 
        <configuration>
            <property>hive.metastore.wareshouse.dir</property>
            <value>/usr/hive/warehouse/tablespace/</value>

            <property>hive.exe.dynamic.partition.mode</property>
            <value>true</value>
            ...
            ..
            ..

        </configuration>


hadoop-env.sh

// COMMAND ----------

beeline -e "use somedb; show partitions table_name;"

// COMMAND ----------


create database if not exists demofds;

// COMMAND ----------

//hive roles
javatpoint.com/hive-architecure

//metastore db cab ve ntsql/derby

important tables-->


// COMMAND ----------

//creating tables
https://cwiki.apache.org/confluence/display/hive/languagemanual

create table swcstg(
TestColOne string,
TestColTwo string,
TestColThree string,
TestColfour string,
TestColfive string,
TestColSix string,
TestColSeven string,
TestColEight string,
TestColNine string,
TestColTen string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE

//hive
show create table swcstg;
describe formatted swcstg;

load data inpath 'testfile.csv' into table swcstg


// COMMAND ----------

//loading data into table
1. load data 
2. insert into table 


// COMMAND ----------


//load data will copy the file into hive table location, here file will be not moved to table location only copied from locat to hdfs tbale location
//if you run below command more than one..itw will just make copy_1, copy_2....
load data local inpath  "somepath"  into table swcstg   //during table creation we have define delimmited and line termination
load data local inpath  "somepath"  overwrite into table swcstg    //by default it wiil append without overwrite

//from hdfs once you load the data file will be moved from sometest to table location (mind it)

load data inpath '/somtest/test_file.csv' into table swcstg
load data inpath '/somtest/test_file.csv' overwrite into table swcstg



// COMMAND ----------

2. insert vs append

create table swcstgOne(
TestColOne string,
TestColTwo string,
TestColThree string,
TestColfour string,
TestColfive string,
TestColSix string,
TestColSeven string,
TestColEight string,
TestColNine string,
TestColTen string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE

insert into table swcstgOne
select * from swcstg

// COMMAND ----------

create external table swcstgThree(
TestColOne string,
TestColTwo string,
TestColThree string,
TestColfour string,
TestColfive string,
TestColSix string,
TestColSeven string,
TestColEight string,
TestColNine string,
TestColTen string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
LOCATION '/apps/f1scdsr/dwh/f1scdsr.db/feed_temp_file/somtest'
TBLPROPERTIES ("skip.header.line.count"="1");   //skip the header

// COMMAND ----------

purpose of external tables ?

--> multiple framework is using the data 

// COMMAND ----------

//just to show we can crete hive table here
spark.sql("""
create table swcstgOne(
TestColOne string,
TestColTwo string,
TestColThree string,
TestColfour string,
TestColfive string,
TestColSix string,
TestColSeven string,
TestColEight string,
TestColNine string,
TestColTen string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
""")

// COMMAND ----------

spark.sql("describe formatted  swcstgone").show()

// COMMAND ----------

dbutils.fs.ls("/user/hive/warehouse").foreach(println)

// COMMAND ----------



spark.sql("""
create external table myexternalTable(
  destination string,
  origin string,
  totalCount Int
)
row format delimited by fields terminated by ","
lines terminated by "\n"
location
""")




// COMMAND ----------

//diff RDBMS VS HIVE

RDBMS Tables are Schema on Write. For each and every write operation there will be validations such as Data Types. Scale, Precision, Check Constraints, Null Constraints. Unique Constraints performed.
RDBMS Tables are fine tuned for best of the for transactions (POS. Bank Transfers
etc) whereas Hive is meant for heavy weight batch data processing.
where we can create indexes which are populated live in RDBMS. In Hive, indexes are typically static and when we ever add the data. indexes have to be rebuilt.
Even though one can specify constraints in Hive tables. They are only informational. The constraints might not be enforced.
We don’t perform ACID Transactions in Hive Tables.
There are no Transaction based statements such as COMMIT. ROLLBACK etc. in Hive.
Metastore and Data are decoupled in Hive. Metastore is available in RDBMS and actual business data is typically stored in HDFS.

// COMMAND ----------



// COMMAND ----------

//truncating and dropping the table
show tables;
drop table swcstg;
truncate table swcstg;
drop databse demo




// COMMAND ----------

//putting data into final table
/*
by default it's List partitioning
bu buccketing --> it's hash paritioning
**/
create table swcstg_final(
TestColOne string,
TestColTwo string,
TestColThree string,
 TestColfive string,
TestColfour string,
TestColSix string,
TestColSeven string,
TestColEight string,
TestColNine string,
 TestColTen string
)
STORED AS parquet  


/*
when it comes to other format than textfile, it should know the delimiter, we are not able to 'load data inpath'

--> insert overwirt into tabl eselect * from sometable will only work


so,

 create stg  --> load datain in stg ---> insert into swcstg_final
**/

insert into table swcstg_final select * from swcstg
insert overwrite table swcstg_final select * from swcstg






// COMMAND ----------

//partitions stg table

create table swcstgpartitioned(
TestColOne string,
TestColTwo string,
TestColThree string,
TestColfour string,
TestColSix string,
TestColSeven string,
TestColEight string,
TestColNine string,
 TestColTen string
)
partitioned by (TestColfive string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE

alter table swcstgpartitioned add partition(TestColfive="product")  //will add empty partiton
alter table swcstgpartitioned add partition(TestColfive="product")partition(TestColfive="er")

+----------------------+
|      partition       |
+----------------------+
| testcolfive=er       |
| testcolfive=product  |
+----------------------+

now we can simple copy the files
load data local inpath "path.csv" into table swcstgpartitioned add partition(TestColfive="product")

//=================>final table partitioned

create table swcstgpartitionedOne(
TestColOne string,
TestColTwo string,
TestColThree string,
TestColfour string,
TestColSix string,
TestColSeven string,
TestColEight string,
TestColNine string,
TestColTen string
)
partitioned by (TestColfive string)
stored as parquet


insert into swcstgpartitionedOne partition(TestColfive)   //if you mention manuaaly value will be static partition
select 
TestColOne ,
TestColTwo ,
TestColThree ,
TestColfour ,
TestColSix ,
TestColSeven ,
TestColEight ,
TestColNine ,
TestColTen ,
TestColfive
from swcstg


// COMMAND ----------

//truncating partition table

truncate table swcstgpartitionedOne partition(TestColfive='product')

// COMMAND ----------

//bucketing

set hive.enfore.bucketing=true

create table test(
  a int,
  b int
)cluster by (a) into 3 bucket
row format delimited fields terminated by ";"



insert into test select * from test_stg;




// COMMAND ----------

//ORC fileformate is compatible with ACID properties


set hive.support.concurrency=true
set hive.enfore.bucketing=true
hive.exec.dynamic.partition.mode=nonstrict
hive.txn.manager=





// COMMAND ----------

// MAGIC %md
// MAGIC ###Exercise

// COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/suankiData") foreach println

// COMMAND ----------

// MAGIC %fs
// MAGIC rm -r dbfs:/user/hive/warehouse/retail_db.db/

// COMMAND ----------

spark.sql("""
create database if not exists retail_db
""")

// COMMAND ----------

spark.sql("""show databases""").show()

// COMMAND ----------

spark.sql("use retail_db")

// COMMAND ----------

// MAGIC %sql
// MAGIC select current_schema()

// COMMAND ----------

spark.sql("drop table if exists retail_db.orders")

// COMMAND ----------

spark.sql("""
create table if not exists orders( 
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
""")

// COMMAND ----------



// COMMAND ----------

spark.sql("describe formatted orders").show(40,truncate=false)

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls dbfs:/FileStore/tables/suankiData/orders/part_00000

// COMMAND ----------

spark.sql("""
load data inpath 'dbfs:/FileStore/tables/suankiData/orders/part_00000' into table retail_db.orders
""")

/**
Note:
1.  load data inpath  --> will move the file from hdfs to table location 
2. load data local inpath --> will copy the file from local to table location

3. overwrite the existing data with new data

        load data inpath 'dbfs:/FileStore/tables/suankiData/orders/part_00000' overwrite into table orders
**/


// COMMAND ----------

spark.sql("select *  from orders").show(truncate=false)

// COMMAND ----------

//Creating external tables

spark.sql("""
create external table if not exists ordersAsExternalTable( 
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "dbfs:/FileStore/tables/suankiData/orders/"
""")

// COMMAND ----------

spark.sql("describe formatted ordersAsExternalTable ").show(40,false)

// COMMAND ----------

display(spark.sql("select * from ordersAsExternalTable limit 10"))

// COMMAND ----------

spark.sql("""
create external table if not exists order_item_stg(
  order_item_id  INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_sub_totabl FLOAT,
  order_item_product_price FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
LOCATION "dbfs:/FileStore/tables/suankiData/order_items/"
""")

// COMMAND ----------

spark.sql("select * from order_item_stg limit 5").show()

// COMMAND ----------

spark.sql("""
create table if not exists order_item(
  order_item_id  INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_sub_totabl FLOAT,
  order_item_product_price FLOAT
)
STORED AS ORC
""")

// COMMAND ----------

spark.sql("""
insert into order_item select * from order_item_stg
""")

// COMMAND ----------

spark.sql("""
select  * from order_item limit 5
""").show()

// COMMAND ----------

//Creationg partitioned table in hive order_month as key
spark.sql("""
create table if not exists orderAsPartitionedTable( 
  order_id int,
  order_date string,
  order_customer_id int
)
PARTITIONED BY (order_status STRING)
STORED AS PARQUET
""")



// COMMAND ----------

spark.sql("""
insert into orderAsPartitionedTable select order_id,order_date,order_customer_id,order_status from orders
""")

// COMMAND ----------

spark.sql("describe formatted orderAsPartitionedTable").show(40,false)

// COMMAND ----------

// MAGIC %fs
// MAGIC
// MAGIC ls dbfs:/user/hive/warehouse/retail_db.db/orderaspartitionedtable

// COMMAND ----------

// MAGIC %md
// MAGIC ###Apache hive Functions

// COMMAND ----------

// MAGIC %sql
// MAGIC describe function substr

// COMMAND ----------

// MAGIC %sql
// MAGIC describe function substring

// COMMAND ----------

// MAGIC %sql
// MAGIC select current_date()

// COMMAND ----------

/**
lower
upper
initcap
**/

// COMMAND ----------

// MAGIC %sql
// MAGIC select current_date(), initcap("hello"), lower("HeLLo"), length("test")

// COMMAND ----------

/**
date_add('2018-12-30',365)
date_sub
datediff
add_months('2018-12-30',2)

**

// COMMAND ----------

// MAGIC %sql
// MAGIC select * , substr(order_date,1,4) as year , curdate() as curr_date from orders limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC select trunc(current_date(),'MM' ) as firstDateOfMonth, trunc(current_date(),'YY' ) as firstDateOfYear,
// MAGIC date_format(current_date(), 'yy') as yearOnly , date_format(current_date(), 'yyMMdd')

// COMMAND ----------

// MAGIC %sql
// MAGIC show functions

// COMMAND ----------

// MAGIC %sql
// MAGIC desc order_item

// COMMAND ----------

// MAGIC %sql
// MAGIC --orderBy and distributed by
// MAGIC
// MAGIC select order_item_order_id,
// MAGIC sum(order_item_sub_totabl) as order_revenue
// MAGIC from retail_db.order_item
// MAGIC group by order_item_order_id
// MAGIC limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC --orderBy and distributed by
// MAGIC
// MAGIC select order_item_order_id,
// MAGIC sum(order_item_sub_totabl) as order_revenue
// MAGIC from retail_db.order_item
// MAGIC group by order_item_order_id
// MAGIC having order_revenue>500
// MAGIC limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from retail_db.orders order by order_customer_id    limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC -- map task  --> shuffle --> reduce tasks
// MAGIC -- distribution is used to distribute the data while shuffle stage
// MAGIC --
// MAGIC select order_item_order_id,
// MAGIC sum(order_item_sub_totabl) as order_revenue
// MAGIC from retail_db.order_item
// MAGIC group by order_item_order_id
// MAGIC distribute by order_item_order_id --use when global sorting is not required
// MAGIC limit 10

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC insert into table stock_eod_sortby
// MAGIC select * from stocks_eod
// MAGIC distribute by tradedate sort by tradedate,volume desc
// MAGIC --date will be distributed by tradedate and withing each mapper ..will sort by tradedate and volumn ,, so mapper and reducer may increase
// MAGIC --cluster by tradedate

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from retail_db.orders
// MAGIC where not exists(
// MAGIC   select 1 from retail_db.order_item
// MAGIC   where order_item.order_item_order_id = orders.order_id
// MAGIC )
// MAGIC limit 5
// MAGIC
// MAGIC --co-related subqueries

// COMMAND ----------

// MAGIC %md
// MAGIC ###Join
// MAGIC notw: there is no outer join in hive outer join is left join
// MAGIC
// MAGIC links: https://data-flair.training/blogs/hive-join/
// MAGIC
// MAGIC --left join
// MAGIC
// MAGIC --right join
// MAGIC
// MAGIC --inner join
// MAGIC
// MAGIC --left outer join
// MAGIC
// MAGIC --right outer join
// MAGIC
// MAGIC --mapside join
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC --joining multiple tables
// MAGIC --all the order_detail from order_items for order_id in order
// MAGIC
// MAGIC select o.order_id, o.order_Date,o.order_status,
// MAGIC    oi.order_item_product_id, oi.order_item_sub_totabl
// MAGIC FROM retail_db.orders o inner join retail_db.order_item oi 
// MAGIC     ON o.order_id = oi.order_item_order_id limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC select o.order_id, o.order_Date,o.order_status,
// MAGIC    oi.order_item_product_id, oi.order_item_sub_totabl
// MAGIC FROM retail_db.orders o left join retail_db.order_item oi 
// MAGIC     ON o.order_id = oi.order_item_order_id limit 10
// MAGIC
// MAGIC     --null in order_item b/c order_id 3 is not present in order_item

// COMMAND ----------

// MAGIC %sql
// MAGIC --o.order_id, o.order_Date,o.order_status,
// MAGIC    --oi.order_item_product_id, oi.order_item_sub_totabl
// MAGIC select count(*)
// MAGIC FROM retail_db.orders o right  join retail_db.order_item oi 
// MAGIC     ON o.order_id = oi.order_item_order_id limit 10
// MAGIC
// MAGIC     --null in order_item b/c order_id 3 is not present in order_item

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC --mapside join vs reduce side join
// MAGIC --by default it's map side join
// MAGIC -- set hive.auto.convert.join=true  
// MAGIC --hive,mapjoin.smalltable.filesize=2500000 if small table size is below this then consider map join
// MAGIC -- meaning smaller table would be cached  to every node and would be join
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC
