# Databricks notebook source
# MAGIC %run "./prepare_user_df"

# COMMAND ----------

user_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Overview of Narrow and Wide Transformations
# MAGIC
# MAGIC Let us get an overview of Narrow and Wide Transformations.
# MAGIC * Here are the functions related to narrow transformations. Narrow transformations doesn't result in shuffling. These are also known as row level transformations.
# MAGIC   * `df.select`
# MAGIC   * `df.filter`
# MAGIC   * `df.withColumn`
# MAGIC   * `df.withColumnRenamed`
# MAGIC   * `df.drop`
# MAGIC * Here are the functions related to wide transformations.
# MAGIC   * `df.distinct`
# MAGIC   * `df.union` or any set operation
# MAGIC   * `df.join` or any join operation
# MAGIC   * `df.groupBy`
# MAGIC   * `df.sort` or `df.orderBy`
# MAGIC * Any function that result in shuffling is wide transformation. For all the wide transformations, we have to deal with group of records based on a key.

# COMMAND ----------

help(user_df.select)

# COMMAND ----------

user_df.show(truncate=False)

# COMMAND ----------

user_df.select('id','first_name','last_name').show()

# COMMAND ----------

user_df.select(['id','first_name','last_name']).show()

# COMMAND ----------

#defining alias to df
user_df.alias('u').select('u.*','u.id').show()

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

user_df.select(col('id'),'first_name','last_name',
               concat(col('first_name'),lit(','),col('last_name')).alias('full_name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###selectExpr

# COMMAND ----------

help(user_df.selectExpr)

# COMMAND ----------

#we don't need to import concat or lit, it's using underlying sql methods
user_df.selectExpr('id','first_name','last_name',
               "concat(first_name, ',', last_name) as full_name").show()

# COMMAND ----------

user_df.createOrReplaceTempView('users')

# COMMAND ----------



# COMMAND ----------

spark.sql("""select id , first_name, last_name, concat(first_name,',',last_name) as full_name from users""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###* referring columns using spark data frame names
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

col('id')

# COMMAND ----------

user_df['id']

# COMMAND ----------

user_df.select( col('id'), user_df['id'], 'id' ).show()

# COMMAND ----------

#should not work b/c u is not object in this session
user_df.alias('u'
).select(u['id'],'id',col('id'))

# COMMAND ----------

user_df.alias('u'
).select('u.id','id',col('id')) #should work

# COMMAND ----------

#should not work b/c selectexpr only take column names or sql style expressions on column names
user_df.selectExpr(col('id'), 'first_name')

# COMMAND ----------

user_df.selectExpr('id', 'first_name') #should work

# COMMAND ----------

user_df.alias('u').selectExpr('u.id')

# COMMAND ----------

user_df.createOrReplaceTempView('test')

# COMMAND ----------

spark.sql("""
          select id, first_name, last_name,
          concat(u.first_name,',',u.last_name) as full_name
          from test as u
          """).show()

# COMMAND ----------

cols = ['id','first_name','last_name']
user_df.select(*cols)#

# COMMAND ----------

help(col)

# COMMAND ----------

user_id = col('id')
user_df.select(user_id)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are quite a few functions available on top of column type
# MAGIC * `cast` (can be used on all important data frame functions such as `select`, `filter`, `groupBy`, `orderBy`, etc)
# MAGIC * `asc`, `desc` (typically used as part of `sort` or `orderBy`)
# MAGIC * `contains` (typically used as part of `filter` or `where`)

# COMMAND ----------


user_df.select('id','customer_from').dtypes  #2021-01-15


# COMMAND ----------

#user_df.select(col('id'),'customer_from',    date_format('customer_from','yyyyMMdd')).show()

# COMMAND ----------


#typecast to date_format which result of type string
from pyspark.sql.functions import date_format

user_df.select(
    col('id'),
    date_format('customer_from','yyyyMMdd')

).dtypes



# COMMAND ----------


cols = [ col('id'),    date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')]
user_df.select( *cols).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Invoking functions using spark column objects

# COMMAND ----------


user_df.show(truncate=False)


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC * Concat 'first_nae and last name to genrate full name
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

full_name = concat(col('first_name'),lit(', '),col('last_name'))
customer_from = date_format(col('customer_from'),'yyyymmdd').cast('int')  #to cast in int we need to convert standard data formate to yyyymmdd format

full_name_alias = full_name.alias('full_name')
customer_from_alias = customer_from.alias('customer_from')

# COMMAND ----------


user_df.select('id', full_name_alias, customer_from_alias).show()


# COMMAND ----------


