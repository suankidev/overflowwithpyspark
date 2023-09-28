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

user_df.show()

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


