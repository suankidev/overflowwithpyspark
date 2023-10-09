# Databricks notebook source
from pyspark.sql.types import Row

users_list = [[1, 'Scott'], [2, 'Donald'], [3, 'Mickey'], [4, 'Elvis']]


#converting to row type

print(type(users_list))
print(users_list[1])


# COMMAND ----------

user_df=spark.createDataFrame(users_list,'id int,name string')

# COMMAND ----------

user_df.show()

# COMMAND ----------

#conver list of list to list of row
# print(list(map(lambda x:Row(*x),users_list)))
user_list_row =  [Row(*i) for i in users_list]
print(user_list_row)

# COMMAND ----------

spark.createDataFrame(user_list_row,'id int,user string').show()

# COMMAND ----------

def dummy(*args):
    print(args)


#making tuple whatever we are passing
dummy(1)
dummy([1,2,4])
dummy({'name':"sk","id":1})

# COMMAND ----------

def dummy(**args):
    print(args)


#making tuple whatever we are passing
# dummy(1)
# dummy(**[1,2,4])
dummy(**{'name':"sk","id":1})

# COMMAND ----------

# MAGIC %md
# MAGIC ##Basic of Data types in spark
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

from pyspark.sql.types import Row
user_list = [ Row(**user) for user in users]


user_df = spark.createDataFrame(user_list)


# COMMAND ----------

user_df.columns

# COMMAND ----------

user_df.printSchema()

# COMMAND ----------

user_df.show()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ##Specifying schema as string
# MAGIC

# COMMAND ----------


import datetime
users = [(1,
  'Corrie',
  'Van den Oord',
  'cvandenoord0@etsy.com',
  True,
  1000.55,
  datetime.date(2021, 1, 15),
  datetime.datetime(2021, 2, 10, 1, 15)),
 (2,
  'Nikolaus',
  'Brewitt',
  'nbrewitt1@dailymail.co.uk',
  True,
  900.0,
  datetime.date(2021, 2, 14),
  datetime.datetime(2021, 2, 18, 3, 33)),
 (3,
  'Orelie',
  'Penney',
  'openney2@vistaprint.com',
  True,
  850.55,
  datetime.date(2021, 1, 21),
  datetime.datetime(2021, 3, 15, 15, 16, 55)),
 (4,
  'Ashby',
  'Maddocks',
  'amaddocks3@home.pl',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 10, 17, 45, 30)),
 (5,
  'Kurt',
  'Rome',
  'krome4@shutterfly.com',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 2, 0, 55, 18))]


# COMMAND ----------

# MAGIC %md
# MAGIC ###specifying schem

# COMMAND ----------

users_schema = '''
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    is_customer BOOLEAN,
    amount_paid FLOAT,
    customer_from DATE,
    last_updated_ts TIMESTAMP
'''

#or
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import IntegerType,StringType,BooleanType,FloatType,TimestampType,DateType
users_schema = StructType([
    StructField('id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('is_customer', BooleanType()),
    StructField('amount_paid', FloatType()),
    StructField('customer_from', DateType()),
    StructField('last_updated_ts', TimestampType())
])



# COMMAND ----------



# COMMAND ----------

user_df = spark.createDataFrame(users, schema=users_schema)

# COMMAND ----------

user_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Array type column in spark

# COMMAND ----------

from pyspark.sql.types import Row
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import explode_outer

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": ["+1 234 567 8901", "+1 234 567 8911"],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers": ["+1 234 567 8923", "+1 234 567 8934"],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": ["+1 714 512 9752", "+1 714 512 6601"],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": ["+1 817 934 7142"],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

user_list = [Row(**column) for column in users]

# COMMAND ----------

user_df =  spark.createDataFrame(user_list)

# COMMAND ----------

user_df.columns

# COMMAND ----------

user_df.dtypes

# COMMAND ----------

user_df.select('id','first_name','phone_numbers').show(truncate=False)

# COMMAND ----------

#Ashby /id=4 does not have phone number it won't show in o/p 
user_df.withColumn('phone_number',explode('phone_numbers'))\
    .drop('phone_numbers').show(truncate=False)


# COMMAND ----------



user_df.select('id', col('first_name'),
               col('phone_numbers')[0].alias('home_number'), 
               col('phone_numbers')[1].alias('office_num') 
               ).show(truncate=False)


# COMMAND ----------

user_df.withColumn('phone_number', explode_outer(col('phone_numbers'))
                   ).drop('phone_numbers').show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Map type column
# MAGIC #### We would be able to transform dict type column to spark map type

# COMMAND ----------

import datetime

# COMMAND ----------


users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": {"mobile": "+1 234 567 8901", "home": "+1 234 567 8911"},
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers": {"mobile": "+1 234 567 8923", "home": "+1 234 567 8934"},
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": {"mobile": "+1 714 512 9752", "home": "+1 714 512 6601"},
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": {"mobile": "+1 817 934 7142"},
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

user_list = [Row(**row) for row in users]

# COMMAND ----------



# COMMAND ----------

user_df = spark.createDataFrame(user_list)

# COMMAND ----------

user_df.printSchema()

# COMMAND ----------

user_df.show(truncate=False)

# COMMAND ----------

user_df.dtypes

# COMMAND ----------

user_df.select('id','phone_numbers').show(truncate=False)

# COMMAND ----------

user_df.select('id',col('phone_numbers')['mobile']).show()

# COMMAND ----------

user_df.select('id',explode('phone_numbers')).show(truncate=False)
#results are in form of key and value  , id no 4 is missing here

# COMMAND ----------

user_df.select('id',explode_outer('phone_numbers')).show(truncate=False)

# COMMAND ----------

final_user_df = user_df.select('*',explode('phone_numbers'))\
    .withColumnRenamed('key','ContactType')\
        .withColumnRenamed('value','phone_number')\
            .drop('phone_numbers')

# COMMAND ----------

display(final_user_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType 

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": Row(mobile=None, home=None),
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]




# COMMAND ----------

user_struct = [Row(**row) for row in users]

# COMMAND ----------

user_struct_df = spark.createDataFrame(user_struct)

# COMMAND ----------

user_struct_df.dtypes

# COMMAND ----------

user_struct_df.select('id','phone_numbers').show(truncate=False)

# COMMAND ----------

def dummy(*test):
    print(test)
    print(len(test))

dummy({1})
dummy({1,2})
dummy(1,2,3,4)

# COMMAND ----------

user_df.select('id','phone_numbers.*','phone_numbers.mobile' ).show(truncate=False)

# COMMAND ----------

user_struct_df.select('id','phone_numbers.*','phone_numbers.mobile' ).show(truncate=False)

# COMMAND ----------

user_df.select('id','phone_numbers').show(truncate=False)

# COMMAND ----------

user_df.select('id',col('phone_numbers')['mobile']).show(truncate=False)

# COMMAND ----------

user_struct_df.select('id','phone_numbers.*',col('phone_numbers')['mobile'] ).show(truncate=False)
