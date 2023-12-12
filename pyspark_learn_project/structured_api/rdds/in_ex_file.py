# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 17:44:17 2023

@author: sujee
"""

"""
#first part:
spark:
    rdd
    structtured api
    theory of spark


#second part:
sql:


#third part:
project explaination:


"""

#%%

from pyspark.sql.session import SparkSession


def get_spark_session():
     spark = (SparkSession.builder.appName("spark_demo_project")
         .master("local[2]").getOrCreate()   )

     return spark


#%%

from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, IntegerType, StringType ,Row

spark=get_spark_session()

myschema = StructType([StructField("name", StringType(), False), StructField("Age", StringType(), False)])
names = [Row("sujeet", "30"), Row("Ankita", "24")]
spark.createDataFrame(names, myschema).show()
spark.createDataFrame(names, ["name", "age"]).show()
spark.sparkContext.parallelize(names).toDF(["name", "age"]).show()








#%%
