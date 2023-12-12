# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 11:44:59 2023

@author: sujee
"""
#%%

from pyspark.sql.session import SparkSession

def get_spark_session():
    spark = (SparkSession.builder.appName('test_app')
             .master('local').getOrCreate())

    return spark





#print(get_spark_session())


spark = get_spark_session()

#%%

print(spark)
line = "Return a list of the lines in the string, breaking at line boundaries list"


r = spark.sparkContext.parallelize(line,2)


print(r.take(5))


#how many time list word is in line


#count of each word



#count of 'b' char in string
