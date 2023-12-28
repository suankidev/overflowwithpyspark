# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 12:47:24 2023

@author: sujee
"""
from pyspark.sql.session import SparkSession

def get_spark_session():
    spark = (SparkSession.builder.appName('test_app')
             .master('local').getOrCreate())

    return spark





#print(get_spark_session())


spark = get_spark_session()

#spark.conf.set("spark.sql.warehouse.dir",r"C:\Users\sujee\Desktop\spark_output\warehouse_dir")

sc = spark.sparkContext

print(sc)
l = [1,2,3,5,6]
d = sc.broadcast(l)


print("---->", d.value[2])
print("printting all values ----> %s" % (d))


num = sc.accumulator(1)


t = spark.sparkContext.parallelize(l)

def fu(x):
    global num
    num += x

t.foreach(fu)

print('accumulatro vlaue', num.value)
