# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------



#pandas.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)


import pandas as pd
student_dict = {'Name':['Joe','Nat'], 'Age':[20,21], 'Marks':[85.10, 77.80]}


sd = pd.DataFrame(student_dict)

df =spark.read.option('header',True).csv("dbfs:/FileStore/tables/Automobile_data.csv")

cars = df.toPandas()

#cars = pd.read_csv("/FileStore/tables/Automobile_data.csv")
print(cars)





# COMMAND ----------


import pandas as pd

# Setting maximum rows to be shown 
#pd.options.display.max_rows = 20

# Setting minimum rows to be shown
pd.set_option("display.max_rows", 5)

# Print DataFrame
print(cars)


# COMMAND ----------

sd.info()

# COMMAND ----------

sd.describe()

# COMMAND ----------

sd.loc[[0,1],['Age']]

# COMMAND ----------

sd

# COMMAND ----------

#insert column
sd.insert(loc=2,column='Class', value='A')

# COMMAND ----------

sd.insert(loc=2,column='Timing', value=['9:00-13:00','13:30-16:15'])

sd

# COMMAND ----------



# COMMAND ----------

#drop columns

sd = sd.drop(columns=['Class', 'Timing'])


# COMMAND ----------



# COMMAND ----------

sd

# COMMAND ----------

filter = sd['Marks'] > 80

sd.loc[filter]

# COMMAND ----------

sd['Marks'].where(filter, other=0, inplace=True)
sd

# COMMAND ----------

sd.filter(filter,axis='index')

# COMMAND ----------

sd.filter(like='N', axis='columns')

# COMMAND ----------

sd.rename(columns = {'Timing':'Time'})

# COMMAND ----------

sd

# COMMAND ----------

sd.groupby('Age').mean()

# COMMAND ----------

for index, row in sd.iterrows():
    print(index, row)
    

# COMMAND ----------

sd.sort_values(by=['Marks'])
