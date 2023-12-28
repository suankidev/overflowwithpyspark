# Databricks notebook source
application_id='fb0d03b0-ca5b-4f4b-a063-5beed296f41e'
directory_id='7346c6b6-3d02-4206-99d1-290f5cd33d27'
secret_key='6SQ8Q~t_yzxvHBLj3t-Jo0GHxh1uGCHiJ99TFcKt'


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": f"{secret_key}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}



# COMMAND ----------

flag=False
exists=False
try:
  dbutils.fs.ls('/mnt/suankistoragedatalakes/raw')
  exists=True
except Exception as e:
  flag=False
else:
  flag=True

if (flag == False & exists == False):
  # Optionally, you can add <directory-name> to the source URI of your mount point.
  try:
    dbutils.fs.mount(
    source = "abfss://raw@suankistoragedatalakes.dfs.core.windows.net/",
    mount_point = "/mnt/suankistoragedatalakes/raw",extra_configs = configs)
  except Exception as e:
    print(e)

# COMMAND ----------

 dbutils.fs.ls('/mnt/suankistoragedatalakes/raw')

# COMMAND ----------

flag=False
exists=False
try:
  dbutils.fs.ls('/mnt/suankistoragedatalakes/processed')
  exists=True
except Exception as e:
  flag=False
else:
  flag=True

if (flag == False & exists == False):
  # Optionally, you can add <directory-name> to the source URI of your mount point.
  try:
    dbutils.fs.mount(
    source = "abfss://processed@suankistoragedatalakes.dfs.core.windows.net/",
    mount_point = "/mnt/suankistoragedatalakes/processed",extra_configs = configs)
  except Exception as e:
    print(e)

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/suankistoragedatalakes/')

# COMMAND ----------

survey_path="dbfs:/mnt/suankistoragedatalakes/raw/stockOverflowSurvey2019"
raw_path="/mnt/suankistoragedatalakes/raw"
processed_path="/mnt/suankistoragedatalakes/processed"

# COMMAND ----------

dbutils.fs.ls("/mnt")
