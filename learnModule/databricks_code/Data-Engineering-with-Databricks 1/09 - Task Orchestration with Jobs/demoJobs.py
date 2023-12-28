# Databricks notebook source
dbutils.widgets.text("source","dev",label="envType")

# COMMAND ----------

source = dbutils.widgets.get("source")
print(f"sujeet Kumar : {source}")
