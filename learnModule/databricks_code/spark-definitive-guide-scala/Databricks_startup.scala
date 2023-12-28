// Databricks notebook source
val t = (1 to 10).toSeq.map(_.toString)


t foreach println



// COMMAND ----------

dbutils.widgets.text("Text", "Hello World!")
dbutils.widgets.dropdown("Dropdown", "1", (1 to 10).toSeq.map(_.toString))
dbutils.widgets.combobox("combobox", "A", List("A","B","C") )
dbutils.widgets.multiselect("Multiselect", "Yes", List("Yes","No","Maybe"))

// COMMAND ----------


