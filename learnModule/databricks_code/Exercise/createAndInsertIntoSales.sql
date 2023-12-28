-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC print("creating sales table and insert records")

-- COMMAND ----------

create database hive_metastore.skuma;
use hive_metastore.skuma;

-- COMMAND ----------

create or replace table sales(
    customer_id string,
    spend double,
    units int,
    ordertime timestamp generated always as (current_timestamp())
);
insert into sales values
('a1', 28.9, 7),
('a3', 974.12, 23),
('a4', 8.99, 1)

-- COMMAND ----------

create or replace table favourite_stores(
    customer_id string,
    store_id string
);

insert into favourite_stores values
('a1','s1'),
('a2', 's2'),
('a4', 's2')

-- COMMAND ----------


