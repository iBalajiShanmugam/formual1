-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

create database demo;

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database  demo

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Learning Objectives
-- MAGIC 1. Create managed table using python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of droppong a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

show tables from demo

-- COMMAND ----------

describe extended demo.race_results_python

-- COMMAND ----------

select * from demo.race_results_python
where race_year=2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Learning Objectives
-- MAGIC 1. Create External table using python
-- MAGIC 2. Create External table using SQL
-- MAGIC 3. Effect of droppong a External table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format('parquet').option('path', f'{demo_folder_path}/race_result_ext').saveAsTable('demo.race_result_ext')

-- COMMAND ----------

select * from race_result_ext

-- COMMAND ----------

describe extended demo.race_result_ext

-- COMMAND ----------


