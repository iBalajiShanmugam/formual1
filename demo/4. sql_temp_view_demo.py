# Databricks notebook source
# MAGIC %md
# MAGIC #### Access dataframes using SQL
# MAGIC #####Objectives
# MAGIC 1. Creating temporary views on dataframe
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f'{presentation_folder_path}/race_resuts')

# COMMAND ----------

race_result_df.createTempView('v_race_result')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from v_race_result;

# COMMAND ----------

display(spark.sql("select * from v_race_result"))

# COMMAND ----------


