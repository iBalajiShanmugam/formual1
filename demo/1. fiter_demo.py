# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

races_filter_df = race_df.filter((race_df["race_year"] == 2019) & (race_df["round"] <= 5))

# COMMAND ----------

races_sql_filter_df = race_df.filter("race_year = 2019 and round <=5")

# COMMAND ----------

display(races_filter_df)

# COMMAND ----------


