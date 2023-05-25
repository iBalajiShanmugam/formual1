# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Aggregate function demo

# COMMAND ----------

# MAGIC %md
# MAGIC ####Build-in Aggregate functions

# COMMAND ----------

race_result_df = spark.read.parquet(f'{presentation_folder_path}/race_resuts')

# COMMAND ----------

demo_df = race_result_df.filter('race_year = 2020')

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

demo_df.select(count('race_name')).show()

# COMMAND ----------

demo_df.select(countDistinct('race_name')).show()

# COMMAND ----------

demo_df.select(sum('points')).show()

# COMMAND ----------

demo_df.groupBy('driver_name').sum('points').show()

# COMMAND ----------

from pyspark.sql.functions import col, desc

# COMMAND ----------

demo_df.groupBy('driver_name') \
    .agg(sum('points'), count('race_name')) \
    .orderBy(col('sum(points)').desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Window Function

# COMMAND ----------

demo_df = race_result_df.filter('race_year in (2019,2020)')

# COMMAND ----------

display(demo_df.groupBy('driver_name', 'race_year') \
    .agg(sum('points'), count('race_name')) \
    .orderBy(col('race_year'),col('sum(points)').desc()))

# COMMAND ----------


