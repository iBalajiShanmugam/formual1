# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc

# COMMAND ----------

driver_standing_df = race_result_df \
    .groupby('race_year', 'driver_name', 'driver_nationality', 'team') \
    .agg(
        sum("points").alias('total_points'),
        count(when(col('position') == 1, True)).alias('wins')) 

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy('race_year') \
    .orderBy(desc('total_points'), desc('wins'))

final_df = driver_standing_df.withColumn('rank',rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/driver_standings')

# COMMAND ----------


