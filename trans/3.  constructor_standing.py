# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f'{presentation_folder_path}/race_resuts')

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc

# COMMAND ----------

constructors_standing_df = race_result_df \
    .groupby('race_year', 'team') \
    .agg(
        sum("points").alias('total_points'),
        count(when(col('position') == 1, True)).alias('wins')) 

# COMMAND ----------

display(constructors_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructors_rank_spec = Window.partitionBy('race_year') \
    .orderBy(desc('total_points'), desc('wins'))

final_df = constructors_standing_df.withColumn('rank',rank().over(constructors_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/constructors_standings')

# COMMAND ----------


