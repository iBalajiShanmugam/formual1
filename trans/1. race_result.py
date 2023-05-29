# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

driver_df = spark.read.parquet(f'{processed_folder_path}/drivers') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races') \
    .withColumnRenamed('name', 'race_name') \
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors') \
    .withColumnRenamed('name', 'team')

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

resuts_df = spark.read.parquet(f'{processed_folder_path}/results') \
    .withColumnRenamed('time', 'race_time')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, 'inner') \
.select(
    races_df.race_id,
    races_df.race_name,
    races_df.race_year,
    races_df.race_date,
    circuits_df.circuit_location
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join results to all other dataframes

# COMMAND ----------

race_result_df = resuts_df.join(race_circuits_df, resuts_df.race_id == race_circuits_df.race_id) \
    .join(driver_df, resuts_df.driver_id == driver_df.driver_id) \
    .join(constructors_df, resuts_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select(
    'race_year',
    'race_name',
    'race_date',
    'circuit_location',
    'driver_name',
    'driver_nationality',
    'team',
    'grid',
    'fastest_lap',
    'race_time',
    'points',
    'position'
).withColumn('created_date', current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')
