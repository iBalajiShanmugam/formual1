# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df = spark.read.parquet(f'{processed_folder_path}/races').filter("race_year = 2019")

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits').filter("circuit_id < 70")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(race_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner JOIN

# COMMAND ----------

race_circuits_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "inner") \
    .select(
        circuits_df.name.alias('circuits_name'),
        circuits_df.location,
        circuits_df.country,
        race_df.name.alias('race_name'),
        race_df.round
    )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Left Outer JOIN

# COMMAND ----------

#Left Outer Join
race_circuits_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "left") \
    .select(
        circuits_df.name.alias('circuits_name'),
        circuits_df.location,
        circuits_df.country,
        race_df.name.alias('race_name'),
        race_df.round
    )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#Right Outer Join
race_circuits_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "right") \
    .select(
        circuits_df.name.alias('circuits_name'),
        circuits_df.location,
        circuits_df.country,
        race_df.name.alias('race_name'),
        race_df.round
    )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#full Outer Join
race_circuits_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "full") \
    .select(
        circuits_df.name.alias('circuits_name'),
        circuits_df.location,
        circuits_df.country,
        race_df.name.alias('race_name'),
        race_df.round
    )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Semi Joins

# COMMAND ----------

#semi Join
race_circuits_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "semi") \
    .select(
        circuits_df.name.alias('circuits_name'),
        circuits_df.location,
        circuits_df.country
    )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Anti Joins

# COMMAND ----------

#semi Join
race_circuits_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "anti") \
    .select(
        circuits_df.name.alias('circuits_name'),
        circuits_df.location,
        circuits_df.country
    )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------


