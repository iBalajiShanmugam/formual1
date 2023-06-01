# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1: Ingest pitstops json file

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('stop', IntegerType(), False),
        StructField('lap', IntegerType(), True),
        StructField('time', StringType(), True),
        StructField('duration', StringType(), True),
        StructField('milliseconds', IntegerType(), True)
    ]
)

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .json(f'{raw_folder_path}/pit_stops.json', multiLine=True) 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_df.withColumnRenamed('raceId', 'race_id')
    .withColumnRenamed('driverId', 'driver_id')) \
    .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

overwrite_partition(pit_stops_final_df, 'f1_processed','pit_stops', 'race_id')

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

dbutils.notebook.exit('Success')
