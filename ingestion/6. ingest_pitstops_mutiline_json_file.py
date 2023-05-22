# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1: Ingest pitstops json file

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
    .json('/mnt/formula1dldataset/raw/pit_stops.json', multiLine=True) 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet('/mnt/formula1dldataset/processed/pit_stops')
