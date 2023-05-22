# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1: Ingest laptime folder(multiple csv file)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

laptime_schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('stop', IntegerType(), False),
        StructField('lap', IntegerType(), True),
        StructField('position', IntegerType(), True),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True)
    ]
)

# COMMAND ----------

lap_times_df = spark.read \
    .schema(laptime_schema) \
    .csv('/mnt/formula1dldataset/raw/lap_times') 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

lap_times_final_df.write.mode('overwrite').parquet('/mnt/formula1dldataset/processed/lap_times')

# COMMAND ----------


