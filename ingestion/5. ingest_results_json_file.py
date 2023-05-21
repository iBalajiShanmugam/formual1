# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1: Ingest Results json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(
    fields=[
        StructField('resultId', IntegerType(), False),
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('constructorId', IntegerType(), False),
        StructField('number', StringType(), True),
        StructField('grid', IntegerType(), False),
        StructField('position', IntegerType(), True),
        StructField('positionText', StringType(), False),
        StructField('positionOrder', IntegerType(), False),
        StructField('points',FloatType(), False),
        StructField('laps', IntegerType(), False),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True),
        StructField('fastestLap', IntegerType(), True),
        StructField('rank', IntegerType(), True),
        StructField('fastestLapTime', StringType(), True),
        StructField('fastestLapSpeed', StringType(), True),
        StructField('statusId', IntegerType(), False)
    ]
)

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json('/mnt/formula1dldataset/raw/results.json') 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Drop unwanted columns from dataframe

# COMMAND ----------

results_drop_df = results_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_final_df = results_drop_df.withColumnRenamed('resultId', 'result_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('positionText', 'postion_text') \
    .withColumnRenamed('positionOrder', 'position_order') \
    .withColumnRenamed('fastestLap', 'fastest_lap') \
    .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/formula1dldataset/processed/results')

# COMMAND ----------


