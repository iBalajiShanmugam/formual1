# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1: Ingest qualifying multiline multiple json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(
    fields=[
        StructField('qualifyId', IntegerType(), False),
        StructField('raceId', IntegerType(), True),
        StructField('driverId', IntegerType(), True),
        StructField('constructorId', IntegerType(), True),
        StructField('number', IntegerType(), True),
        StructField('position', IntegerType(), True),
        StructField('q1', IntegerType(), True),
        StructField('q2', IntegerType(), True),
        StructField('q3', IntegerType(), True),
    ]
)

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .json('/mnt/formula1dldataset/raw/qualifying', multiLine=True) 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet('/mnt/formula1dldataset/processed/qualifying')

# COMMAND ----------


