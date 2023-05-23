# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1: Ingest qualifying multiline multiple json file

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
    .json(f'{raw_folder_path}/qualifying', multiLine=True) 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id')) \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/qualifying')

# COMMAND ----------

dbutils.notebook.exit('Success')
