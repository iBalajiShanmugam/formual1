# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest Drivers Nested JSON file

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1: Read the JSON file using spark dataframe reader api

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,DataType,StructField,StructType

# COMMAND ----------

name_schema = StructType(
    fields = [
        StructField('forename', StringType(), True),
        StructField('surname', StringType(), True)
    ]
)

# COMMAND ----------

driver_schema = StructType(
    fields= [
        StructField('driverId', IntegerType(), False),
        StructField('driverRef', StringType(), True),
        StructField('number', StringType(), True),
        StructField('code', StringType(), True),
        StructField('name', name_schema),
        StructField('nationality', StringType(), True),
        StructField('url', StringType(), True)
    ]
)

# COMMAND ----------

drivers_df = spark.read \
    .json(f'{raw_folder_path}/drivers.json', schema =driver_schema )

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename columns and add new columns
# MAGIC   1. driverid renamed to driver_id
# MAGIC   2. driverRef renamed to driver_ref
# MAGIC   3. ingestion date added
# MAGIC   4. name added with concatenation for forename and surname

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('driverRef','driver_ref') \
    .withColumn('name', concat(drivers_df.name.forename,lit(' '), drivers_df.name.surname)) \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

drivers_ingestion_df = add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3: Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_ingestion_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the output to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')
