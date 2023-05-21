# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1 : Read the csv file using spark dataframe api

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(
    fields =[
        StructField('raceId', IntegerType(), False),
        StructField('year', IntegerType(), True),
        StructField('round', IntegerType(), True),
        StructField('circuitId', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('date', DateType(), True),
        StructField('time', StringType(), True),
        StructField('url', StringType(), True)
    ] 
)

# COMMAND ----------

races_df = spark.read.csv('/mnt/formula1dldataset/raw/races.csv', schema=races_schema, header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Add ingestion dte and race_timestamp to datafram api

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

# COMMAND ----------

races_with_time_stamp = races_df.withColumn('ingestion_date', current_timestamp())\
    .withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss') )

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3: Selecting only column we need

# COMMAND ----------

races_selected_df = races_with_time_stamp.select(
    col('raceId').alias('race_id'),
    col('year').alias('race_year'),
    col('round'),
    col('circuitId').alias('circuit_id'),
    col('name'),
    col('ingestion_date'),
    col('race_timestamp')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

races_selected_df.write.mode('overwrite') \
    .partitionBy('race_year') \
    .parquet('/mnt/formula1dldataset/processed/races')

# COMMAND ----------


