# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1: Ingest Constructors json file

# COMMAND ----------

constructors_schema = """
    constructorId INT,
    constructorRef STRING,
    name STRING,
    nationality STRING,
    url STRING
"""

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json('/mnt/formula1dldataset/raw/constructors.json') 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Drop unwanted columns from dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#constructor_drop_df = constructor_df.drop('url')
#constructor_drop_df = constructor_df.drop(constructor_df.url)
#constructor_drop_df = constructor_df.drop(constructor_df['url'])
constructor_drop_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref').withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet('/mnt/formula1dldataset/processed/constructors')

# COMMAND ----------


