# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

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
    .json(f'{raw_folder_path}/constructors.json') 
    

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

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn('data_source', lit(v_data_source))

    

# COMMAND ----------

constructor_ingestion_date_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4: Write the dataframe to processed container

# COMMAND ----------

constructor_ingestion_date_df.write.mode('overwrite').parquet(f'{processed_folder_path}/constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')
