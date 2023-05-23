# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(
    fields = [
        StructField("circuitsId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True),
        StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

circuits_df = spark.read.csv(
    path = f"{raw_folder_path}/circuits.csv", 
    header=True, 
    schema = circuits_schema)

# COMMAND ----------


# This is another way to call Dataframe reader API
# circuits_df = spark.read.format('csv') \
#     .option('header', True) \
#     .schema(circuits_schema) \
#     .load('/mnt/formula1dldataset/raw/circuits.csv')

# COMMAND ----------

#It will show the stats about the dataframe
#circuits_df.describe().show() 

#It will give top N row default 20 row 
#circuits_df.show()

#It will print the schema 
#circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Select only the required columns

# COMMAND ----------

#selecting the all column using *
#circuits_selected_df = circuits_df.select("*")

# COMMAND ----------

# selecting the column using string coumn name
# circuits_selected_df = circuits_df.select(
#     "circuitsId",
#     "circuitRef",
#     "name",
#     "location",
#     "country",
#     "lat",
#     "lng",
#     "alt",
#     "url",
# )

# COMMAND ----------

# selecting the column using dataframe object
# circuits_selected_df = circuits_df.select(
#     circuits_df.circuitsId,
#     circuits_df.circuitRef,
#     circuits_df.name,
#     circuits_df.location,
#     circuits_df.country,
#     circuits_df.lat,
#     circuits_df.lng,
#     circuits_df.alt,
#     circuits_df.url,
# )

# COMMAND ----------

# selecting the column using dataframe 
# circuits_selected_df = circuits_df.select(
#     circuits_df["circuitsId"],
#     circuits_df["circuitRef"],
#     circuits_df["name"],
#     circuits_df["location"],
#     circuits_df["country"],
#     circuits_df["lat"],
#     circuits_df["lng"],
#     circuits_df["alt"],
#     circuits_df["url"]
# )

# COMMAND ----------



# COMMAND ----------

#selecting the column using col() function
from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(
    col("circuitsId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt"),
    col("url")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitsId", "circute_id") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.parquet(f'{processed_folder_path}/circuits', mode='overwrite')


# COMMAND ----------

dbutils.notebook.exit('Success')
