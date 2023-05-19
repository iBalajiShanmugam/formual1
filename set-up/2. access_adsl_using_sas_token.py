# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circutes.csv

# COMMAND ----------

formula1data_sas_token = dbutils.secrets.get('formula1-scope','formula1data-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dldataset.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dldataset.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dldataset.dfs.core.windows.net", formula1data_sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dldataset.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dldataset.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dldataset.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


