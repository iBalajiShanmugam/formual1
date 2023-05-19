# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circutes.csv

# COMMAND ----------

# fs.azure.account.key.formula1dldataset.dfs.core.windows.net {{secrets/formula1-scope/formula1data-account-key}}

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dldataset.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dldataset.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dldataset.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


