# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 3. Set Saprk Config with App/Client id, Directory/ Tenant id & Secret
# MAGIC 3. call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utility related to mount (list all the mounts, unmounts)

# COMMAND ----------

client_id = dbutils.secrets.get('formula1-scope','formula1data-client-id')
tenant_id = dbutils.secrets.get('formula1-scope','formula1data-tenant-id')
client_secret =dbutils.secrets.get('formula1-scope','formula1data-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dldataset.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dldataset/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dldataset/demo")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dldataset/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dldataset/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#unmount the mount point
# dbutils.fs.unmount('/mnt/formula1dldataset/demo')

# COMMAND ----------


