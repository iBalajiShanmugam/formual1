# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake for the project

# COMMAND ----------

def mount_adls(storage_account, container_name):
    #Get the secrets from key valut
    client_id = dbutils.secrets.get('formula1-scope','formula1data-client-id')
    tenant_id = dbutils.secrets.get('formula1-scope','formula1data-tenant-id')
    client_secret =dbutils.secrets.get('formula1-scope','formula1data-client-secret')

    #set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    #if anython mount already unmount the end point 
    if any(mount.mountPoint == f"/mnt/{storage_account}/{container_name}" for mount in dbutils.fs.mounts()):
	    dbutils.fs.unmount(f"/mnt/{storage_account}/{container_name}")
 
    #Mount storage account and container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls('formula1dldataset','raw')

# COMMAND ----------

mount_adls('formula1dldataset','presentation')

# COMMAND ----------

mount_adls('formula1dldataset','processed')
