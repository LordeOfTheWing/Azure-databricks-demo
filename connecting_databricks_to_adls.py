# Databricks notebook source
# MAGIC %md 
# MAGIC #### Connecting Databricks to Azure Data Lake Gen 2 Service using Access keys
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Declare the access key in a variable using Azure key vault as the secret scope
ACCESS_KEY= dbutils.secrets.get('Azure Key Vault','adlskey')
ADLS_PATH = "abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net/"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net",
    ACCESS_KEY)

# COMMAND ----------

# Connect to data lake using the abfs (Azure Blob FIle System) driver
display(dbutils.fs.ls(ADLS_PATH))

# COMMAND ----------

display(spark.read.csv(f"{ADLS_PATH}/source_data.csv"))

# COMMAND ----------


