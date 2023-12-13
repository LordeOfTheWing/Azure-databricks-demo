# Databricks notebook source
BRONZE_LAYER_PATH = "abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net/formula1_raw"
SILVER_LAYER_PATH = "abfss://silver@saprojectmaestrosnd.dfs.core.windows.net/processed"
GOLD_LAYER_PATH = "abfss://gold@saprojectmaestrosnd.dfs.core.windows.net/presentation"
ACCESS_KEY = dbutils.secrets.get('Azure Key Vault', 'adlsgen2key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net"
    ,ACCESS_KEY
)
