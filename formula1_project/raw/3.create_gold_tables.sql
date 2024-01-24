-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_gold
LOCATION "abfss://gold@<storage_account_name>.dfs.core.windows.net/presentation"

-- COMMAND ----------

SHOW TABLES IN f1_gold;
