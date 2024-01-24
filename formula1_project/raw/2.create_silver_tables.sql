-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_silver
LOCATION "abfss://silver@<storage_account_name>.dfs.core.windows.net/processed"

-- COMMAND ----------

SHOW TABLES IN f1_silver;
