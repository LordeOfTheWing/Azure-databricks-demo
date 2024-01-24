-- Databricks notebook source
-- MAGIC %run "/Repos/info@datasealsoftware.com/Azure-databricks-demo/formula1_project/Includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DEMO;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED DEMO;

-- COMMAND ----------

SHOW TABLES IN DEMO;

-- COMMAND ----------

SELECT current_schema()

-- COMMAND ----------

USE DEMO;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{GOLD_LAYER_PATH}/processed/race_results")
-- MAGIC display(race_results_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ######  Managed Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").saveAsTable("DEMO.race_results")
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED DEMO.race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### External Tables
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.option("path", f"{GOLD_LAYER_PATH}/processed/race_results_ext").format("parquet").saveAsTable("demo.race_results_ext")

-- COMMAND ----------

desc extended demo.race_results_ext;

-- COMMAND ----------

--Creating an external table using SQL
CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "abfss://<your_container_name>@<your_storage_account_name>.dfs.core.windows.net<your_directory_path>/race_results_ext_sql";

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext WHERE race_year =2020;

-- COMMAND ----------

SELECT * FROM DEMO.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN DEMO;
