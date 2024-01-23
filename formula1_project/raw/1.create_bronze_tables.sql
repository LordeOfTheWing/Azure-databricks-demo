-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create Tables For CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Circuits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.circuit;
CREATE TABLE IF NOT EXISTS f1_bronze.circuit
(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (
  path "abfss://bronze@<storage_account_name>.dfs.core.windows.net/formula1_raw/circuits.csv",
  header true
);

-- COMMAND ----------

SELECT * FROM f1_bronze.circuit;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Races Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.race;
CREATE TABLE IF NOT EXISTS f1_bronze.race
(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS(path "abfss://bronze@<storage_account_name>.dfs.core.windows.net/formula1_raw/races.csv", 
header true);

-- COMMAND ----------

SELECT * FROM f1_bronze.race;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Create Tables For JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Constructors Table
-- MAGIC   - Single Line JSON
-- MAGIC   - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.constructor;
CREATE TABLE IF NOT EXISTS f1_bronze.constructor
(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "abfss://bronze@<storage_account_name>.dfs.core.windows.net/formula1_raw/constructors.json");

-- COMMAND ----------

SELECT * FROM f1_bronze.constructor;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ##### Create Drivers Table
-- MAGIC   - Single Line JSON
-- MAGIC   - Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.driver;
CREATE TABLE IF NOT EXISTS f1_bronze.driver
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)USING json
OPTIONS(path "abfss://bronze@<storage_account_name>.dfs.core.windows.net/formula1_raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_bronze.driver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ##### Create Results Table
-- MAGIC   - Single Line JSON
-- MAGIC   - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS  f1_bronze.result;
CREATE TABLE IF NOT EXISTS f1_bronze.result
(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)USING json
OPTIONS(path "abfss://bronze@<storage_account_name>.dfs.core.windows.net/formula1_raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_bronze.result;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ##### Create Pit stops Table
-- MAGIC   - Multi Line JSON
-- MAGIC   - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.pitstop;
CREATE TABLE IF NOT EXISTS f1_bronze.pitstop
(
  raceId INT,
  driverId INT,
  stop STRING,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)USING json
OPTIONS(path "abfss://bronze@<storage_account_name>.dfs.core.windows.net/formula1_raw/pit_stops.json", multiline true)

-- COMMAND ----------

SELECT * FROM f1_bronze.pitstop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Tables For List Of Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1.Create Lap Times Table
-- MAGIC   - CSV file
-- MAGIC   - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.lap_time;
CREATE TABLE IF NOT EXISTS f1_bronze.lap_time
(
  raceId INT, 
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)USING csv
OPTIONS(path "abfss://bronze@<storage_account_name>.dfs.core.windows.net/formula1_raw/lap_times/*.csv")

-- COMMAND ----------

SELECT * FROM f1_bronze.lap_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2.Create Qualifying Table
-- MAGIC   - JSON file
-- MAGIC   - MultiLine JSON
-- MAGIC   - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.qualifying;
CREATE TABLE IF NOT EXISTS f1_bronze.qualifying
(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)USING json
OPTIONS(path "abfss://bronze@<storage_account_name>.dfs.core.windows.net/formula1_raw/qualifying/*.json", multiline true)

-- COMMAND ----------

SELECT * FROM f1_bronze.qualifying;

-- COMMAND ----------

SHOW TABLES IN f1_bronze;
