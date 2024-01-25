-- Databricks notebook source
use f1_silver

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_gold.calculated_result
USING parquet
AS
SELECT race.race_year,
  constructor.name team_name,
  driver.name driver_name,
  result.position,
  result.points,
  11 - result.position calculated_points
FROM f1_silver.result 
JOIN f1_silver.driver on (result.driver_id = driver.driver_id)
JOIN f1_silver.constructor on (result.constructor_id = constructor.constructor_id)
JOIN f1_silver.race on (result.race_id = race.race_id)
WHERE result.position <= 10;

-- COMMAND ----------

SELECT *
FROM f1_gold.calculated_result;
