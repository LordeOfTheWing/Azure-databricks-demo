-- Databricks notebook source
SELECT driver_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
GROUP BY driver_name
HAVING total_races > 50
ORDER BY avg_points DESC;


-- COMMAND ----------

SELECT driver_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING total_races > 50
ORDER BY avg_points DESC;


-- COMMAND ----------

SELECT driver_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING total_races > 50
ORDER BY avg_points DESC;

