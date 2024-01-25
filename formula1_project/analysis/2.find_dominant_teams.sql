-- Databricks notebook source
SELECT team_name,
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT team_name,
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
WHERE race_year BETWEEN 2011 and 2020
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT team_name,
count(1) total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
FROM f1_gold.calculated_result
WHERE race_year BETWEEN 2001 and 2010
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC;
