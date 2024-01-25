-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report On Dominant Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE VIEW f1_gold.vw_dominant_drivers AS
SELECT driver_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points,
rank() OVER (order by avg(calculated_points) DESC) driver_rank
FROM f1_gold.calculated_result
GROUP BY driver_name
HAVING total_races > 50
ORDER BY avg_points DESC;


-- COMMAND ----------

SELECT race_year
,driver_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year,driver_name
ORDER BY race_year,avg_points DESC;


-- COMMAND ----------

SELECT race_year
,driver_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year,driver_name
ORDER BY race_year,avg_points DESC;


-- COMMAND ----------

SELECT race_year
,driver_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year,driver_name
ORDER BY race_year,avg_points DESC;

