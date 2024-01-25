-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report On Dominant Formula 1 Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE VIEW f1_gold.vw_dominant_teams AS
SELECT team_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points,
rank() OVER (order by avg(calculated_points) DESC) team_rank
FROM f1_gold.calculated_result
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC;


-- COMMAND ----------

SELECT race_year
,team_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
WHERE team_name IN (SELECT team_name FROM f1_gold.vw_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year,team_name
ORDER BY race_year,avg_points DESC;


-- COMMAND ----------

SELECT race_year
,team_name, 
count(1) total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
FROM f1_gold.calculated_result
WHERE team_name IN (SELECT team_name FROM f1_gold.vw_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year,team_name
ORDER BY race_year,avg_points DESC;

