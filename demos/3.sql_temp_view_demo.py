# Databricks notebook source
# MAGIC %run "/Repos/info@datasealsoftware.com/Azure-databricks-demo/formula1_project/Includes/configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{GOLD_LAYER_PATH}/processed/race_results")

# COMMAND ----------

races_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

p_race_year = 2020
race_results_2020_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")
display(race_results_2020_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES;
