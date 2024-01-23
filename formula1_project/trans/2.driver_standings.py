# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

race_results_df = spark.read.parquet(f"{GOLD_LAYER_PATH}/processed/race_results")
display(race_results_df)

# COMMAND ----------

driver_standings_df = race_results_df \
                        .groupBy("race_year", "driver_name", "driver_nationality", "team") \
                        .agg(sum("points").alias("total_points"),
                             count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window

driver_standings_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank", rank().over(driver_standings_spec))
display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{GOLD_LAYER_PATH}/processed/driver_standings")
