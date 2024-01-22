# Databricks notebook source
# MAGIC %run "/Repos/info@datasealsoftware.com/Azure-databricks-demo/formula1_project/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Repos/info@datasealsoftware.com/Azure-databricks-demo/formula1_project/Includes/common_functions"

# COMMAND ----------

from pyspark.sql import functions as func

# COMMAND ----------

final_races_df = spark.read.parquet(f"{GOLD_LAYER_PATH}/processed/race_results")

# COMMAND ----------

display(final_races_df)

# COMMAND ----------

races_filt_df = final_races_df.filter("race_year = 2020")
display(races_filt_df)

# COMMAND ----------

races_filt_df.select(func.count("*")).show()

# COMMAND ----------

races_filt_df.select(func.countDistinct("race_name")).show()

# COMMAND ----------

races_filt_df.select("race_name").distinct().show()

# COMMAND ----------

display(races_filt_df.printSchema())

# COMMAND ----------



# COMMAND ----------

races_filt_df.select(func.sum("points")).show()

# COMMAND ----------

display(races_filt_df.filter("driver_name = 'Lewis Hamilton'").select(func.sum("points").alias("total_race_points"), func.countDistinct("race_name").alias("no_of_total_races")))

# COMMAND ----------

result_df = races_filt_df.groupBy("driver_name").sum("points").select("driver_name", func.col("sum(points)").alias("total_points"))
result_df.orderBy("total_points", ascending=False).show()


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

races_filt_df\
    .groupBy("driver_name")\
    .agg(sum("points").alias("total_points"),\
     countDistinct("race_name").alias("total_races"))\
    .show()

# COMMAND ----------


