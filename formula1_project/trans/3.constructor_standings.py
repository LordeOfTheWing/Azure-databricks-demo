# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

constructors_df = spark.read.parquet(f"{GOLD_LAYER_PATH}/race_result")
display(constructors_df)

# COMMAND ----------

constructors_standings_df = constructors_df\
                            .groupBy("race_year","team")\
                            .agg(count(when(col("position") == 1, True)).alias("wins")
                                ,sum("points").alias("total_points"))\
                            .orderBy("total_points", "wins", ascending=False)

# COMMAND ----------

display(constructors_standings_df)

# COMMAND ----------

constructors_standings_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = constructors_standings_df.withColumn("rank", rank().over(constructors_standings_spec))
display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.constructor_standing")
