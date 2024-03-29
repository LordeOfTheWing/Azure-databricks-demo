# Databricks notebook source
# MAGIC %md
# MAGIC ##### Transformations

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/race") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")
                            

# COMMAND ----------

circuits_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/circuit") \
    .withColumnRenamed("name", "circuit_name")\
    .withColumnRenamed("location", "circuit_location")
     

# COMMAND ----------

drivers_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/driver")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("nationality", "driver_nationality")
 

# COMMAND ----------

constructors_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/constructor")\
    .withColumnRenamed("name", "team")


# COMMAND ----------

results_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/result")\
    .withColumnRenamed("time","race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id,"inner") \
                    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.race_id == races_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id== constructors_df.constructor_id)

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", 
                                  "team", "grid", "fastest_lap","race_time","points","position")\
                                .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix' ").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.race_result")

# COMMAND ----------


