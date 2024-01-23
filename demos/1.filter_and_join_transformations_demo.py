# Databricks notebook source
# MAGIC %run "/Repos/info@datasealsoftware.com/Azure-databricks-demo/formula1_project/Includes/configuration"

# COMMAND ----------

df = spark.read \
    .parquet(f"{SILVER_LAYER_PATH}/races")

# COMMAND ----------

filt_df = df.filter("race_year = 2009  and round > 5")
display(filt_df)

# COMMAND ----------

filt_df = df.filter((df.race_year == 2009) & (df.round > 5))
display(filt_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins in dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Inner Joins

# COMMAND ----------

circuits_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/circuits").withColumnRenamed("name", "circuit_name")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/races").filter("race_year = 2009").withColumnRenamed("name","race_name")
display(races_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Left Outer Join

# COMMAND ----------

circuits_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/circuits").withColumnRenamed("name", "circuit_name").filter("circuit_id <= 50")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/races").filter("race_year = 2019").withColumnRenamed("name","race_name")
display(races_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Right Outer Join

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Full Outer Join

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Join

# COMMAND ----------

circuits_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/circuits").withColumnRenamed("name", "circuit_name").filter("circuit_id <= 50")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/races").filter("race_year = 2019").withColumnRenamed("name","race_name")
display(races_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")
display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi Join

# COMMAND ----------

circuits_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/circuits").withColumnRenamed("name", "circuit_name").filter("circuit_id <= 50")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/races").filter("race_year = 2019").withColumnRenamed("name","race_name")
display(races_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")
display(races_circuits_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")
display(races_circuits_df)
