# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest races.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType
from pyspark.sql.functions import col, current_timestamp, lit, concat, to_timestamp

# COMMAND ----------

ADLS_PATH = "abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net"
ADLS_TARGET = "abfss://silver@saprojectmaestrosnd.dfs.core.windows.net"
ACCESS_KEY = dbutils.secrets.get('Azure Key Vault', 'adlsgen2key')

spark.conf.set(
    "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net",
    ACCESS_KEY
    )


# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# Step1 Read the races CSV file
races_df = spark.read.option("Header", True) \
    .schema(races_schema) \
    .csv(f"{ADLS_PATH}/formula1_raw/races.csv")

display(races_df)


# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))
display(races_selected_df)

# COMMAND ----------

final_races_df = races_selected_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_date", current_timestamp())

display(final_races_df)

# COMMAND ----------

final_races_df.write.mode("overwrite").parquet(f"{ADLS_TARGET}/processed/races")
