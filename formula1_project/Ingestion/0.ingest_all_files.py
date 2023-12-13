# Databricks notebook source
v_results = dbutils.notebook.run("2.Ingest_circuits_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("3.Ingest_races_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("4.Ingest_constructors_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("5.Ingest_drivers_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("6.Ingest_results_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("7.Ingest_pit_stops_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("8.Ingest_lap_times_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("9.Ingest_qualifying_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_results
