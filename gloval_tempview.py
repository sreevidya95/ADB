# Databricks notebook source
# MAGIC %run "../ADB/run_variables_and_widgets"

# COMMAND ----------

race_df = spark.read.parquet(f"{processes_folder_path}/race_parquet")
display(race_df)

# COMMAND ----------

race_df.createOrReplaceGlobalTempView("race_global_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.race_global_view
