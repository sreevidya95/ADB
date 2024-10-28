# Databricks notebook source
# MAGIC %run "../ADB/run_variables_and_widgets"
# MAGIC

# COMMAND ----------

race_df = spark.read.parquet(f"{processes_folder_path}/lap_times")
race_filtered_df = race_df.filter("driver_id = 14 and position = 13")#sql way
display(race_filtered_df)

# COMMAND ----------

race_filtered1_df = race_df.filter((race_df["race_id"] == 67) & (race_df["lap"]==28))
display(race_filtered1_df)
