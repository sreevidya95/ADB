# Databricks notebook source
# MAGIC %run "/Workspace/Repos/nlkasyap09@yahoo.com/ADB/run_variables_and_widgets"

# COMMAND ----------

race_df = spark.read.parquet(f"{processes_folder_path}/race_parquet")
race_final = race_df.withColumnRenamed("year","race_year").withColumnRenamed("name","race_name").drop("url").withColumnRenamed("race_timestamp","race_date")
circuit_df=spark.read.parquet(f"{processes_folder_path}/circuits_parquet")
circuit_final = circuit_df.withColumnRenamed("location","circuit_location")
driver_df = spark.read.parquet(f"{processes_folder_path}/driver")


