# Databricks notebook source
race_df = spark.read.parquet("/mnt/formulad111/processed/race_parquet")
display(race_df)

# COMMAND ----------

race_df.createOrReplaceTempView("race_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_view
# MAGIC where year = 2009
