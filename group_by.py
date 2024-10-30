# Databricks notebook source
race_df = spark.read.parquet("/mnt/formulad111/processed/race_parquet")
display(race_df)

# COMMAND ----------

race_df.groupBy('year').min('round').show()

# COMMAND ----------

from pyspark.sql.functions import min,sum,avg
race_df.groupBy('year').agg(sum('round'),min('round'),avg('round')).show()
