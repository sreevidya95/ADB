# Databricks notebook source
df = spark.read.parquet("/mnt/formulad111/processed/race_parquet")
display(df)

# COMMAND ----------

# MAGIC %md Aggregate function count
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count,avg,sum,countDistinct,lit
df.select(count('*')).show()
df.filter("year = 2020").select(countDistinct('*')).show()

# COMMAND ----------

df.select(avg('round')).show()

# COMMAND ----------

df.select(sum('round')).show()
