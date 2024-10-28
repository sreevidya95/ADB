# Databricks notebook source
# MAGIC %md
# MAGIC #Creating connection between data lake and storage account using access keys
# MAGIC ##steps:
# MAGIC 1.set configuration using spark.config.set()
# MAGIC 2.list files in the storage account using dbutils.fs.ls()

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulad111.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formulad111.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formulad111.dfs.core.windows.net","sp=rl&st=2024-10-21T17:48:44Z&se=2024-10-23T01:48:44Z&spr=https&sv=2022-11-02&sr=c&sig=G76GGs7HaYoyGYfEAlwgc6PD6CqW8QsrIPVtfzNcKx4%3D")
display(dbutils.fs.ls("abfss://demo@formulad111.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulad111.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


