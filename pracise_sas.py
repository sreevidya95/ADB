# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formulad111.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formulad111.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formulad111.dfs.core.windows.net","sp=rl&st=2024-10-22T09:38:58Z&se=2024-10-22T17:38:58Z&spr=https&sv=2022-11-02&sr=c&sig=i7JAnkv5io47pHn7Q6KY35xb8tGcc9ThkY1XYy93wmo%3D")

# COMMAND ----------

display(spark.read.csv('abfs://demo@formulad111.dfc.core.windows.net/circuits.csv'))
