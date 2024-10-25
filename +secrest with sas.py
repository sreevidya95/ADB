# Databricks notebook source
sas_token=dbutils.secrets.get(scope='formulad111',key='sas')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulad111.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formulad111.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formulad111.dfs.core.windows.net",sas_token)
display(dbutils.fs.ls("abfss://demo@formulad111.dfs.core.windows.net"))
