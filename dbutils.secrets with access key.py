# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------


display(dbutils.secrets.listScopes())
dbutils.secrets.list(scope='formulad111')


# COMMAND ----------

value=dbutils.secrets.get(scope='formulad111',key='accesskey')


# COMMAND ----------

# DBTITLE 1,ils.sec
spark.conf.set("fs.azure.account.key.formulad111.dfs.core.windows.net",value)
display(spark.read.csv('abfs://demo@formulad111.dfs.core.windows.net/circuits.csv'))
