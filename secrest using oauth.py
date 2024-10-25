# Databricks notebook source
tenant_id = dbutils.secrets.get(scope='formulad111',key='endpoint')
client_id = dbutils.secrets.get(scope='formulad111',key='clientid')
client_secret=dbutils.secrets.get(scope='formulad111',key='clientsecret')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formulad111.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formulad111.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formulad111.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formulad111.dfs.core.windows.net",client_secret )
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formulad111.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(spark.read.csv('abfs://demo@formulad111.dfs.core.windows.net/circuits.csv'))
