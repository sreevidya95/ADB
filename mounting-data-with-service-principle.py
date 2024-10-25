# Databricks notebook source
tenant_id = dbutils.secrets.get(scope='formulad111',key='tenantid')
client_id = dbutils.secrets.get(scope='formulad111',key='id')
client_secret=dbutils.secrets.get(scope='formulad111',key='secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
    source='abfss://demo@formulad111.dfs.core.windows.net/',
    mount_point='/mnt/demo',
    extra_configs=configs
)

# COMMAND ----------

display(spark.read.csv('/mnt/demo/circuits.csv'))
