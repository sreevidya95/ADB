# Databricks notebook source
def mount_container(container_name,storage_name):
    #getting id,secret and end point using secrets utils
    tenant_id = dbutils.secrets.get(scope='formulad111',key='tenantid')
    client_id = dbutils.secrets.get(scope='formulad111',key='id')
    client_secret=dbutils.secrets.get(scope='formulad111',key='secret')
    #configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    for mount in dbutils.fs.mounts():
          if(mount.mountPoint == f"/mnt/{storage_name}/{container_name}" ):
                dbutils.fs.unmount(mount.mountPoint)
                
    dbutils.fs.mount(
       source=f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
       mount_point = f"/mnt/{storage_name}/{container_name}",
       extra_configs = configs
    )
    display(dbutils.fs.ls('/mnt'))

# COMMAND ----------

mount_container('processed','formulad111')

# COMMAND ----------


