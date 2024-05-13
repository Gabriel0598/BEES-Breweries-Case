# Databricks notebook source
# MAGIC %md
# MAGIC ### Configure connection between Databricks and Storage Account

# COMMAND ----------

service_credential = dbutils.fs.head("dbfs:/FileStore/tables/secret_app_managed.conf")

spark.conf.set("fs.azure.account.auth.type.breweriesdatalakestorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.breweriesdatalakestorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.breweriesdatalakestorage.dfs.core.windows.net", "3bf42ddc-1b7c-43f5-ba24-8b6740142c3d")
spark.conf.set("fs.azure.account.oauth2.client.secret.breweriesdatalakestorage.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.breweriesdatalakestorage.dfs.core.windows.net", "https://login.microsoftonline.com/a01c6abf-622f-418f-adfc-dc982c4686da/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Mining from json files

# COMMAND ----------

df = (spark.read.format("json")
        .load("abfss://synapse-user@breweriesdatalakestorage.dfs.core.windows.net/raw_data_breweries/json/*.json")
        )

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(5, False)

# COMMAND ----------

df.count()

# COMMAND ----------

df.saveAsTable()
