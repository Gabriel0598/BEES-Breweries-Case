# Databricks notebook source
df_brews = spark.read.table("db_breweries.tbl_silver_global_breweries")
df_brews.printSchema()

# COMMAND ----------

df_vw_brew = (df_brews
              .groupBy("brewery_type", "country", "state")
                .count()
        ).withColumnRenamed("count", "qtn_breweries")

df_vw_brew.printSchema()

# COMMAND ----------

df_vw_brew.show(10, False)

# COMMAND ----------

df_vw_brew.select("qtn_breweries").distinct().count()

# COMMAND ----------

df_vw_brew.select("brewery_type", "qtn_breweries").distinct().orderBy(df_vw_brew.qtn_breweries.asc()).show()

# COMMAND ----------

df_final_view = df_vw_brew.select("brewery_type", "country", "state", "qtn_breweries").distinct().orderBy(df_vw_brew.brewery_type.asc())
df_final_view.count()

# COMMAND ----------

df_final_view.createOrReplaceTempView("vw_gold_breweries_per_type")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_gold_breweries_per_type;
