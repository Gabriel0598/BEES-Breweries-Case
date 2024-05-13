# Databricks notebook source
# MAGIC %md
# MAGIC #### Read from delta in DBFS

# COMMAND ----------

src_path = "dbfs:/FileStore/tables/bronze/"

df_raw_data = spark.read.format("delta").load(src_path)

# COMMAND ----------

df_raw_data.show(5, False)

# COMMAND ----------

df_raw_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exploring locations available (cities, countries, address, etc)

# COMMAND ----------

df_raw_data.persist()

# COMMAND ----------

df_raw_data.select("country").distinct().show()

# COMMAND ----------

from pyspark.sql.functions import col

df_raw_data.select("state").distinct().orderBy(col("state").asc()).show(50)

# COMMAND ----------

from pyspark.sql.functions import col

df_raw_data.select("state_province").distinct().orderBy(col("state_province").asc()).show(50)

# COMMAND ----------

from pyspark.sql.functions import col

df_raw_data.select("country", "state", "state_province").filter(col("country") == "Ireland").orderBy(col("state").asc()).show(50)

# COMMAND ----------

df_raw_data.select("city").distinct().orderBy(col("city").asc()).show(100)

# COMMAND ----------

df_raw_data.select("postal_code").distinct().orderBy(col("postal_code").asc()).show(50, False)

# COMMAND ----------

df_raw_data.select("address_1").distinct().orderBy(col("address_1").asc()).show(50, False)

# COMMAND ----------

df_dist_add1 = df_raw_data.select("address_1").distinct()
df_dist_add1.count()

# COMMAND ----------

df_dup_add1 = (df_raw_data.select("id", "address_1", "address_2", "address_3")
                .groupBy(["address_1", "address_2", "address_3"])
                    .count()
                        .where('count > 1')
                            .sort('count', ascending=False)
               )

df_dup_add1.show()

# COMMAND ----------

df_raw_data.select("id", "address_1").filter(col("address_1") == "NULL").show(10, False)

# COMMAND ----------

df_raw_data.select("id", "address_1", "address_2", "address_3").orderBy(col("address_1").asc()).show(50, False)

# COMMAND ----------

# Find null

df_null_add = (df_raw_data.select("*")
                .filter(col("id").isin("0faa0fb2-fffa-416d-9eab-46f67477c8ef", "b7b68d22-5045-4501-b9bf-ec94946eaffc", "fe6b9893-b93e-43d5-a9f6-3e0c89a3f13c"))
               )

df_null_add.show(3, False)

# COMMAND ----------

+---------+---------+---------+------------+---------+-------------+------------------------------------+---------+------------+---------------------------------------------+----------+-----------+----------+--------------+------+-----------------------------+
|address_1|address_2|address_3|brewery_type|city     |country      |id                                  |latitude |longitude   |name                                         |phone     |postal_code|state     |state_province|street|website_url                  |
+---------+---------+---------+------------+---------+-------------+------------------------------------+---------+------------+---------------------------------------------+----------+-----------+----------+--------------+------+-----------------------------+
|NULL     |NULL     |NULL     |micro       |Mesa     |United States|0faa0fb2-fffa-416d-9eab-46f67477c8ef|33.436188|-111.5860662|12 West Brewing Company - Production Facility|NULL      |85207      |Arizona   |Arizona       |NULL  |NULL                         |
|NULL     |NULL     |NULL     |micro       |Crosslake|United States|b7b68d22-5045-4501-b9bf-ec94946eaffc|NULL     |NULL        |14 Lakes Brewery                             |2186924129|56442      |Minnesota |Minnesota     |NULL  |NULL                         |
|NULL     |NULL     |NULL     |micro       |Mariposa |United States|fe6b9893-b93e-43d5-a9f6-3e0c89a3f13c|37.570148|-119.9036592|1850 Brewing Company                         |NULL      |95338      |California|California    |NULL  |http://www.1850restaurant.com|
+---------+---------+---------+------------+---------+-------------+------------------------------------+---------+------------+---------------------------------------------+----------+-----------+----------+--------------+------+-----------------------------+

# COMMAND ----------

df_raw_data.select("address_2").distinct().show()

# COMMAND ----------

df_dist_add2 = df_raw_data.select("address_2").distinct()
df_dist_add2.count()

# COMMAND ----------

from pyspark.sql.functions import col

df_raw_data.select("address_1", "address_2").where(col("address_2") == "Clonmore").count()

# COMMAND ----------

df_raw_data.select("address_1", "address_2").where(col("address_2") == "Clonmore").show(1, False)

# COMMAND ----------

df_raw_data.select("address_3").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse brewery type and additional data
# MAGIC

# COMMAND ----------

df_raw_data.printSchema()

# COMMAND ----------

df_raw_data.select("brewery_type").distinct().orderBy(col("brewery_type").asc()).show()

# COMMAND ----------

df_raw_data.select("latitude").distinct().count()

# COMMAND ----------

df_raw_data.select("latitude").distinct().show(5, False)

# COMMAND ----------

df_raw_data.select("longitude").distinct().count()

# COMMAND ----------

df_raw_data.select("longitude").distinct().show(5, False)

# COMMAND ----------

df_raw_data.select("name").distinct().count()

# COMMAND ----------

df_raw_data.select("name").distinct().orderBy(col("name").asc()).show(50, False)

# COMMAND ----------

df_raw_data.select("phone").distinct().count()

# COMMAND ----------

df_raw_data.select("phone").distinct().show(5, False)

# COMMAND ----------

df_raw_data.select("website_url").distinct().count()

# COMMAND ----------

df_raw_data.select("website_url").distinct().show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleansing data
# MAGIC

# COMMAND ----------

df_raw_data.printSchema()

# COMMAND ----------

df_raw_data.select("address_1", "address_2", "address_3", "latitude", "longitude").filter(col("address_2") == "Clonmore").show()

# COMMAND ----------

# Substitution of address_1 according to info provided from google maps:
# Address = Killeshin, Carlow, R93 Y766, Ireland

import requests
maps = requests.get('https://maps.app.goo.gl/LZWMExxJ5ZsknM3bA')
print(maps)

# COMMAND ----------

df_raw_data.select("id", "name", "address_1", "address_2", "address_3", "latitude", "longitude", "country").filter(col("address_2") == "Clonmore").show(1, False)

# COMMAND ----------

from pyspark.sql.functions import when

df_repl_clonmore = (df_raw_data.select("*")
                        .withColumn("address_1_ren", when(df_raw_data.latitude == "52.84930763", "Killeshin, Carlow, R93 Y766")
                                                    .when(df_raw_data.longitude == "-6.979343891", "Killeshin, Carlow, R93 Y766")
                                                    .otherwise(df_raw_data.address_1))
                        .withColumn("name_ren", when(df_raw_data.id == "e5f3e72a-fee2-4813-82cf-f2e53b439ae6", "12 Acres Brewing Company - Clonmore")
                                                .otherwise(df_raw_data.name))
)

df_repl_clonmore.printSchema()

# COMMAND ----------

df_repl_clonmore.select("id", "address_1_ren", "name_ren").filter(col("id") == "e5f3e72a-fee2-4813-82cf-f2e53b439ae6").show(2, False)

# COMMAND ----------

df_repl_clonmore.show(50, False)

# COMMAND ----------

df_repl_clonmore.printSchema()

# COMMAND ----------

import datetime
import pyspark.sql.functions as F

df_clen_brews = (df_repl_clonmore
                    .withColumnRenamed("address_1_ren", "address")
                        .withColumnRenamed("name_ren", "name_establishment")
                            .withColumn("date_ingestion", F.lit(datetime.datetime.now().strftime("%Y-%m-%d")))
                                .select(
                                    col("id")
                                    , col("name_establishment")
                                    , col("brewery_type")
                                    , col("phone")
                                    , col("website_url")
                                    , col("address")
                                    , col("postal_code")
                                    , col("city")
                                    , col("state")
                                    , col("country")
                                    , col("latitude")
                                    , col("longitude")
                                    , col("date_ingestion")
                        ))

df_clen_brews.printSchema()

# COMMAND ----------

df_clen_brews.show(5, False)

# COMMAND ----------

df_clen_brews.filter(col("id") == "e5f3e72a-fee2-4813-82cf-f2e53b439ae6").show(5, False)

# COMMAND ----------

df_clen_brews.select("date_ingestion").show(1)

# COMMAND ----------

df_clen_brews.filter(col("date_ingestion") == "2024-05-13").count()

# COMMAND ----------

# Aggregate type of breweries
from pyspark.sql.functions import count

df = (df_clen_brews
      .groupBy(
          col("id")
          , col("name_establishment")
          , col("brewery_type")
          , col("phone")
          , col("website_url")
          , col("address")
          , col("postal_code")
          , col("city")
          , col("state")
          , col("country")
          , col("latitude")
          , col("longitude")
          , col("date_ingestion")
      ).agg(count("*").alias("count_new_registers"))
      .orderBy(col("brewery_type").asc())
)

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# select final df

final_df = (df
            .select(
                col("id")
                , col("name_establishment")
                , col("brewery_type")
                , col("phone")
                , col("website_url")
                , col("address")
                , col("postal_code")
                , col("city")
                , col("state")
                , col("country")
                , col("latitude")
                , col("longitude")
                , col("date_ingestion")
      )      
)

final_df.printSchema()

# COMMAND ----------

final_df.show(50, False)

# COMMAND ----------

spark.sql("CREATE SCHEMA db_breweries").show()

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").saveAsTable("db_breweries.tbl_silver_global_breweries")

# COMMAND ----------

spark.sql("SELECT COUNT(*) as qtn_brew FROM db_breweries.tbl_silver_global_breweries").show()
