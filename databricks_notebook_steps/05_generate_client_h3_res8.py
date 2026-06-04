# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5 - Generate H3 resolution 8 for client data
# MAGIC
# MAGIC This reads the client H3 resolution 9 table from step 4 and adds H3
# MAGIC resolution 8 using the same latitude/longitude rounded to 3 decimals.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# TODO: update these table names before running.
CLIENT_H3_RES9_TABLE = "TODO_CATALOG.TODO_SCHEMA.client_h3_res9"
CLIENT_H3_RES8_TABLE = "TODO_CATALOG.TODO_SCHEMA.client_h3_res8"

# COMMAND ----------

if CLIENT_H3_RES9_TABLE.startswith("TODO_") or CLIENT_H3_RES8_TABLE.startswith("TODO_"):
    raise ValueError("Update CLIENT_H3_RES9_TABLE and CLIENT_H3_RES8_TABLE before running.")

client_df = spark.table(CLIENT_H3_RES9_TABLE)

client_h3_res8_df = (
    client_df.withColumn("lat_3dp", F.round(F.col("latitude").cast("double"), 3))
    .withColumn("lon_3dp", F.round(F.col("longitude").cast("double"), 3))
    .withColumn(
        "has_valid_coordinates",
        F.col("lat_3dp").between(-90.0, 90.0) & F.col("lon_3dp").between(-180.0, 180.0),
    )
    .withColumn(
        "coord_3dp_key",
        F.when(
            F.col("has_valid_coordinates"),
            F.concat_ws(",", F.format_number("lat_3dp", 3), F.format_number("lon_3dp", 3)),
        ),
    )
    .withColumn(
        "h3_res8_int",
        F.when(F.col("has_valid_coordinates"), F.expr("h3_longlatash3(lon_3dp, lat_3dp, 8)")),
    )
    .withColumn(
        "h3_res8",
        F.when(F.col("h3_res8_int").isNotNull(), F.expr("h3_h3tostring(h3_res8_int)")),
    )
)

client_h3_res8_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    CLIENT_H3_RES8_TABLE
)

display(
    client_h3_res8_df.select(
        "client_record_id",
        "account_name_raw",
        "location_name_raw",
        "address_raw",
        "city_raw",
        "country_raw",
        "lat_3dp",
        "lon_3dp",
        "h3_res9",
        "h3_res8",
    ).limit(50)
)
