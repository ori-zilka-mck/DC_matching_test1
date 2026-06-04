# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5 - Generate DC Byte H3 resolution 8
# MAGIC
# MAGIC This reads the DC Byte H3 resolution 9 table from step 4, keeps the same
# MAGIC 3-decimal rounded coordinate key, and adds H3 resolution 8.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# TODO: update these table names before running.
DC_BYTE_H3_RES9_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_h3_res9"
DC_BYTE_H3_RES8_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_h3_res8"

# COMMAND ----------

if DC_BYTE_H3_RES9_TABLE.startswith("TODO_") or DC_BYTE_H3_RES8_TABLE.startswith("TODO_"):
    raise ValueError("Update DC_BYTE_H3_RES9_TABLE and DC_BYTE_H3_RES8_TABLE before running.")

dc_byte_df = spark.table(DC_BYTE_H3_RES9_TABLE)

dc_byte_h3_res8_df = (
    dc_byte_df.withColumn("lat_3dp", F.round(F.col("latitude").cast("double"), 3))
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
    # Databricks native H3 uses longitude, latitude order.
    .withColumn(
        "h3_res8_int",
        F.when(F.col("has_valid_coordinates"), F.expr("h3_longlatash3(lon_3dp, lat_3dp, 8)")),
    )
    .withColumn(
        "h3_res8",
        F.when(F.col("h3_res8_int").isNotNull(), F.expr("h3_h3tostring(h3_res8_int)")),
    )
)

dc_byte_h3_res8_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    DC_BYTE_H3_RES8_TABLE
)

display(
    dc_byte_h3_res8_df.select(
        "dc_byte_record_id",
        "company_name_raw",
        "dc_name_raw",
        "city_raw",
        "country_raw",
        "latitude",
        "longitude",
        "lat_3dp",
        "lon_3dp",
        "coord_3dp_key",
        "h3_res9",
        "h3_res8",
    ).limit(50)
)
