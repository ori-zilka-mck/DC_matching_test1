# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2 - Generate DC Byte H3 resolution 9
# MAGIC
# MAGIC This reads the standardized DC Byte table from step 1, rounds coordinates
# MAGIC to 3 decimal places, and creates an H3 resolution 9 cell for each valid
# MAGIC latitude/longitude pair.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# TODO: update these table names before running.
DC_BYTE_STANDARDIZED_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_standardized"
DC_BYTE_H3_RES9_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_h3_res9"

# COMMAND ----------

if DC_BYTE_STANDARDIZED_TABLE.startswith("TODO_") or DC_BYTE_H3_RES9_TABLE.startswith("TODO_"):
    raise ValueError("Update DC_BYTE_STANDARDIZED_TABLE and DC_BYTE_H3_RES9_TABLE before running.")

dc_byte_df = spark.table(DC_BYTE_STANDARDIZED_TABLE)

dc_byte_h3_res9_df = (
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
        "h3_res9_int",
        F.when(F.col("has_valid_coordinates"), F.expr("h3_longlatash3(lon_3dp, lat_3dp, 9)")),
    )
    .withColumn(
        "h3_res9",
        F.when(F.col("h3_res9_int").isNotNull(), F.expr("h3_h3tostring(h3_res9_int)")),
    )
)

dc_byte_h3_res9_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    DC_BYTE_H3_RES9_TABLE
)

display(
    dc_byte_h3_res9_df.select(
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
    ).limit(50)
)
