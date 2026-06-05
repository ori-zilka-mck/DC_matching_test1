# Databricks notebook source
# MAGIC %md
# MAGIC # Step 8 - Match DC Byte to client data within H3 resolution 9 cells
# MAGIC
# MAGIC This matches every DC Byte record to every client record sharing the same
# MAGIC H3 res9 cell, then validates candidates with fuzzy account name, location
# MAGIC name, city, and address scores.

# COMMAND ----------

from __future__ import annotations

import re
from difflib import SequenceMatcher

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

# COMMAND ----------

# TODO: update these table names before running.
DC_BYTE_H3_RES9_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_h3_res9"
CLIENT_H3_RES9_TABLE = "TODO_CATALOG.TODO_SCHEMA.client_h3_res9"
H3_RES9_MATCH_OUTPUT_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_client_matches_h3_res9"

# COMMAND ----------

if (
    DC_BYTE_H3_RES9_TABLE.startswith("TODO_")
    or CLIENT_H3_RES9_TABLE.startswith("TODO_")
    or H3_RES9_MATCH_OUTPUT_TABLE.startswith("TODO_")
):
    raise ValueError("Update the DC Byte, client, and output table names before running.")


def compact_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).lower().strip()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 100.0
    return 100.0 * SequenceMatcher(None, left, right).ratio()


def token_set_score(left_value: object, right_value: object) -> float:
    left_tokens = set(compact_text(left_value).split())
    right_tokens = set(compact_text(right_value).split())
    if not left_tokens or not right_tokens:
        return 0.0
    common = sorted(left_tokens & right_tokens)
    left_only = sorted(left_tokens - right_tokens)
    right_only = sorted(right_tokens - left_tokens)
    common_text = " ".join(common)
    left_text = " ".join(common + left_only)
    right_text = " ".join(common + right_only)
    return round(
        max(
            ratio(left_text, right_text),
            ratio(common_text, left_text),
            ratio(common_text, right_text),
        ),
        2,
    )


def street_number(value: object) -> str:
    for token in compact_text(value).split():
        if token.isdigit():
            return token
    return ""


token_set_score_udf = F.udf(token_set_score, T.DoubleType())
street_number_udf = F.udf(street_number, T.StringType())

# COMMAND ----------

dc = spark.table(DC_BYTE_H3_RES9_TABLE).filter(F.col("h3_res9").isNotNull())
client = spark.table(CLIENT_H3_RES9_TABLE).filter(F.col("h3_res9").isNotNull())

candidate_pairs = (
    dc.alias("dc")
    .join(client.alias("cl"), F.col("dc.h3_res9") == F.col("cl.h3_res9"), "inner")
    .select(
        F.col("dc.h3_res9").alias("h3_res9"),
        F.col("dc.dc_byte_record_id").alias("dc_byte_record_id"),
        F.col("dc.source_primary_id").alias("dc_byte_source_primary_id"),
        F.col("dc.company_name_raw").alias("dc_company_name_raw"),
        F.col("dc.dc_name_raw").alias("dc_location_name_raw"),
        F.col("dc.address_raw").alias("dc_address_raw"),
        F.col("dc.city_raw").alias("dc_city_raw"),
        F.col("dc.country_raw").alias("dc_country_raw"),
        F.col("dc.account_name_std").alias("dc_account_name_std"),
        F.col("dc.location_name_std").alias("dc_location_name_std"),
        F.col("dc.address_std").alias("dc_address_std"),
        F.col("dc.city_std").alias("dc_city_std"),
        F.col("dc.country_match_key").alias("dc_country_match_key"),
        F.col("dc.latitude").alias("dc_latitude"),
        F.col("dc.longitude").alias("dc_longitude"),
        F.col("cl.client_record_id").alias("client_record_id"),
        F.col("cl.source_primary_id").alias("client_source_primary_id"),
        F.col("cl.account_name_raw").alias("client_account_name_raw"),
        F.col("cl.location_name_raw").alias("client_location_name_raw"),
        F.col("cl.address_raw").alias("client_address_raw"),
        F.col("cl.city_raw").alias("client_city_raw"),
        F.col("cl.country_raw").alias("client_country_raw"),
        F.col("cl.account_name_std").alias("client_account_name_std"),
        F.col("cl.location_name_std").alias("client_location_name_std"),
        F.col("cl.address_std").alias("client_address_std"),
        F.col("cl.city_std").alias("client_city_std"),
        F.col("cl.country_match_key").alias("client_country_match_key"),
        F.col("cl.latitude").alias("client_latitude"),
        F.col("cl.longitude").alias("client_longitude"),
    )
)

scored_pairs = (
    candidate_pairs.withColumn(
        "address_score_raw", token_set_score_udf("dc_address_std", "client_address_std")
    )
    .withColumn("dc_street_number", street_number_udf("dc_address_std"))
    .withColumn("client_street_number", street_number_udf("client_address_std"))
    .withColumn(
        "address_score",
        F.when(
            (F.length("dc_street_number") > 0)
            & (F.length("client_street_number") > 0)
            & (F.col("dc_street_number") != F.col("client_street_number")),
            F.least(F.col("address_score_raw"), F.lit(70.0)),
        ).otherwise(F.col("address_score_raw")),
    )
    .withColumn("account_name_score", token_set_score_udf("dc_account_name_std", "client_account_name_std"))
    .withColumn(
        "location_name_score", token_set_score_udf("dc_location_name_std", "client_location_name_std")
    )
    .withColumn("city_score", token_set_score_udf("dc_city_std", "client_city_std"))
    .withColumn(
        "country_match",
        (F.length("dc_country_match_key") > 0)
        & (F.col("dc_country_match_key") == F.col("client_country_match_key")),
    )
    .withColumn(
        "combined_score",
        F.round(
            F.least(
                F.lit(100.0),
                (F.col("address_score") * F.lit(0.42))
                + (F.col("account_name_score") * F.lit(0.28))
                + (F.col("location_name_score") * F.lit(0.20))
                + (F.col("city_score") * F.lit(0.10))
                + F.when(F.col("country_match"), F.lit(3.0)).otherwise(F.lit(0.0)),
            ),
            2,
        ),
    )
)

matched_pairs = scored_pairs.withColumn(
    "match_rule",
    F.when(
        (F.col("address_score") >= 88)
        & ((F.col("account_name_score") >= 55) | (F.col("location_name_score") >= 65)),
        F.lit("res9 address plus account/location"),
    )
    .when(
        (F.col("address_score") >= 95) & (F.col("city_score") >= 80),
        F.lit("res9 very strong address plus city"),
    )
    .when(
        (F.col("address_score") >= 78)
        & (F.col("account_name_score") >= 85)
        & (F.col("location_name_score") >= 70)
        & (F.col("city_score") >= 80),
        F.lit("res9 address plus strong account/name/city"),
    )
    .when(
        (F.col("address_score") >= 70)
        & (F.col("account_name_score") >= 92)
        & (F.col("location_name_score") >= 78)
        & (F.col("city_score") >= 80),
        F.lit("res9 strong account/name with address validation"),
    ),
).filter(F.col("match_rule").isNotNull())

dc_rank_window = Window.partitionBy("dc_byte_record_id").orderBy(
    F.desc("combined_score"), F.desc("address_score"), F.desc("account_name_score")
)
client_rank_window = Window.partitionBy("client_record_id").orderBy(
    F.desc("combined_score"), F.desc("address_score"), F.desc("account_name_score")
)

matched_pairs = (
    matched_pairs.withColumn("dc_match_rank", F.row_number().over(dc_rank_window))
    .withColumn("client_match_rank", F.row_number().over(client_rank_window))
    .withColumn(
        "recommended_one_to_one_match",
        (F.col("dc_match_rank") == 1) & (F.col("client_match_rank") == 1),
    )
    .withColumn("match_stage", F.lit("H3 res9"))
)

matched_pairs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    H3_RES9_MATCH_OUTPUT_TABLE
)

display(
    matched_pairs.orderBy(F.desc("recommended_one_to_one_match"), F.desc("combined_score")).limit(100)
)
