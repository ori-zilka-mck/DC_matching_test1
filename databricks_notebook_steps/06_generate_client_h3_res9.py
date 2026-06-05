# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6 - Generate H3 resolution 9 for client data
# MAGIC
# MAGIC This is the same idea as step 4, but for the future client table. Because
# MAGIC the client table name and column names are not known yet, update the
# MAGIC placeholders in the configuration cell before running.

# COMMAND ----------

from __future__ import annotations

import re
import unicodedata

from pyspark.sql import functions as F, types as T

# COMMAND ----------

# TODO: update these once the client data table exists.
CLIENT_SOURCE_TABLE = "TODO_CLIENT_SOURCE_TABLE"
CLIENT_H3_RES9_TABLE = "TODO_CATALOG.TODO_SCHEMA.client_h3_res9"

CLIENT_COLUMNS = {
    "source_primary_id": None,  # Optional; set to a unique ID column if one exists.
    "account_name": "TODO_CLIENT_ACCOUNT_NAME_COLUMN",
    "location_name": "TODO_CLIENT_LOCATION_NAME_COLUMN",
    "address": "TODO_CLIENT_ADDRESS_COLUMN",
    "city": "TODO_CLIENT_CITY_COLUMN",
    "state_region": None,  # Optional.
    "postal_code": None,  # Optional.
    "country": "TODO_CLIENT_COUNTRY_COLUMN",
    "latitude": "TODO_CLIENT_LATITUDE_COLUMN",
    "longitude": "TODO_CLIENT_LONGITUDE_COLUMN",
}

# COMMAND ----------

COMPANY_SUFFIXES = {
    "ag",
    "bv",
    "co",
    "company",
    "corp",
    "corporation",
    "gmbh",
    "inc",
    "incorporated",
    "kk",
    "limited",
    "llc",
    "lp",
    "ltd",
    "nv",
    "plc",
    "pte",
    "pty",
    "sa",
    "sas",
    "sarl",
    "the",
}

ADDRESS_ABBREVIATIONS = {
    "ave": "avenue",
    "av": "avenue",
    "blvd": "boulevard",
    "bldg": "building",
    "ctr": "center",
    "ct": "court",
    "dr": "drive",
    "fl": "floor",
    "hwy": "highway",
    "ln": "lane",
    "pkwy": "parkway",
    "pl": "place",
    "rd": "road",
    "st": "street",
    "ste": "suite",
}

COUNTRY_ALIASES = {
    "america": "United States",
    "england": "United Kingdom",
    "great britain": "United Kingdom",
    "the netherlands": "Netherlands",
    "uae": "United Arab Emirates",
    "u a e": "United Arab Emirates",
    "uk": "United Kingdom",
    "u k": "United Kingdom",
    "united states": "United States",
    "united states america": "United States",
    "united states of america": "United States",
    "us": "United States",
    "u s": "United States",
    "usa": "United States",
    "u s a": "United States",
}


def ascii_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    text = unicodedata.normalize("NFKD", text)
    return text.encode("ascii", "ignore").decode("ascii")


def compact_text(value: object) -> str:
    text = ascii_text(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_company(value: object) -> str:
    return " ".join(token for token in compact_text(value).split() if token not in COMPANY_SUFFIXES)


def normalize_address(value: object) -> str:
    return " ".join(
        ADDRESS_ABBREVIATIONS.get(token, token) for token in compact_text(value).split()
    )


def normalize_country(value: object) -> str:
    text = compact_text(value)
    text_without_the = " ".join(token for token in text.split() if token != "the")
    if not text:
        return ""
    if text in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[text]
    if text_without_the in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[text_without_the]
    return " ".join(token.capitalize() for token in text.split())


compact_text_udf = F.udf(compact_text, T.StringType())
normalize_company_udf = F.udf(normalize_company, T.StringType())
normalize_address_udf = F.udf(normalize_address, T.StringType())
normalize_country_udf = F.udf(normalize_country, T.StringType())

# COMMAND ----------

required_logical_columns = [
    "account_name",
    "location_name",
    "address",
    "city",
    "country",
    "latitude",
    "longitude",
]
placeholder_columns = [
    value
    for key, value in CLIENT_COLUMNS.items()
    if key in required_logical_columns and isinstance(value, str) and value.startswith("TODO_")
]
if CLIENT_SOURCE_TABLE.startswith("TODO_") or CLIENT_H3_RES9_TABLE.startswith("TODO_") or placeholder_columns:
    raise ValueError(
        "Update CLIENT_SOURCE_TABLE, CLIENT_H3_RES9_TABLE, and required CLIENT_COLUMNS before running."
    )

client_raw_df = spark.table(CLIENT_SOURCE_TABLE)


def raw_col(logical_name: str) -> F.Column:
    raw_name = CLIENT_COLUMNS.get(logical_name)
    if raw_name and raw_name in client_raw_df.columns:
        return F.col(f"`{raw_name}`").cast("string")
    return F.lit(None).cast("string")


client_df = client_raw_df.select(
    raw_col("source_primary_id").alias("source_primary_id"),
    raw_col("account_name").alias("account_name_raw"),
    raw_col("location_name").alias("location_name_raw"),
    raw_col("address").alias("address_raw"),
    raw_col("city").alias("city_raw"),
    raw_col("state_region").alias("state_region_raw"),
    raw_col("postal_code").alias("postal_code_raw"),
    raw_col("country").alias("country_raw"),
    raw_col("latitude").alias("latitude_raw"),
    raw_col("longitude").alias("longitude_raw"),
)

client_h3_res9_df = (
    client_df.withColumn(
        "source_row_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in client_df.columns]), 256),
    )
    .withColumn(
        "client_record_id",
        F.when(
            F.length(F.trim(F.col("source_primary_id"))) > 0,
            F.concat(F.lit("client:"), F.trim(F.col("source_primary_id"))),
        ).otherwise(F.concat(F.lit("client_row:"), F.col("source_row_hash"))),
    )
    .withColumn("account_name_std", normalize_company_udf("account_name_raw"))
    .withColumn("location_name_std", compact_text_udf("location_name_raw"))
    .withColumn("address_std", normalize_address_udf("address_raw"))
    .withColumn("city_std", compact_text_udf("city_raw"))
    .withColumn("state_region_std", compact_text_udf("state_region_raw"))
    .withColumn("postal_code_std", compact_text_udf("postal_code_raw"))
    .withColumn("country_std", normalize_country_udf("country_raw"))
    .withColumn("country_match_key", compact_text_udf("country_std"))
    .withColumn("latitude", F.regexp_replace(F.col("latitude_raw"), ",", ".").cast("double"))
    .withColumn("longitude", F.regexp_replace(F.col("longitude_raw"), ",", ".").cast("double"))
    .withColumn("lat_3dp", F.round(F.col("latitude"), 3))
    .withColumn("lon_3dp", F.round(F.col("longitude"), 3))
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
        "h3_res9_int",
        F.when(F.col("has_valid_coordinates"), F.expr("h3_longlatash3(lon_3dp, lat_3dp, 9)")),
    )
    .withColumn(
        "h3_res9",
        F.when(F.col("h3_res9_int").isNotNull(), F.expr("h3_h3tostring(h3_res9_int)")),
    )
)

client_h3_res9_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    CLIENT_H3_RES9_TABLE
)

display(
    client_h3_res9_df.select(
        "client_record_id",
        "account_name_raw",
        "location_name_raw",
        "address_raw",
        "city_raw",
        "country_raw",
        "lat_3dp",
        "lon_3dp",
        "h3_res9",
    ).limit(50)
)
