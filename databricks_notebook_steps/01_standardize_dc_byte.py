# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 - Standardize DC Byte key attributes
# MAGIC
# MAGIC Paste this into a Databricks notebook and update the configuration cell.
# MAGIC It prepares DC Byte globally, not just the US, and writes a standardized
# MAGIC table that later steps can reuse.

# COMMAND ----------

from __future__ import annotations

import re
import unicodedata

from pyspark.sql import functions as F, types as T

# COMMAND ----------

# TODO: update these two table names before running.
DC_BYTE_SOURCE_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_raw"
DC_BYTE_STANDARDIZED_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_standardized"

# Update only if your DC Byte feed uses different column names.
DC_BYTE_COLUMNS = {
    "source_primary_id": "Site Id",
    "company_name": "Company Name",
    "dc_name": "DC Name",
    "address": "Address",
    "city": "City",
    "state_region": None,  # Example: "State or Province" if present.
    "postal_code": None,  # Example: "Postal Code" if present.
    "country": "Country",
    "latitude": "Latitude",
    "longitude": "Longitude",
}

# COMMAND ----------

COUNTRY_ALIASES = {
    "america": "United States",
    "czech republic": "Czechia",
    "deutschland": "Germany",
    "england": "United Kingdom",
    "great britain": "United Kingdom",
    "hong kong sar": "Hong Kong",
    "korea republic of": "South Korea",
    "mainland china": "China",
    "netherlands the": "Netherlands",
    "republic of korea": "South Korea",
    "russian federation": "Russia",
    "the netherlands": "Netherlands",
    "uae": "United Arab Emirates",
    "u a e": "United Arab Emirates",
    "uk": "United Kingdom",
    "u k": "United Kingdom",
    "united kingdom of great britain and northern ireland": "United Kingdom",
    "united states": "United States",
    "united states america": "United States",
    "united states of america": "United States",
    "us": "United States",
    "u s": "United States",
    "usa": "United States",
    "u s a": "United States",
    "viet nam": "Vietnam",
}

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
    "aly": "alley",
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


def title_from_compact(value: object) -> str:
    text = compact_text(value)
    return " ".join(token.capitalize() for token in text.split())


def normalize_country(value: object) -> str:
    text = compact_text(value)
    if not text:
        return ""
    text_without_the = " ".join(token for token in text.split() if token != "the")
    return COUNTRY_ALIASES.get(text, COUNTRY_ALIASES.get(text_without_the, title_from_compact(text)))


def normalize_company(value: object) -> str:
    tokens = [token for token in compact_text(value).split() if token not in COMPANY_SUFFIXES]
    return " ".join(tokens)


def normalize_address(value: object) -> str:
    tokens = compact_text(value).split()
    return " ".join(ADDRESS_ABBREVIATIONS.get(token, token) for token in tokens)


compact_text_udf = F.udf(compact_text, T.StringType())
normalize_country_udf = F.udf(normalize_country, T.StringType())
normalize_company_udf = F.udf(normalize_company, T.StringType())
normalize_address_udf = F.udf(normalize_address, T.StringType())

# COMMAND ----------

if DC_BYTE_SOURCE_TABLE.startswith("TODO_") or DC_BYTE_STANDARDIZED_TABLE.startswith("TODO_"):
    raise ValueError("Update DC_BYTE_SOURCE_TABLE and DC_BYTE_STANDARDIZED_TABLE before running.")

raw_df = spark.table(DC_BYTE_SOURCE_TABLE)


def raw_col(logical_name: str) -> F.Column:
    raw_name = DC_BYTE_COLUMNS.get(logical_name)
    if raw_name and raw_name in raw_df.columns:
        return F.col(f"`{raw_name}`").cast("string")
    return F.lit(None).cast("string")


standardized_df = raw_df.select(
    raw_col("source_primary_id").alias("source_primary_id"),
    raw_col("company_name").alias("company_name_raw"),
    raw_col("dc_name").alias("dc_name_raw"),
    raw_col("address").alias("address_raw"),
    raw_col("city").alias("city_raw"),
    raw_col("state_region").alias("state_region_raw"),
    raw_col("postal_code").alias("postal_code_raw"),
    raw_col("country").alias("country_raw"),
    raw_col("latitude").alias("latitude_raw"),
    raw_col("longitude").alias("longitude_raw"),
).withColumn("source_system", F.lit("DC Byte"))

standardized_df = (
    standardized_df.withColumn(
        "source_row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(column_name), F.lit(""))
                    for column_name in standardized_df.columns
                ],
            ),
            256,
        ),
    )
    .withColumn(
        "dc_byte_record_id",
        F.when(
            F.length(F.trim(F.col("source_primary_id"))) > 0,
            F.concat(F.lit("dcbyte:"), F.trim(F.col("source_primary_id"))),
        ).otherwise(F.concat(F.lit("dcbyte_row:"), F.col("source_row_hash"))),
    )
    .withColumn("account_name_std", normalize_company_udf("company_name_raw"))
    .withColumn("location_name_std", compact_text_udf("dc_name_raw"))
    .withColumn("address_std", normalize_address_udf("address_raw"))
    .withColumn("city_std", compact_text_udf("city_raw"))
    .withColumn("state_region_std", compact_text_udf("state_region_raw"))
    .withColumn("postal_code_std", compact_text_udf("postal_code_raw"))
    .withColumn("country_std", normalize_country_udf("country_raw"))
    .withColumn("country_match_key", compact_text_udf("country_std"))
    .withColumn("latitude", F.regexp_replace(F.col("latitude_raw"), ",", ".").cast("double"))
    .withColumn("longitude", F.regexp_replace(F.col("longitude_raw"), ",", ".").cast("double"))
    .withColumn(
        "has_valid_coordinates",
        F.col("latitude").between(-90.0, 90.0) & F.col("longitude").between(-180.0, 180.0),
    )
)

standardized_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    DC_BYTE_STANDARDIZED_TABLE
)

display(
    standardized_df.select(
        "dc_byte_record_id",
        "company_name_raw",
        "dc_name_raw",
        "address_raw",
        "city_raw",
        "country_raw",
        "account_name_std",
        "location_name_std",
        "address_std",
        "city_std",
        "country_std",
        "latitude",
        "longitude",
        "has_valid_coordinates",
    ).limit(50)
)
