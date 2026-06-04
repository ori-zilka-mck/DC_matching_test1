# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2 - Normalize DC Byte address lines
# MAGIC
# MAGIC This step keeps the original DC Byte raw address values and adds normalized
# MAGIC address columns for matching and deduplication.
# MAGIC
# MAGIC Normalization applied:
# MAGIC - lowercase text
# MAGIC - remove extra spaces
# MAGIC - strip punctuation where appropriate
# MAGIC - standardize common abbreviations such as `st` to `street`, `rd` to `road`,
# MAGIC   and `ave` to `avenue`

# COMMAND ----------

from __future__ import annotations

import re
import unicodedata

from pyspark.sql import functions as F, types as T

# COMMAND ----------

# TODO: update these table names before running.
DC_BYTE_STANDARDIZED_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_standardized"
DC_BYTE_ADDRESS_NORMALIZED_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_address_normalized"

# COMMAND ----------

ADDRESS_ABBREVIATIONS = {
    "aly": "alley",
    "annex": "annex",
    "ave": "avenue",
    "av": "avenue",
    "blvd": "boulevard",
    "boul": "boulevard",
    "bldg": "building",
    "br": "branch",
    "brg": "bridge",
    "byps": "bypass",
    "cir": "circle",
    "ctr": "center",
    "ct": "court",
    "cv": "cove",
    "dr": "drive",
    "expy": "expressway",
    "ext": "extension",
    "fl": "floor",
    "fwy": "freeway",
    "hwy": "highway",
    "ln": "lane",
    "mt": "mount",
    "pkwy": "parkway",
    "pl": "place",
    "plz": "plaza",
    "rd": "road",
    "rte": "route",
    "sq": "square",
    "st": "street",
    "ste": "suite",
    "ter": "terrace",
    "tpke": "turnpike",
}


def ascii_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    text = unicodedata.normalize("NFKD", text)
    return text.encode("ascii", "ignore").decode("ascii")


def normalize_address_line(value: object) -> str:
    text = ascii_text(value).lower()
    text = text.replace("&", " and ")
    # Preserve letters and numbers, but use spaces for punctuation/separators.
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [token for token in text.split() if token]
    tokens = [ADDRESS_ABBREVIATIONS.get(token, token) for token in tokens]
    return " ".join(tokens)


normalize_address_line_udf = F.udf(normalize_address_line, T.StringType())

# COMMAND ----------

if DC_BYTE_STANDARDIZED_TABLE.startswith("TODO_") or DC_BYTE_ADDRESS_NORMALIZED_TABLE.startswith("TODO_"):
    raise ValueError(
        "Update DC_BYTE_STANDARDIZED_TABLE and DC_BYTE_ADDRESS_NORMALIZED_TABLE before running."
    )

dc_byte_df = spark.table(DC_BYTE_STANDARDIZED_TABLE)

address_normalized_df = (
    dc_byte_df.withColumn("address_raw_original", F.col("address_raw"))
    .withColumn("address_normalized", normalize_address_line_udf("address_raw"))
    # Keep the existing standard matching column name used by later notebooks.
    .withColumn("address_std", F.col("address_normalized"))
    .withColumn(
        "address_normalization_changed",
        F.coalesce(F.col("address_raw"), F.lit("")) != F.coalesce(F.col("address_normalized"), F.lit("")),
    )
)

address_normalized_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    DC_BYTE_ADDRESS_NORMALIZED_TABLE
)

display(
    address_normalized_df.select(
        "dc_byte_record_id",
        "company_name_raw",
        "dc_name_raw",
        "address_raw_original",
        "address_normalized",
        "address_normalization_changed",
        "city_raw",
        "country_raw",
    ).limit(50)
)
