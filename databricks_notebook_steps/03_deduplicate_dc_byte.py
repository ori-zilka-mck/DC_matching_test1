# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3 - Deduplicate DC Byte records
# MAGIC
# MAGIC A DC Byte row is treated as a duplicate when either:
# MAGIC
# MAGIC 1. It has the same `Site ID` as another DC Byte row.
# MAGIC 2. Its latitude/longitude truncate to the same 4-decimal coordinate key as
# MAGIC    another row, and the normalized address similarity is greater than 90%.
# MAGIC
# MAGIC The output keeps one representative row per duplicate group and preserves
# MAGIC metadata about all duplicate members.

# COMMAND ----------

from __future__ import annotations

import re
from collections import defaultdict
from difflib import SequenceMatcher

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

# COMMAND ----------

# TODO: update these table names before running.
DC_BYTE_ADDRESS_NORMALIZED_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_address_normalized"
DC_BYTE_DEDUPED_TABLE = "TODO_CATALOG.TODO_SCHEMA.dc_byte_deduplicated"

# COMMAND ----------

if DC_BYTE_ADDRESS_NORMALIZED_TABLE.startswith("TODO_") or DC_BYTE_DEDUPED_TABLE.startswith("TODO_"):
    raise ValueError(
        "Update DC_BYTE_ADDRESS_NORMALIZED_TABLE and DC_BYTE_DEDUPED_TABLE before running."
    )


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


def truncate_to_4dp(column_name: str) -> F.Column:
    value = F.col(column_name).cast("double")
    scaled = value * F.lit(10000.0)
    truncated = F.when(value >= 0, F.floor(scaled)).otherwise(F.ceil(scaled)) / F.lit(10000.0)
    return F.when(value.isNull(), F.lit(None).cast("double")).otherwise(truncated)


token_set_score_udf = F.udf(token_set_score, T.DoubleType())

# COMMAND ----------

source_df = spark.table(DC_BYTE_ADDRESS_NORMALIZED_TABLE)

source_columns = source_df.columns
hash_columns = [F.coalesce(F.col(column_name).cast("string"), F.lit("")) for column_name in source_columns]

row_window = Window.orderBy(
    F.coalesce(F.col("source_primary_id"), F.lit("")),
    F.coalesce(F.col("source_row_hash"), F.lit("")),
    F.coalesce(F.col("address_raw"), F.lit("")),
    F.coalesce(F.col("dc_name_raw"), F.lit("")),
)

working_df = (
    source_df.withColumn(
        "dedupe_input_hash",
        F.sha2(F.concat_ws("||", *hash_columns), 256),
    )
    .withColumn("dedupe_sequence", F.row_number().over(row_window))
    .withColumn(
        "dedupe_record_id",
        F.concat(
            F.coalesce(F.col("dc_byte_record_id"), F.lit("dcbyte_row")),
            F.lit(":"),
            F.col("dedupe_sequence").cast("string"),
        ),
    )
    .withColumn("site_id_key", F.lower(F.trim(F.coalesce(F.col("source_primary_id"), F.lit("")))))
    .withColumn("lat_trunc_4dp", truncate_to_4dp("latitude"))
    .withColumn("lon_trunc_4dp", truncate_to_4dp("longitude"))
    .withColumn(
        "coord_trunc_4dp_key",
        F.when(
            F.col("lat_trunc_4dp").isNotNull() & F.col("lon_trunc_4dp").isNotNull(),
            F.concat_ws(
                ",",
                F.format_number("lat_trunc_4dp", 4),
                F.format_number("lon_trunc_4dp", 4),
            ),
        ),
    )
)

# COMMAND ----------

# Duplicate rule A: same Site ID.
site_groups = (
    working_df.filter(F.length("site_id_key") > 0)
    .groupBy("site_id_key")
    .agg(F.collect_list("dedupe_record_id").alias("dedupe_record_ids"))
    .filter(F.size("dedupe_record_ids") > 1)
    .collect()
)

duplicate_edges: list[tuple[str, str, str]] = []
for row in site_groups:
    record_ids = row["dedupe_record_ids"]
    first_id = record_ids[0]
    for other_id in record_ids[1:]:
        duplicate_edges.append((first_id, other_id, "same Site ID"))

# Duplicate rule B: same truncated 4dp coordinate key and >90% address similarity.
left = working_df.select(
    "dedupe_record_id", "coord_trunc_4dp_key", "address_std", "address_raw"
).alias("left")
right = working_df.select(
    "dedupe_record_id", "coord_trunc_4dp_key", "address_std", "address_raw"
).alias("right")

coordinate_address_pairs = (
    left.join(
        right,
        (F.col("left.coord_trunc_4dp_key") == F.col("right.coord_trunc_4dp_key"))
        & (F.col("left.dedupe_record_id") < F.col("right.dedupe_record_id"))
        & F.col("left.coord_trunc_4dp_key").isNotNull(),
        "inner",
    )
    .withColumn(
        "address_similarity",
        token_set_score_udf(F.col("left.address_std"), F.col("right.address_std")),
    )
    .filter(F.col("address_similarity") > 90.0)
    .select(
        F.col("left.dedupe_record_id").alias("left_record_id"),
        F.col("right.dedupe_record_id").alias("right_record_id"),
        "address_similarity",
        F.col("left.coord_trunc_4dp_key").alias("coord_trunc_4dp_key"),
    )
)

for row in coordinate_address_pairs.collect():
    duplicate_edges.append(
        (
            row["left_record_id"],
            row["right_record_id"],
            "same 4dp coordinate key and address similarity > 90%",
        )
    )

# COMMAND ----------

all_record_ids = [row["dedupe_record_id"] for row in working_df.select("dedupe_record_id").collect()]

parent = {record_id: record_id for record_id in all_record_ids}


def find(record_id: str) -> str:
    root = parent[record_id]
    if root != record_id:
        parent[record_id] = find(root)
    return parent[record_id]


def union(left_id: str, right_id: str) -> None:
    left_root = find(left_id)
    right_root = find(right_id)
    if left_root == right_root:
        return
    parent[max(left_root, right_root)] = min(left_root, right_root)


for left_id, right_id, _reason in duplicate_edges:
    union(left_id, right_id)

group_rows = [(record_id, find(record_id)) for record_id in all_record_ids]
group_map_df = spark.createDataFrame(
    group_rows,
    schema=T.StructType(
        [
            T.StructField("dedupe_record_id", T.StringType(), False),
            T.StructField("dedupe_group_id", T.StringType(), False),
        ]
    ),
)

reason_by_group: dict[str, set[str]] = defaultdict(set)
for left_id, right_id, reason in duplicate_edges:
    reason_by_group[find(left_id)].add(reason)
    reason_by_group[find(right_id)].add(reason)

reason_rows = [
    (group_id, "; ".join(sorted(reasons))) for group_id, reasons in reason_by_group.items()
]
reason_df = spark.createDataFrame(
    reason_rows or [("__no_duplicate_groups__", "unique record")],
    schema=T.StructType(
        [
            T.StructField("dedupe_group_id", T.StringType(), False),
            T.StructField("dedupe_reason", T.StringType(), False),
        ]
    ),
).filter(F.col("dedupe_group_id") != "__no_duplicate_groups__")

# COMMAND ----------

grouped_df = working_df.join(group_map_df, "dedupe_record_id", "left")

metadata_df = grouped_df.groupBy("dedupe_group_id").agg(
    F.count("*").alias("dedupe_member_count"),
    F.concat_ws(" | ", F.sort_array(F.collect_set("dedupe_record_id"))).alias(
        "dedupe_member_record_ids"
    ),
    F.concat_ws(" | ", F.sort_array(F.collect_set("source_primary_id"))).alias(
        "dedupe_member_site_ids"
    ),
    F.concat_ws(" | ", F.sort_array(F.collect_set("dc_byte_record_id"))).alias(
        "dedupe_member_dc_byte_record_ids"
    ),
    F.concat_ws(" | ", F.sort_array(F.collect_set("address_raw"))).alias(
        "dedupe_member_raw_addresses"
    ),
    F.concat_ws(" | ", F.sort_array(F.collect_set("address_std"))).alias(
        "dedupe_member_normalized_addresses"
    ),
    F.avg("latitude").alias("latitude_deduped"),
    F.avg("longitude").alias("longitude_deduped"),
    F.first("coord_trunc_4dp_key", ignorenulls=True).alias("coord_trunc_4dp_key_deduped"),
)

representative_window = Window.partitionBy("dedupe_group_id").orderBy(
    F.when(F.length("site_id_key") > 0, F.lit(0)).otherwise(F.lit(1)),
    F.coalesce(F.col("source_primary_id"), F.lit("")),
    F.coalesce(F.col("address_raw"), F.lit("")),
    F.col("dedupe_record_id"),
)

representative_df = (
    grouped_df.withColumn("dedupe_representative_rank", F.row_number().over(representative_window))
    .filter(F.col("dedupe_representative_rank") == 1)
    .drop("dedupe_representative_rank")
)

deduped_df = (
    representative_df.drop("latitude", "longitude", "coord_trunc_4dp_key")
    .join(metadata_df, "dedupe_group_id", "left")
    .join(reason_df, "dedupe_group_id", "left")
    .withColumn("dedupe_reason", F.coalesce(F.col("dedupe_reason"), F.lit("unique record")))
    .withColumnRenamed("latitude_deduped", "latitude")
    .withColumnRenamed("longitude_deduped", "longitude")
    .withColumnRenamed("coord_trunc_4dp_key_deduped", "coord_trunc_4dp_key")
)

deduped_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    DC_BYTE_DEDUPED_TABLE
)

display(
    deduped_df.select(
        "dedupe_group_id",
        "dedupe_reason",
        "dedupe_member_count",
        "source_primary_id",
        "company_name_raw",
        "dc_name_raw",
        "address_raw",
        "address_std",
        "coord_trunc_4dp_key",
        "latitude",
        "longitude",
        "dedupe_member_site_ids",
        "dedupe_member_raw_addresses",
    )
    .orderBy(F.desc("dedupe_member_count"), "dedupe_group_id")
    .limit(100)
)
