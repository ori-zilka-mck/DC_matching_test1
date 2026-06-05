#!/usr/bin/env python3
"""Reconcile United States DC Byte and Global Data data center records.

This workflow reads the existing source workbooks but writes a brand-new output
workbook so prior Excel deliverables are not modified.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from reconcile_us_dc_451 import (
    add_normalized_fields,
    aggregate_group,
    address_score,
    ascii_text,
    company_score,
    compact_text,
    dedupe_source,
    extract_postal,
    extract_state_from_address,
    full_pair_scores,
    h3_candidates,
    is_res8_match,
    is_res9_match,
    parse_h3_cells,
    ratio,
    street_core,
)


DC_BYTE_WORKBOOK = Path("DC Byte_rawdata.xlsx")
GLOBAL_WORKBOOK = Path("Global data_rawdata.xlsx")
OUTPUT_WORKBOOK = Path("DC_Byte_Global_data_US_reconciliation.xlsx")

OUTPUT_SHEETS = {
    "summary": "reconciliation_summary",
    "dc_norm": "dc_byte_us_normalized",
    "global_norm": "global_data_us_normalized",
    "dc_dedup": "dc_byte_deduped",
    "global_dedup": "global_data_deduped",
    "matches": "match_pairs",
    "manual": "manual_review",
    "unmatched_dc": "unmatched_dc_byte",
    "unmatched_global": "unmachted_global_data",
}


@dataclass(frozen=True)
class StageSummary:
    stage: str
    dc_byte_records: int
    global_data_records: int
    matched_across_both: int
    unique_to_dc_byte: int
    unique_to_global_data: int
    still_unmatched: int


def load_dc_byte() -> pd.DataFrame:
    raw = pd.read_excel(DC_BYTE_WORKBOOK, sheet_name="Raw data")
    df = pd.DataFrame(
        {
            "source": "DC Byte",
            "source_row": raw.index + 2,
            "source_primary_id": raw["Site Id"],
            "site_id": raw["Site Id"],
            "datacenter_id": "",
            "campus_id": "",
            "campus_name": "",
            "part_of_campus": "",
            "global_id": "",
            "project_owner_raw": raw["Company Name"],
            "project_name_raw": raw["DC Name"],
            "company_raw": raw["Company Name"],
            "facility_owner_raw": raw["Company Name"],
            "facility_name_raw": raw["DC Name"],
            "street_address_raw": raw["Address"],
            "city_raw": raw["City"],
            "state_raw": raw["Address"].map(extract_state_from_address),
            "postal_code_raw": raw["Address"].map(extract_postal),
            "country_raw": raw["Country"],
            "latitude_raw": raw["Latitude"],
            "longitude_raw": raw["Longitude"],
        }
    )
    return add_normalized_fields(df)


def load_global_data() -> pd.DataFrame:
    raw = pd.read_excel(GLOBAL_WORKBOOK, sheet_name="Raw data")
    df = pd.DataFrame(
        {
            "source": "Global data",
            "source_row": raw.index + 2,
            "source_primary_id": raw["Id"],
            "site_id": "",
            "datacenter_id": "",
            "campus_id": "",
            "campus_name": "",
            "part_of_campus": "",
            "global_id": raw["Id"],
            "project_owner_raw": raw["Project_Owner"],
            "project_name_raw": raw["Project_Name"],
            "company_raw": raw["Project_Owner"],
            "facility_owner_raw": raw["Project_Owner"],
            "facility_name_raw": raw["Project_Name"],
            "street_address_raw": raw["Street_Address"],
            "city_raw": raw["City"],
            "state_raw": raw["State"],
            "postal_code_raw": raw["PostCode"],
            "country_raw": raw["Country"],
            "latitude_raw": raw["Latitude"],
            "longitude_raw": raw["Longitude"],
            "project_stage_raw": raw["Project_Stage"],
            "project_type_raw": raw["Project_Type"],
            "project_value_usdm_raw": raw["Project_Value_USDm"],
            "project_overview_raw": raw["Project_Overview"],
        }
    )
    return add_normalized_fields(df)


def global_group_key(row: pd.Series) -> str:
    global_id = ascii_text(row["global_id"])
    return f"global_id:{global_id}" if global_id else f"row:{row['source_row']}"


def aggregate_global_group(rows: pd.DataFrame, logical_id: str, dedupe_reason: str) -> dict[str, object]:
    output = aggregate_group(rows, logical_id, dedupe_reason)
    output["global_ids"] = output["source_primary_ids"]
    output["project_owners_raw"] = output["company_raw"]
    output["project_names_raw"] = output["facility_name_raw"]
    for column in [
        "project_stage_raw",
        "project_type_raw",
        "project_value_usdm_raw",
        "project_overview_raw",
    ]:
        if column in rows.columns:
            values = [ascii_text(value) for value in rows[column] if ascii_text(value)]
            seen = list(dict.fromkeys(values))
            output[column] = " | ".join(seen[:8])
    return output


def primary_dedupe_global(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    working["primary_group_key"] = working.apply(global_group_key, axis=1)
    rows = [
        aggregate_global_group(group, group_key, "Id primary collapse")
        for group_key, group in working.groupby("primary_group_key", sort=False)
    ]
    return pd.DataFrame(rows)


def dedupe_global_data(df: pd.DataFrame) -> pd.DataFrame:
    # Reuse the same coordinate/address fuzzy heuristic after Id-based grouping.
    return _restore_global_columns(_heuristic_dedupe_global(primary_dedupe_global(df)))


def _heuristic_dedupe_global(grouped: pd.DataFrame) -> pd.DataFrame:
    from reconcile_us_dc_451 import UnionFind

    if grouped.empty:
        return grouped.copy()
    uf = UnionFind(grouped["logical_record_id"])
    coordinate_buckets: dict[tuple[object, object], list[int]] = defaultdict(list)
    for idx, row in grouped.iterrows():
        if pd.notna(row["lat_round_4"]) and pd.notna(row["lon_round_4"]):
            coordinate_buckets[(row["lat_round_4"], row["lon_round_4"])].append(idx)

    for indices in coordinate_buckets.values():
        for position, left_idx in enumerate(indices):
            for right_idx in indices[position + 1 :]:
                left = grouped.loc[left_idx]
                right = grouped.loc[right_idx]
                if ratio(left["street_address_norm"], right["street_address_norm"]) >= 90:
                    uf.union(left["logical_record_id"], right["logical_record_id"])

    grouped = grouped.copy()
    grouped["heuristic_group_key"] = grouped["logical_record_id"].map(uf.find)

    rows: list[dict[str, object]] = []
    for group_key, cluster in grouped.groupby("heuristic_group_key", sort=False):
        expanded_rows = pd.DataFrame(
            {
                "source": cluster["source"],
                "source_row": cluster["member_source_rows"],
                "source_primary_id": cluster["source_primary_ids"],
                "site_id": "",
                "datacenter_id": "",
                "campus_id": "",
                "campus_name": "",
                "part_of_campus": "",
                "global_id": cluster["global_ids"],
                "project_owner_raw": cluster["project_owners_raw"],
                "project_name_raw": cluster["project_names_raw"],
                "company_raw": cluster["company_raw"],
                "facility_owner_raw": cluster["facility_owner_raw"],
                "facility_name_raw": cluster["facility_name_raw"],
                "street_address_raw": cluster["street_address_raw"],
                "city_raw": cluster["city_raw"],
                "state_raw": cluster["state_raw"],
                "postal_code_raw": cluster["postal_code_raw"],
                "country_raw": cluster["country_raw"],
                "latitude_raw": cluster["latitude"],
                "longitude_raw": cluster["longitude"],
                "project_stage_raw": cluster.get("project_stage_raw", ""),
                "project_type_raw": cluster.get("project_type_raw", ""),
                "project_value_usdm_raw": cluster.get("project_value_usdm_raw", ""),
                "project_overview_raw": cluster.get("project_overview_raw", ""),
            }
        )
        reason = cluster["dedupe_reason"].iloc[0]
        if len(cluster) > 1:
            reason = f"{reason}; coordinate/address fuzzy dedupe"
        row = aggregate_global_group(expanded_rows, group_key, reason)
        row["collapsed_logical_record_ids"] = " | ".join(cluster["logical_record_id"])
        rows.append(row)
    return pd.DataFrame(rows)


def _restore_global_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in [
        "global_ids",
        "project_owners_raw",
        "project_names_raw",
        "project_stage_raw",
        "project_type_raw",
        "project_value_usdm_raw",
        "project_overview_raw",
    ]:
        if column not in df.columns:
            df[column] = ""
    return df


def summary_row(stage: str, dc_count: int, global_count: int, matched: int) -> StageSummary:
    return StageSummary(
        stage=stage,
        dc_byte_records=dc_count,
        global_data_records=global_count,
        matched_across_both=matched,
        unique_to_dc_byte=max(dc_count - matched, 0),
        unique_to_global_data=max(global_count - matched, 0),
        still_unmatched=max(dc_count - matched, 0) + max(global_count - matched, 0),
    )


def match_record(
    stage: str,
    reason: str,
    dc: pd.Series,
    global_row: pd.Series,
    scores: dict[str, object],
    h3_intersection: str = "",
    manual_label: str = "",
) -> dict[str, object]:
    return {
        "match_stage": stage,
        "match_reason": reason,
        "manual_label": manual_label,
        "dc_byte_logical_id": dc["logical_record_id"],
        "dc_byte_source_ids": dc["source_primary_ids"],
        "global_data_logical_id": global_row["logical_record_id"],
        "global_data_ids": global_row["source_primary_ids"],
        "h3_intersection": h3_intersection,
        "combined_score": scores["combined_score"],
        "address_score": scores["address_score"],
        "company_score": scores["company_score"],
        "facility_name_score": scores["facility_name_score"],
        "city_score": scores["city_score"],
        "state_match": scores["state_match"],
        "postal_match": scores["postal_match"],
        "dc_company_raw": dc["company_raw"],
        "global_project_owner_raw": global_row["company_raw"],
        "dc_facility_name_raw": dc["facility_name_raw"],
        "global_project_name_raw": global_row["facility_name_raw"],
        "dc_address_raw": dc["street_address_raw"],
        "global_address_raw": global_row["street_address_raw"],
        "dc_city_raw": dc["city_raw"],
        "global_city_raw": global_row["city_raw"],
        "dc_state_raw": dc["state_raw"],
        "global_state_raw": global_row["state_raw"],
        "dc_postal_raw": dc["postal_code_raw"],
        "global_postal_raw": global_row["postal_code_raw"],
        "dc_latitude": dc["latitude"],
        "dc_longitude": dc["longitude"],
        "global_latitude": global_row["latitude"],
        "global_longitude": global_row["longitude"],
    }


def choose_matches(
    candidates: list[dict[str, object]],
    dc_df: pd.DataFrame,
    global_df: pd.DataFrame,
    stage: str,
    predicate,
) -> list[dict[str, object]]:
    eligible = [candidate for candidate in candidates if predicate(candidate)]
    eligible.sort(key=lambda c: (float(c["combined_score"]), float(c["address_score"])), reverse=True)
    used_dc: set[int] = set()
    used_global: set[int] = set()
    matches: list[dict[str, object]] = []
    for candidate in eligible:
        dc_idx = int(candidate["dc_idx"])
        global_idx = int(candidate["raw451_idx"])
        if dc_idx in used_dc or global_idx in used_global:
            continue
        used_dc.add(dc_idx)
        used_global.add(global_idx)
        dc = dc_df.loc[dc_idx]
        global_row = global_df.loc[global_idx]
        if stage == "H3 res9":
            reason = "exact H3 res9"
        elif float(candidate["company_score"]) >= float(candidate["address_score"]):
            reason = "H3 res8 plus name match"
        else:
            reason = "H3 res8 plus address match"
        matches.append(
            match_record(
                stage,
                reason,
                dc,
                global_row,
                candidate,
                h3_intersection=ascii_text(candidate["h3_intersection"]),
            )
        )
    return matches


def manual_candidate_pairs(
    dc_df: pd.DataFrame,
    global_df: pd.DataFrame,
    unmatched_dc: set[int],
    unmatched_global: set[int],
) -> list[dict[str, object]]:
    by_city_state: dict[tuple[str, str], list[int]] = defaultdict(list)
    by_state: dict[str, list[int]] = defaultdict(list)
    by_postal: dict[str, list[int]] = defaultdict(list)
    by_h3_res8: dict[str, list[int]] = defaultdict(list)
    for idx in unmatched_global:
        row = global_df.loc[idx]
        by_city_state[(row["city_norm"], row["state_norm"])].append(idx)
        if row["state_norm"]:
            by_state[row["state_norm"]].append(idx)
        if row["postal_code_norm"]:
            by_postal[row["postal_code_norm"]].append(idx)
        for cell in parse_h3_cells(row["h3_res8"]):
            by_h3_res8[cell].append(idx)

    pairs: list[dict[str, object]] = []
    for dc_idx in unmatched_dc:
        dc = dc_df.loc[dc_idx]
        candidate_ids: set[int] = set()
        candidate_ids.update(by_city_state.get((dc["city_norm"], dc["state_norm"]), []))
        if dc["postal_code_norm"]:
            candidate_ids.update(by_postal.get(dc["postal_code_norm"], []))
        for cell in parse_h3_cells(dc["h3_res8"]):
            candidate_ids.update(by_h3_res8.get(cell, []))
        if len(candidate_ids) < 8 and dc["state_norm"]:
            candidate_ids.update(by_state.get(dc["state_norm"], [])[:250])
        for global_idx in candidate_ids:
            global_row = global_df.loc[global_idx]
            scores = full_pair_scores(dc, global_row)
            if (
                scores["combined_score"] >= 50
                or scores["address_score"] >= 80
                or scores["company_score"] >= 88
            ):
                pairs.append({"dc_idx": dc_idx, "global_idx": global_idx, **scores})
    pairs.sort(key=lambda c: float(c["combined_score"]), reverse=True)
    return pairs


def manual_review(
    dc_df: pd.DataFrame,
    global_df: pd.DataFrame,
    unmatched_dc: set[int],
    unmatched_global: set[int],
) -> tuple[list[dict[str, object]], pd.DataFrame, set[int], set[int]]:
    used_dc: set[int] = set()
    used_global: set[int] = set()
    matches: list[dict[str, object]] = []
    review_rows: list[dict[str, object]] = []

    for candidate in manual_candidate_pairs(dc_df, global_df, unmatched_dc, unmatched_global):
        dc_idx = int(candidate["dc_idx"])
        global_idx = int(candidate["global_idx"])
        if dc_idx in used_dc or global_idx in used_global:
            continue
        score = float(candidate["combined_score"])
        addr = float(candidate["address_score"])
        comp = float(candidate["company_score"])
        if score >= 84 or (addr >= 90 and comp >= 70):
            label = "Highly likely match"
        elif score >= 70 or (addr >= 84 and comp >= 55):
            label = "Medium confidence match"
        elif score >= 56 or addr >= 78 or comp >= 88:
            label = "Low confidence match"
        else:
            continue

        used_dc.add(dc_idx)
        used_global.add(global_idx)
        dc = dc_df.loc[dc_idx]
        global_row = global_df.loc[global_idx]
        match = match_record(
            "manual review",
            "manual review",
            dc,
            global_row,
            candidate,
            manual_label=label,
        )
        matches.append(match)
        review_rows.append({"review_source": "DC Byte", "review_label": label, **match})
        review_rows.append({"review_source": "Global data", "review_label": label, **match})

    for dc_idx in sorted(unmatched_dc - used_dc):
        dc = dc_df.loc[dc_idx]
        review_rows.append(
            {
                "review_source": "DC Byte",
                "review_label": "Definitely unique",
                "manual_label": "Definitely unique",
                "dc_byte_logical_id": dc["logical_record_id"],
                "dc_byte_source_ids": dc["source_primary_ids"],
                "global_data_logical_id": "",
                "global_data_ids": "",
                "dc_company_raw": dc["company_raw"],
                "dc_facility_name_raw": dc["facility_name_raw"],
                "dc_address_raw": dc["street_address_raw"],
                "dc_city_raw": dc["city_raw"],
                "dc_state_raw": dc["state_raw"],
                "dc_postal_raw": dc["postal_code_raw"],
                "dc_latitude": dc["latitude"],
                "dc_longitude": dc["longitude"],
            }
        )
    for global_idx in sorted(unmatched_global - used_global):
        global_row = global_df.loc[global_idx]
        review_rows.append(
            {
                "review_source": "Global data",
                "review_label": "Definitely unique",
                "manual_label": "Definitely unique",
                "dc_byte_logical_id": "",
                "dc_byte_source_ids": "",
                "global_data_logical_id": global_row["logical_record_id"],
                "global_data_ids": global_row["source_primary_ids"],
                "global_project_owner_raw": global_row["company_raw"],
                "global_project_name_raw": global_row["facility_name_raw"],
                "global_address_raw": global_row["street_address_raw"],
                "global_city_raw": global_row["city_raw"],
                "global_state_raw": global_row["state_raw"],
                "global_postal_raw": global_row["postal_code_raw"],
                "global_latitude": global_row["latitude"],
                "global_longitude": global_row["longitude"],
            }
        )

    return matches, pd.DataFrame(review_rows), used_dc, used_global


def rows_for_unmatched_tab(
    df: pd.DataFrame,
    remaining_unique: set[int],
    low_match_indices: set[int],
    source_label: str,
) -> pd.DataFrame:
    unique_rows = df.loc[sorted(remaining_unique)].copy()
    unique_rows["manual_label"] = "Definitely unique"
    unique_rows["unmatched_tab_note"] = "unmatched_unique"

    low_rows = df.loc[sorted(low_match_indices)].copy()
    low_rows["manual_label"] = "Low confidence match"
    low_rows["unmatched_tab_note"] = (
        "low confidence manual match; counted as matched in reconciliation_summary"
    )
    output = pd.concat([unique_rows, low_rows], ignore_index=True)
    output.insert(0, "unmatched_tab_source", source_label)
    return output


def write_outputs(outputs: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(OUTPUT_WORKBOOK, engine="openpyxl", mode="w") as writer:
        for sheet_name, df in outputs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def main() -> None:
    dc = load_dc_byte()
    global_data = load_global_data()

    dc_us = dc[dc["country_standard"] == "United States"].copy()
    global_us = global_data[global_data["country_standard"] == "United States"].copy()

    summaries: list[StageSummary] = [
        summary_row("1. after US filtering", len(dc_us), len(global_us), 0)
    ]

    dc_deduped = dedupe_source(dc_us, "DC Byte").reset_index(drop=True)
    global_deduped = dedupe_global_data(global_us).reset_index(drop=True)
    summaries.append(
        summary_row(
            "2. after within-source deduplication",
            len(dc_deduped),
            len(global_deduped),
            0,
        )
    )

    unmatched_dc = set(dc_deduped.index)
    unmatched_global = set(global_deduped.index)

    res9_candidates = h3_candidates(
        dc_deduped, global_deduped, unmatched_dc, unmatched_global, "h3_res9"
    )
    res9_matches = choose_matches(
        res9_candidates, dc_deduped, global_deduped, "H3 res9", is_res9_match
    )
    matched_dc_ids = {match["dc_byte_logical_id"] for match in res9_matches}
    matched_global_ids = {match["global_data_logical_id"] for match in res9_matches}
    unmatched_dc -= set(dc_deduped.index[dc_deduped["logical_record_id"].isin(matched_dc_ids)])
    unmatched_global -= set(
        global_deduped.index[global_deduped["logical_record_id"].isin(matched_global_ids)]
    )
    summaries.append(
        summary_row(
            "3. after H3 res9 matching",
            len(dc_deduped),
            len(global_deduped),
            len(res9_matches),
        )
    )

    res8_candidates = h3_candidates(
        dc_deduped, global_deduped, unmatched_dc, unmatched_global, "h3_res8"
    )
    res8_matches = choose_matches(
        res8_candidates, dc_deduped, global_deduped, "H3 res8", is_res8_match
    )
    matched_dc_ids = {match["dc_byte_logical_id"] for match in res8_matches}
    matched_global_ids = {match["global_data_logical_id"] for match in res8_matches}
    unmatched_dc -= set(dc_deduped.index[dc_deduped["logical_record_id"].isin(matched_dc_ids)])
    unmatched_global -= set(
        global_deduped.index[global_deduped["logical_record_id"].isin(matched_global_ids)]
    )
    h3_matched = len(res9_matches) + len(res8_matches)
    summaries.append(
        summary_row(
            "4. after H3 res8 matching",
            len(dc_deduped),
            len(global_deduped),
            h3_matched,
        )
    )

    manual_matches, manual_df, manual_dc_indices, manual_global_indices = manual_review(
        dc_deduped, global_deduped, unmatched_dc, unmatched_global
    )
    low_dc_indices = {
        dc_deduped.index[dc_deduped["logical_record_id"] == match["dc_byte_logical_id"]][0]
        for match in manual_matches
        if match.get("manual_label") == "Low confidence match"
    }
    low_global_indices = {
        global_deduped.index[global_deduped["logical_record_id"] == match["global_data_logical_id"]][0]
        for match in manual_matches
        if match.get("manual_label") == "Low confidence match"
    }

    unmatched_dc -= manual_dc_indices
    unmatched_global -= manual_global_indices

    all_matches = pd.DataFrame(res9_matches + res8_matches + manual_matches)
    final_matched = len(all_matches)
    summaries.append(
        StageSummary(
            stage="5. after manual review",
            dc_byte_records=len(dc_deduped),
            global_data_records=len(global_deduped),
            matched_across_both=final_matched,
            unique_to_dc_byte=len(unmatched_dc),
            unique_to_global_data=len(unmatched_global),
            still_unmatched=0,
        )
    )

    outputs = {
        OUTPUT_SHEETS["summary"]: pd.DataFrame([summary.__dict__ for summary in summaries]),
        OUTPUT_SHEETS["dc_norm"]: dc_us,
        OUTPUT_SHEETS["global_norm"]: global_us,
        OUTPUT_SHEETS["dc_dedup"]: dc_deduped,
        OUTPUT_SHEETS["global_dedup"]: global_deduped,
        OUTPUT_SHEETS["matches"]: all_matches,
        OUTPUT_SHEETS["manual"]: manual_df,
        OUTPUT_SHEETS["unmatched_dc"]: rows_for_unmatched_tab(
            dc_deduped, unmatched_dc, low_dc_indices, "DC Byte"
        ),
        OUTPUT_SHEETS["unmatched_global"]: rows_for_unmatched_tab(
            global_deduped, unmatched_global, low_global_indices, "Global data"
        ),
    }
    write_outputs(outputs)

    print(outputs[OUTPUT_SHEETS["summary"]].to_string(index=False))
    print(f"Wrote reconciliation workbook to {OUTPUT_WORKBOOK}")


if __name__ == "__main__":
    main()
