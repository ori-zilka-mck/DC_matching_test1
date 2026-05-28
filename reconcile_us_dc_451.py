#!/usr/bin/env python3
"""Reconcile United States DC Byte and 451 data center records.

The script keeps raw values, creates normalized matching fields, deduplicates
each source independently, performs H3 res9/res8 matching, and writes staged
reconciliation tabs back into the existing workbook.
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import h3
import pandas as pd
from rapidfuzz import fuzz


BASE_WORKBOOK = Path("Sanitized Reconsilation.xlsx")
DC_BYTE_WORKBOOK = Path("DC Byte_rawdata.xlsx")
RAW_451_WORKBOOK = Path("451_rawdata.xlsx")

OUTPUT_SHEETS = {
    "summary": "reconciliation_summary",
    "dc_norm": "dc_byte_us_normalized",
    "raw451_norm": "451_us_normalized",
    "dc_dedup": "dc_byte_deduped",
    "raw451_dedup": "451_deduped",
    "matches": "match_pairs",
    "manual": "manual_review",
    "unmatched_dc": "unmatched_dc_byte",
    "unmatched_451": "unmatched_451",
}

US_COUNTRY_ALIASES = {
    "united states",
    "united states america",
    "united states of america",
    "usa",
    "u s a",
    "us",
    "u s",
    "america",
}

STATE_ABBREVIATIONS = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "district of columbia": "dc",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}
STATE_NAMES_BY_ABBR = {abbr: name for name, abbr in STATE_ABBREVIATIONS.items()}

ADDRESS_ABBREVIATIONS = {
    "aly": "alley",
    "ave": "avenue",
    "av": "avenue",
    "blvd": "boulevard",
    "boul": "boulevard",
    "cir": "circle",
    "ct": "court",
    "ctr": "center",
    "dr": "drive",
    "expy": "expressway",
    "fwy": "freeway",
    "hwy": "highway",
    "ln": "lane",
    "pkwy": "parkway",
    "pl": "place",
    "rd": "road",
    "sq": "square",
    "st": "street",
    "ter": "terrace",
    "tpke": "turnpike",
}

COMPANY_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "llc",
    "lp",
    "ltd",
    "limited",
    "plc",
    "the",
}

ADDRESS_UNIT_TOKENS = {
    "building",
    "bldg",
    "floor",
    "fl",
    "level",
    "room",
    "ste",
    "suite",
    "unit",
}


@dataclass(frozen=True)
class StageSummary:
    stage: str
    dc_byte_records: int
    raw451_records: int
    matched_across_both: int
    unique_to_dc_byte: int
    unique_to_451: int
    still_unmatched: int


def ascii_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    return text.encode("ascii", "ignore").decode("ascii")


def compact_text(value: object) -> str:
    text = ascii_text(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_country(value: object) -> str:
    normalized = compact_text(value)
    normalized_without_the = " ".join(token for token in normalized.split() if token != "the")
    if normalized_without_the in US_COUNTRY_ALIASES:
        return "United States"
    return "United States" if normalized in US_COUNTRY_ALIASES else ascii_text(value)


def normalize_state(value: object) -> str:
    normalized = compact_text(value)
    if not normalized:
        return ""
    if normalized in STATE_ABBREVIATIONS:
        return STATE_ABBREVIATIONS[normalized]
    if normalized in STATE_NAMES_BY_ABBR:
        return normalized
    return normalized


def normalize_company(value: object) -> str:
    tokens = compact_text(value).split()
    tokens = [token for token in tokens if token not in COMPANY_SUFFIXES]
    return " ".join(tokens)


def normalize_address(value: object) -> str:
    tokens = compact_text(value).split()
    normalized_tokens = [ADDRESS_ABBREVIATIONS.get(token, token) for token in tokens]
    return " ".join(normalized_tokens)


def extract_postal(value: object) -> str:
    text = ascii_text(value)
    match = re.search(r"\b\d{5}(?:-\d{4})?\b", text)
    return match.group(0) if match else ""


def extract_state_from_address(value: object) -> str:
    text = compact_text(value)
    tokens = text.split()
    for position, token in enumerate(tokens):
        if token in STATE_NAMES_BY_ABBR:
            return token
        if token in STATE_ABBREVIATIONS:
            return STATE_ABBREVIATIONS[token]
        for state_name, abbreviation in STATE_ABBREVIATIONS.items():
            parts = state_name.split()
            if tokens[position : position + len(parts)] == parts:
                return abbreviation
    return ""


def to_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def join_unique(values: Iterable[object], limit: int | None = None) -> str:
    seen: list[str] = []
    for value in values:
        text = ascii_text(value)
        if text and text not in seen:
            seen.append(text)
    if limit is not None and len(seen) > limit:
        return " | ".join(seen[:limit] + [f"... ({len(seen) - limit} more)"])
    return " | ".join(seen)


def first_non_empty(values: Iterable[object]) -> str:
    for value in values:
        text = ascii_text(value)
        if text:
            return text
    return ""


def mode_non_empty(values: Iterable[object]) -> str:
    cleaned = [ascii_text(value) for value in values if ascii_text(value)]
    if not cleaned:
        return ""
    return Counter(cleaned).most_common(1)[0][0]


def truncate_decimal(value: float, digits: int = 3) -> float:
    factor = 10**digits
    return math.trunc(value * factor) / factor


def h3_cells_for_rows(rows: pd.DataFrame, resolution: int) -> str:
    cells: list[str] = []
    for lat, lon in zip(rows["latitude"], rows["longitude"]):
        if pd.isna(lat) or pd.isna(lon):
            continue
        lat_trunc = truncate_decimal(float(lat), 3)
        lon_trunc = truncate_decimal(float(lon), 3)
        try:
            cell = h3.latlng_to_cell(lat_trunc, lon_trunc, resolution)
        except AttributeError:
            cell = h3.geo_to_h3(lat_trunc, lon_trunc, resolution)
        if cell not in cells:
            cells.append(cell)
    return " | ".join(cells)


def parse_h3_cells(value: object) -> set[str]:
    return {part.strip() for part in ascii_text(value).split("|") if part.strip()}


def street_number(address_norm: str) -> str:
    for token in address_norm.split():
        if token.isdigit():
            return token
    return ""


def street_core(row: pd.Series) -> str:
    address = ascii_text(row.get("street_address_norm"))
    remove_tokens = set(ascii_text(row.get("city_norm")).split())
    state = ascii_text(row.get("state_norm"))
    postal = ascii_text(row.get("postal_code_norm"))
    if state:
        remove_tokens.add(state)
        remove_tokens.update(STATE_NAMES_BY_ABBR.get(state, "").split())
    if postal:
        remove_tokens.add(postal)
    remove_tokens.update({"united", "states", "usa", "us"})

    kept: list[str] = []
    for token in address.split():
        if token in ADDRESS_UNIT_TOKENS:
            break
        if token in remove_tokens:
            continue
        kept.append(token)
    return " ".join(kept)


def ratio(a: object, b: object) -> float:
    left = ascii_text(a)
    right = ascii_text(b)
    if not left or not right:
        return 0.0
    return float(fuzz.token_set_ratio(left, right))


def company_score(dc: pd.Series, raw451: pd.Series) -> float:
    dc_company = dc.get("company_norm", "")
    return max(
        ratio(dc_company, raw451.get("facility_owner_norm", "")),
        ratio(dc_company, raw451.get("company_norm", "")),
    )


def address_score(dc: pd.Series, raw451: pd.Series) -> float:
    dc_core = street_core(dc)
    raw451_core = street_core(raw451)
    score = ratio(dc_core, raw451_core)
    dc_number = street_number(dc_core)
    raw451_number = street_number(raw451_core)
    if dc_number and raw451_number and dc_number != raw451_number:
        score = min(score, 70.0)
    return score


def full_pair_scores(dc: pd.Series, raw451: pd.Series) -> dict[str, float | bool]:
    addr = address_score(dc, raw451)
    comp = company_score(dc, raw451)
    facility_name = ratio(dc.get("facility_name_norm", ""), raw451.get("facility_name_norm", ""))
    city = ratio(dc.get("city_norm", ""), raw451.get("city_norm", ""))
    state_match = bool(dc.get("state_norm") and dc.get("state_norm") == raw451.get("state_norm"))
    postal_match = bool(
        dc.get("postal_code_norm") and dc.get("postal_code_norm") == raw451.get("postal_code_norm")
    )
    combined = (0.45 * addr) + (0.35 * comp) + (0.12 * facility_name) + (0.08 * city)
    if state_match:
        combined += 3
    if postal_match:
        combined += 4
    return {
        "address_score": round(addr, 2),
        "company_score": round(comp, 2),
        "facility_name_score": round(facility_name, 2),
        "city_score": round(city, 2),
        "state_match": state_match,
        "postal_match": postal_match,
        "combined_score": round(min(combined, 100), 2),
    }


def is_part_of_campus(value: object) -> bool:
    return compact_text(value) in {"yes", "y", "true", "1"}


def add_normalized_fields(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["country_standard"] = result["country_raw"].map(normalize_country)
    result["company_norm"] = result["company_raw"].map(normalize_company)
    result["facility_owner_norm"] = result["facility_owner_raw"].map(normalize_company)
    result["facility_name_norm"] = result["facility_name_raw"].map(compact_text)
    result["street_address_norm"] = result["street_address_raw"].map(normalize_address)
    result["city_norm"] = result["city_raw"].map(compact_text)
    result["state_norm"] = result["state_raw"].map(normalize_state)
    result["postal_code_norm"] = result["postal_code_raw"].map(compact_text)
    result["latitude"] = result["latitude_raw"].map(to_float)
    result["longitude"] = result["longitude_raw"].map(to_float)
    result["postal_code_norm"] = result.apply(
        lambda row: row["postal_code_norm"] or compact_text(extract_postal(row["street_address_raw"])),
        axis=1,
    )
    result["lat_trunc_3"] = result["latitude"].map(
        lambda value: truncate_decimal(value, 3) if pd.notna(value) else pd.NA
    )
    result["lon_trunc_3"] = result["longitude"].map(
        lambda value: truncate_decimal(value, 3) if pd.notna(value) else pd.NA
    )
    result["coord_trunc_3_key"] = result.apply(
        lambda row: (
            f"{row['lat_trunc_3']:.3f},{row['lon_trunc_3']:.3f}"
            if pd.notna(row["lat_trunc_3"]) and pd.notna(row["lon_trunc_3"])
            else ""
        ),
        axis=1,
    )
    result["lat_round_4"] = result["latitude"].round(4)
    result["lon_round_4"] = result["longitude"].round(4)
    return result


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


def load_451() -> pd.DataFrame:
    raw = pd.read_excel(RAW_451_WORKBOOK, sheet_name="Raw data")
    df = pd.DataFrame(
        {
            "source": "451",
            "source_row": raw.index + 2,
            "source_primary_id": raw["Datacenter ID"],
            "site_id": "",
            "datacenter_id": raw["Datacenter ID"],
            "campus_id": raw["Campus ID"],
            "campus_name": raw["Campus Name"],
            "part_of_campus": raw["Part of Campus?"],
            "company_raw": raw["Company"],
            "facility_owner_raw": raw["Facility Owner"],
            "facility_name_raw": raw["Datacenter Name"],
            "street_address_raw": raw["Street Address"],
            "city_raw": raw["City"],
            "state_raw": raw["State or Province"],
            "postal_code_raw": raw["Postal Code"],
            "country_raw": raw["Country"],
            "latitude_raw": raw["Latitude"],
            "longitude_raw": raw["Longitude"],
        }
    )
    return add_normalized_fields(df)


def dc_group_key(row: pd.Series) -> str:
    site_id = ascii_text(row["site_id"])
    return f"site:{site_id}" if site_id else f"row:{row['source_row']}"


def raw451_group_key(row: pd.Series) -> str:
    if is_part_of_campus(row["part_of_campus"]):
        campus_id = ascii_text(row["campus_id"])
        if campus_id:
            return f"campus:{campus_id}"
        campus_name = compact_text(row["campus_name"])
        if campus_name:
            return f"campus_name:{campus_name}:{row['city_norm']}:{row['state_norm']}"
    dc_id = ascii_text(row["datacenter_id"])
    return f"dcid:{dc_id}" if dc_id else f"row:{row['source_row']}"


def aggregate_group(rows: pd.DataFrame, logical_id: str, dedupe_reason: str) -> dict[str, object]:
    if "latitude" not in rows.columns or "longitude" not in rows.columns:
        rows = add_normalized_fields(rows)
    lat_values = pd.to_numeric(rows["latitude"], errors="coerce").dropna()
    lon_values = pd.to_numeric(rows["longitude"], errors="coerce").dropna()
    company = join_unique(rows["company_raw"], limit=8)
    owner = join_unique(rows["facility_owner_raw"], limit=8)
    name = join_unique(rows["facility_name_raw"], limit=8)
    address = mode_non_empty(rows["street_address_raw"])
    city = mode_non_empty(rows["city_raw"])
    state = mode_non_empty(rows["state_raw"])
    postal = mode_non_empty(rows["postal_code_raw"]) or extract_postal(address)
    country = mode_non_empty(rows["country_raw"])
    country_standard = mode_non_empty(rows["country_standard"])
    source = first_non_empty(rows["source"])

    output = {
        "source": source,
        "logical_record_id": logical_id,
        "member_count": len(rows),
        "source_primary_ids": join_unique(rows["source_primary_id"]),
        "site_ids": join_unique(rows["site_id"]),
        "datacenter_ids": join_unique(rows["datacenter_id"]),
        "campus_ids": join_unique(rows["campus_id"]),
        "campus_names": join_unique(rows["campus_name"], limit=8),
        "dedupe_reason": dedupe_reason,
        "company_raw": company,
        "facility_owner_raw": owner,
        "facility_name_raw": name,
        "street_address_raw": address,
        "city_raw": city,
        "state_raw": state,
        "postal_code_raw": postal,
        "country_raw": country,
        "country_standard": country_standard,
        "latitude": round(float(lat_values.mean()), 8) if not lat_values.empty else pd.NA,
        "longitude": round(float(lon_values.mean()), 8) if not lon_values.empty else pd.NA,
        "member_source_rows": join_unique(rows["source_row"]),
        "h3_res9": h3_cells_for_rows(rows, 9),
        "h3_res8": h3_cells_for_rows(rows, 8),
    }
    normalized = add_normalized_fields(
        pd.DataFrame(
            [
                {
                    "source": source,
                    "source_row": 0,
                    "source_primary_id": output["source_primary_ids"],
                    "site_id": output["site_ids"],
                    "datacenter_id": output["datacenter_ids"],
                    "campus_id": output["campus_ids"],
                    "campus_name": output["campus_names"],
                    "part_of_campus": "",
                    "company_raw": company,
                    "facility_owner_raw": owner,
                    "facility_name_raw": name,
                    "street_address_raw": address,
                    "city_raw": city,
                    "state_raw": state,
                    "postal_code_raw": postal,
                    "country_raw": country,
                    "latitude_raw": output["latitude"],
                    "longitude_raw": output["longitude"],
                }
            ]
        )
    ).iloc[0]
    for column in [
        "company_norm",
        "facility_owner_norm",
        "facility_name_norm",
        "street_address_norm",
        "city_norm",
        "state_norm",
        "postal_code_norm",
        "lat_trunc_3",
        "lon_trunc_3",
        "coord_trunc_3_key",
        "lat_round_4",
        "lon_round_4",
    ]:
        output[column] = normalized[column]
    return output


class UnionFind:
    def __init__(self, values: Iterable[str]):
        self.parent = {value: value for value in values}

    def find(self, value: str) -> str:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def primary_dedupe(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    group_func = dc_group_key if source_name == "DC Byte" else raw451_group_key
    working = df.copy()
    working["primary_group_key"] = working.apply(group_func, axis=1)
    dedupe_reason = "Site ID primary collapse" if source_name == "DC Byte" else "Data Center ID/campus collapse"
    rows = [
        aggregate_group(group, group_key, dedupe_reason)
        for group_key, group in working.groupby("primary_group_key", sort=False)
    ]
    return pd.DataFrame(rows)


def heuristic_dedupe(grouped: pd.DataFrame) -> pd.DataFrame:
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
        source = first_non_empty(cluster["source"])
        member_ids = join_unique(cluster["logical_record_id"])
        reason = first_non_empty(cluster["dedupe_reason"])
        if len(cluster) > 1:
            reason = f"{reason}; coordinate/address fuzzy dedupe"
        expanded_rows = pd.DataFrame(
            {
                "source": source,
                "source_row": cluster["member_source_rows"],
                "source_primary_id": cluster["source_primary_ids"],
                "site_id": cluster["site_ids"],
                "datacenter_id": cluster["datacenter_ids"],
                "campus_id": cluster["campus_ids"],
                "campus_name": cluster["campus_names"],
                "part_of_campus": "",
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
            }
        )
        row = aggregate_group(expanded_rows, group_key, reason)
        row["collapsed_logical_record_ids"] = member_ids
        rows.append(row)
    return pd.DataFrame(rows)


def dedupe_source(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    return heuristic_dedupe(primary_dedupe(df, source_name))


def cell_index(df: pd.DataFrame, h3_column: str) -> dict[str, list[int]]:
    index: dict[str, list[int]] = defaultdict(list)
    for idx, row in df.iterrows():
        for cell in parse_h3_cells(row[h3_column]):
            index[cell].append(idx)
    return index


def h3_candidates(
    dc_df: pd.DataFrame,
    raw451_df: pd.DataFrame,
    unmatched_dc: set[int],
    unmatched_451: set[int],
    h3_column: str,
) -> list[dict[str, object]]:
    raw451_by_cell = cell_index(raw451_df.loc[list(unmatched_451)], h3_column)
    candidates: dict[tuple[int, int], dict[str, object]] = {}
    for dc_idx in unmatched_dc:
        dc_row = dc_df.loc[dc_idx]
        for cell in parse_h3_cells(dc_row[h3_column]):
            for raw451_idx in raw451_by_cell.get(cell, []):
                key = (dc_idx, raw451_idx)
                if key in candidates:
                    continue
                raw451_row = raw451_df.loc[raw451_idx]
                scores = full_pair_scores(dc_row, raw451_row)
                candidates[key] = {
                    "dc_idx": dc_idx,
                    "raw451_idx": raw451_idx,
                    "h3_intersection": cell,
                    **scores,
                }
    return list(candidates.values())


def is_res9_match(candidate: dict[str, object]) -> bool:
    addr = float(candidate["address_score"])
    comp = float(candidate["company_score"])
    name = float(candidate["facility_name_score"])
    city = float(candidate["city_score"])
    return (
        (addr >= 88 and comp >= 55)
        or (addr >= 95 and city >= 80)
        or (addr >= 84 and comp >= 82)
        or (comp >= 92 and name >= 72 and city >= 80)
    )


def is_res8_match(candidate: dict[str, object]) -> bool:
    addr = float(candidate["address_score"])
    comp = float(candidate["company_score"])
    name = float(candidate["facility_name_score"])
    city = float(candidate["city_score"])
    return (
        (addr >= 90 and comp >= 60)
        or (addr >= 96 and city >= 85)
        or (addr >= 86 and comp >= 86)
        or (comp >= 94 and name >= 78 and city >= 85)
    )


def choose_matches(
    candidates: list[dict[str, object]],
    dc_df: pd.DataFrame,
    raw451_df: pd.DataFrame,
    reason: str,
    predicate,
) -> list[dict[str, object]]:
    eligible = [candidate for candidate in candidates if predicate(candidate)]
    eligible.sort(key=lambda c: (float(c["combined_score"]), float(c["address_score"])), reverse=True)
    used_dc: set[int] = set()
    used_451: set[int] = set()
    matches: list[dict[str, object]] = []
    for candidate in eligible:
        dc_idx = int(candidate["dc_idx"])
        raw451_idx = int(candidate["raw451_idx"])
        if dc_idx in used_dc or raw451_idx in used_451:
            continue
        used_dc.add(dc_idx)
        used_451.add(raw451_idx)
        dc = dc_df.loc[dc_idx]
        raw451 = raw451_df.loc[raw451_idx]
        match_reason = reason
        if reason == "H3 res8":
            if float(candidate["company_score"]) >= float(candidate["address_score"]):
                match_reason = "H3 res8 plus name match"
            else:
                match_reason = "H3 res8 plus address match"
        matches.append(
            {
                "match_stage": reason,
                "match_reason": "exact H3 res9" if reason == "H3 res9" else match_reason,
                "dc_byte_logical_id": dc["logical_record_id"],
                "dc_byte_source_ids": dc["source_primary_ids"],
                "451_logical_id": raw451["logical_record_id"],
                "451_source_ids": raw451["source_primary_ids"],
                "451_campus_ids": raw451["campus_ids"],
                "h3_intersection": candidate["h3_intersection"],
                "combined_score": candidate["combined_score"],
                "address_score": candidate["address_score"],
                "company_score": candidate["company_score"],
                "facility_name_score": candidate["facility_name_score"],
                "city_score": candidate["city_score"],
                "state_match": candidate["state_match"],
                "postal_match": candidate["postal_match"],
                "dc_company_raw": dc["company_raw"],
                "451_company_raw": raw451["company_raw"],
                "451_facility_owner_raw": raw451["facility_owner_raw"],
                "dc_facility_name_raw": dc["facility_name_raw"],
                "451_facility_name_raw": raw451["facility_name_raw"],
                "dc_address_raw": dc["street_address_raw"],
                "451_address_raw": raw451["street_address_raw"],
                "dc_city_raw": dc["city_raw"],
                "451_city_raw": raw451["city_raw"],
                "dc_state_raw": dc["state_raw"],
                "451_state_raw": raw451["state_raw"],
                "dc_postal_raw": dc["postal_code_raw"],
                "451_postal_raw": raw451["postal_code_raw"],
                "dc_latitude": dc["latitude"],
                "dc_longitude": dc["longitude"],
                "451_latitude": raw451["latitude"],
                "451_longitude": raw451["longitude"],
            }
        )
    return matches


def manual_candidate_pairs(
    dc_df: pd.DataFrame,
    raw451_df: pd.DataFrame,
    unmatched_dc: set[int],
    unmatched_451: set[int],
) -> list[dict[str, object]]:
    by_city_state: dict[tuple[str, str], list[int]] = defaultdict(list)
    by_state: dict[str, list[int]] = defaultdict(list)
    by_postal: dict[str, list[int]] = defaultdict(list)
    for idx in unmatched_451:
        row = raw451_df.loc[idx]
        by_city_state[(row["city_norm"], row["state_norm"])].append(idx)
        if row["state_norm"]:
            by_state[row["state_norm"]].append(idx)
        if row["postal_code_norm"]:
            by_postal[row["postal_code_norm"]].append(idx)

    pairs: list[dict[str, object]] = []
    for dc_idx in unmatched_dc:
        dc = dc_df.loc[dc_idx]
        candidate_ids: set[int] = set()
        candidate_ids.update(by_city_state.get((dc["city_norm"], dc["state_norm"]), []))
        if dc["postal_code_norm"]:
            candidate_ids.update(by_postal.get(dc["postal_code_norm"], []))
        if len(candidate_ids) < 8 and dc["state_norm"]:
            candidate_ids.update(by_state.get(dc["state_norm"], [])[:250])
        for raw451_idx in candidate_ids:
            raw451 = raw451_df.loc[raw451_idx]
            scores = full_pair_scores(dc, raw451)
            # Manual review can use weaker spatial evidence, but still needs
            # some text/geography agreement to avoid manufacturing matches.
            if (
                scores["combined_score"] >= 50
                or scores["address_score"] >= 80
                or scores["company_score"] >= 88
            ):
                pairs.append({"dc_idx": dc_idx, "raw451_idx": raw451_idx, **scores})
    pairs.sort(key=lambda c: float(c["combined_score"]), reverse=True)
    return pairs


def manual_review(
    dc_df: pd.DataFrame,
    raw451_df: pd.DataFrame,
    unmatched_dc: set[int],
    unmatched_451: set[int],
) -> tuple[list[dict[str, object]], pd.DataFrame]:
    used_dc: set[int] = set()
    used_451: set[int] = set()
    matches: list[dict[str, object]] = []
    review_rows: list[dict[str, object]] = []

    for candidate in manual_candidate_pairs(dc_df, raw451_df, unmatched_dc, unmatched_451):
        dc_idx = int(candidate["dc_idx"])
        raw451_idx = int(candidate["raw451_idx"])
        if dc_idx in used_dc or raw451_idx in used_451:
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
        used_451.add(raw451_idx)
        dc = dc_df.loc[dc_idx]
        raw451 = raw451_df.loc[raw451_idx]
        match = {
            "match_stage": "manual review",
            "match_reason": "manual review",
            "manual_label": label,
            "dc_byte_logical_id": dc["logical_record_id"],
            "dc_byte_source_ids": dc["source_primary_ids"],
            "451_logical_id": raw451["logical_record_id"],
            "451_source_ids": raw451["source_primary_ids"],
            "451_campus_ids": raw451["campus_ids"],
            "h3_intersection": "",
            "combined_score": candidate["combined_score"],
            "address_score": candidate["address_score"],
            "company_score": candidate["company_score"],
            "facility_name_score": candidate["facility_name_score"],
            "city_score": candidate["city_score"],
            "state_match": candidate["state_match"],
            "postal_match": candidate["postal_match"],
            "dc_company_raw": dc["company_raw"],
            "451_company_raw": raw451["company_raw"],
            "451_facility_owner_raw": raw451["facility_owner_raw"],
            "dc_facility_name_raw": dc["facility_name_raw"],
            "451_facility_name_raw": raw451["facility_name_raw"],
            "dc_address_raw": dc["street_address_raw"],
            "451_address_raw": raw451["street_address_raw"],
            "dc_city_raw": dc["city_raw"],
            "451_city_raw": raw451["city_raw"],
            "dc_state_raw": dc["state_raw"],
            "451_state_raw": raw451["state_raw"],
            "dc_postal_raw": dc["postal_code_raw"],
            "451_postal_raw": raw451["postal_code_raw"],
            "dc_latitude": dc["latitude"],
            "dc_longitude": dc["longitude"],
            "451_latitude": raw451["latitude"],
            "451_longitude": raw451["longitude"],
        }
        matches.append(match)
        review_rows.append({"review_source": "DC Byte", "review_label": label, **match})
        review_rows.append({"review_source": "451", "review_label": label, **match})

    for dc_idx in sorted(unmatched_dc - used_dc):
        dc = dc_df.loc[dc_idx]
        review_rows.append(
            {
                "review_source": "DC Byte",
                "review_label": "Definitely unique",
                "dc_byte_logical_id": dc["logical_record_id"],
                "dc_byte_source_ids": dc["source_primary_ids"],
                "451_logical_id": "",
                "451_source_ids": "",
                "manual_label": "Definitely unique",
                "combined_score": "",
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
    for raw451_idx in sorted(unmatched_451 - used_451):
        raw451 = raw451_df.loc[raw451_idx]
        review_rows.append(
            {
                "review_source": "451",
                "review_label": "Definitely unique",
                "dc_byte_logical_id": "",
                "dc_byte_source_ids": "",
                "451_logical_id": raw451["logical_record_id"],
                "451_source_ids": raw451["source_primary_ids"],
                "451_campus_ids": raw451["campus_ids"],
                "manual_label": "Definitely unique",
                "combined_score": "",
                "451_company_raw": raw451["company_raw"],
                "451_facility_owner_raw": raw451["facility_owner_raw"],
                "451_facility_name_raw": raw451["facility_name_raw"],
                "451_address_raw": raw451["street_address_raw"],
                "451_city_raw": raw451["city_raw"],
                "451_state_raw": raw451["state_raw"],
                "451_postal_raw": raw451["postal_code_raw"],
                "451_latitude": raw451["latitude"],
                "451_longitude": raw451["longitude"],
            }
        )

    return matches, pd.DataFrame(review_rows)


def summary_row(stage: str, dc_count: int, raw451_count: int, matched: int) -> StageSummary:
    return StageSummary(
        stage=stage,
        dc_byte_records=dc_count,
        raw451_records=raw451_count,
        matched_across_both=matched,
        unique_to_dc_byte=max(dc_count - matched, 0),
        unique_to_451=max(raw451_count - matched, 0),
        still_unmatched=max(dc_count - matched, 0) + max(raw451_count - matched, 0),
    )


def write_outputs(
    summary: pd.DataFrame,
    dc_us: pd.DataFrame,
    raw451_us: pd.DataFrame,
    dc_deduped: pd.DataFrame,
    raw451_deduped: pd.DataFrame,
    matches: pd.DataFrame,
    manual: pd.DataFrame,
    unmatched_dc: pd.DataFrame,
    unmatched_451: pd.DataFrame,
) -> None:
    outputs = {
        OUTPUT_SHEETS["summary"]: summary,
        OUTPUT_SHEETS["dc_norm"]: dc_us,
        OUTPUT_SHEETS["raw451_norm"]: raw451_us,
        OUTPUT_SHEETS["dc_dedup"]: dc_deduped,
        OUTPUT_SHEETS["raw451_dedup"]: raw451_deduped,
        OUTPUT_SHEETS["matches"]: matches,
        OUTPUT_SHEETS["manual"]: manual,
        OUTPUT_SHEETS["unmatched_dc"]: unmatched_dc,
        OUTPUT_SHEETS["unmatched_451"]: unmatched_451,
    }
    with pd.ExcelWriter(
        BASE_WORKBOOK,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace",
    ) as writer:
        for sheet_name, df in outputs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def main() -> None:
    dc = load_dc_byte()
    raw451 = load_451()

    dc_us = dc[dc["country_standard"] == "United States"].copy()
    raw451_us = raw451[raw451["country_standard"] == "United States"].copy()

    summaries: list[StageSummary] = [
        summary_row("1. after US filtering", len(dc_us), len(raw451_us), 0)
    ]

    dc_deduped = dedupe_source(dc_us, "DC Byte").reset_index(drop=True)
    raw451_deduped = dedupe_source(raw451_us, "451").reset_index(drop=True)
    summaries.append(
        summary_row("2. after within-source deduplication", len(dc_deduped), len(raw451_deduped), 0)
    )

    unmatched_dc = set(dc_deduped.index)
    unmatched_451 = set(raw451_deduped.index)

    res9_candidates = h3_candidates(dc_deduped, raw451_deduped, unmatched_dc, unmatched_451, "h3_res9")
    res9_matches = choose_matches(
        res9_candidates, dc_deduped, raw451_deduped, "H3 res9", is_res9_match
    )
    unmatched_dc -= {
        dc_deduped.index[dc_deduped["logical_record_id"] == match["dc_byte_logical_id"]][0]
        for match in res9_matches
    }
    unmatched_451 -= {
        raw451_deduped.index[raw451_deduped["logical_record_id"] == match["451_logical_id"]][0]
        for match in res9_matches
    }
    summaries.append(
        summary_row(
            "3. after H3 res9 matching",
            len(dc_deduped),
            len(raw451_deduped),
            len(res9_matches),
        )
    )

    res8_candidates = h3_candidates(dc_deduped, raw451_deduped, unmatched_dc, unmatched_451, "h3_res8")
    res8_matches = choose_matches(
        res8_candidates, dc_deduped, raw451_deduped, "H3 res8", is_res8_match
    )
    unmatched_dc -= {
        dc_deduped.index[dc_deduped["logical_record_id"] == match["dc_byte_logical_id"]][0]
        for match in res8_matches
    }
    unmatched_451 -= {
        raw451_deduped.index[raw451_deduped["logical_record_id"] == match["451_logical_id"]][0]
        for match in res8_matches
    }
    summaries.append(
        summary_row(
            "4. after H3 res8 matching",
            len(dc_deduped),
            len(raw451_deduped),
            len(res9_matches) + len(res8_matches),
        )
    )

    manual_matches, manual_df = manual_review(dc_deduped, raw451_deduped, unmatched_dc, unmatched_451)
    matched_manual_dc = {
        dc_deduped.index[dc_deduped["logical_record_id"] == match["dc_byte_logical_id"]][0]
        for match in manual_matches
    }
    matched_manual_451 = {
        raw451_deduped.index[raw451_deduped["logical_record_id"] == match["451_logical_id"]][0]
        for match in manual_matches
    }
    unmatched_dc -= matched_manual_dc
    unmatched_451 -= matched_manual_451

    all_matches = pd.DataFrame(res9_matches + res8_matches + manual_matches)
    final_matched = len(all_matches)
    summaries.append(
        StageSummary(
            stage="5. after manual review",
            dc_byte_records=len(dc_deduped),
            raw451_records=len(raw451_deduped),
            matched_across_both=final_matched,
            unique_to_dc_byte=len(unmatched_dc),
            unique_to_451=len(unmatched_451),
            still_unmatched=0,
        )
    )

    summary_df = pd.DataFrame([summary.__dict__ for summary in summaries])

    unmatched_dc_df = dc_deduped.loc[sorted(unmatched_dc)].copy()
    unmatched_451_df = raw451_deduped.loc[sorted(unmatched_451)].copy()

    write_outputs(
        summary_df,
        dc_us,
        raw451_us,
        dc_deduped,
        raw451_deduped,
        all_matches,
        manual_df,
        unmatched_dc_df,
        unmatched_451_df,
    )

    print(summary_df.to_string(index=False))
    print(f"Wrote reconciliation tabs to {BASE_WORKBOOK}")


if __name__ == "__main__":
    main()
