#!/usr/bin/env python3
"""Match DC Byte and 451 data centers in the reconciliation workbook.

The script intentionally uses only the Python standard library so it can run in
minimal cloud environments without installing Excel or fuzzy-match packages.
"""

from __future__ import annotations

import csv
import math
import re
import unicodedata
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from zipfile import ZipFile


WORKBOOK = Path("Sanitized Reconsilation.xlsx")
OUTPUT_DIR = Path("matching_output")

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "center",
    "centre",
    "cloud",
    "colocation",
    "colo",
    "data",
    "dc",
    "facility",
    "for",
    "global",
    "in",
    "inc",
    "llc",
    "ltd",
    "of",
    "plc",
    "pty",
    "sa",
    "site",
    "the",
}


@dataclass(frozen=True)
class Row:
    idx: int
    excel_row: int
    owner: str
    name: str
    country: str
    city: str
    address: str
    lat: float | None
    lon: float | None
    source: str

    @property
    def latlon_2dp(self) -> tuple[str, str] | None:
        if self.lat is None or self.lon is None:
            return None
        return (f"{self.lat:.2f}", f"{self.lon:.2f}")

    @property
    def latlon_int(self) -> tuple[int, int] | None:
        if self.lat is None or self.lon is None:
            return None
        return (int(self.lat), int(self.lon))


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _col_idx(cell_ref: str) -> int:
    letters = re.match(r"[A-Z]+", cell_ref).group(0)
    idx = 0
    for ch in letters:
        idx = idx * 26 + ord(ch) - 64
    return idx - 1


def _read_xlsx_rows(path: Path) -> list[list[str]]:
    with ZipFile(path) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            shared_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in shared_root.findall(f"{{{MAIN_NS}}}si"):
                shared_strings.append(
                    "".join(t.text or "" for t in si.iter() if _local_name(t.tag) == "t")
                )

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rid_to_target = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        first_sheet = workbook.find(f".//{{{MAIN_NS}}}sheet")
        if first_sheet is None:
            raise ValueError("Workbook has no sheets")

        rid = first_sheet.attrib[f"{{{REL_NS}}}id"]
        target = rid_to_target[rid].lstrip("/")
        if not target.startswith("xl/"):
            target = f"xl/{target}"

        sheet_root = ET.fromstring(zf.read(target))

        def cell_value(cell: ET.Element) -> str:
            cell_type = cell.attrib.get("t")
            value = cell.find(f"{{{MAIN_NS}}}v")
            if cell_type == "s":
                if value is None or value.text is None:
                    return ""
                return shared_strings[int(value.text)]
            if cell_type == "inlineStr":
                return "".join(t.text or "" for t in cell.iter() if _local_name(t.tag) == "t")
            return "" if value is None or value.text is None else value.text

        rows: list[list[str]] = []
        for row in sheet_root.findall(f".//{{{MAIN_NS}}}sheetData/{{{MAIN_NS}}}row"):
            values: dict[int, str] = {}
            for cell in row.findall(f"{{{MAIN_NS}}}c"):
                values[_col_idx(cell.attrib["r"])] = cell_value(cell)
            if values:
                rows.append([values.get(i, "") for i in range(max(values) + 1)])
        return rows


def _to_float(value: str) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _norm(value: str, *, drop_stopwords: bool = False) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    tokens = [token for token in value.split() if token]
    if drop_stopwords:
        tokens = [token for token in tokens if token not in STOPWORDS]
    return " ".join(tokens)


def _tokens(value: str, *, drop_stopwords: bool = False) -> set[str]:
    return set(_norm(value, drop_stopwords=drop_stopwords).split())


def _ratio(a: str, b: str) -> float:
    a = a.strip()
    b = b.strip()
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def _token_set_ratio(a: str, b: str, *, drop_stopwords: bool = False) -> float:
    ta = _tokens(a, drop_stopwords=drop_stopwords)
    tb = _tokens(b, drop_stopwords=drop_stopwords)
    if not ta or not tb:
        return 0.0
    common = sorted(ta & tb)
    left = sorted(ta - tb)
    right = sorted(tb - ta)
    common_text = " ".join(common)
    left_text = " ".join(common + left)
    right_text = " ".join(common + right)
    return max(
        _ratio(left_text, right_text),
        _ratio(common_text, left_text),
        _ratio(common_text, right_text),
    )


def text_score(a: str, b: str, *, drop_stopwords: bool = False) -> float:
    return max(
        _ratio(_norm(a, drop_stopwords=drop_stopwords), _norm(b, drop_stopwords=drop_stopwords)),
        _token_set_ratio(a, b, drop_stopwords=drop_stopwords),
    )


def same_country(a: Row, b: Row) -> bool:
    return _norm(a.country) == _norm(b.country) and bool(_norm(a.country))


def exact_city(a: Row, b: Row) -> bool:
    return _norm(a.city) == _norm(b.city) and bool(_norm(a.city))


def combined_score(dc: Row, other: Row) -> tuple[float, float, float, float]:
    address = text_score(dc.address, other.address)
    name = text_score(dc.name, other.name, drop_stopwords=True)
    owner = text_score(dc.owner, other.owner, drop_stopwords=True)
    city = text_score(dc.city, other.city)
    return address, name, owner, city


def match_rows(rows: list[Row]) -> tuple[list[dict[str, object]], dict[int, set[int]], dict[int, set[int]]]:
    dc_rows = [row for row in rows if row.source == "DC Byte"]
    rows_451 = [row for row in rows if row.source == "451"]
    by_2dp_451: dict[tuple[str, str], list[Row]] = defaultdict(list)
    by_int_451: dict[tuple[int, int], list[Row]] = defaultdict(list)
    by_city_451: dict[tuple[str, str], list[Row]] = defaultdict(list)

    for row in rows_451:
        if row.latlon_2dp is not None:
            by_2dp_451[row.latlon_2dp].append(row)
        if row.latlon_int is not None:
            by_int_451[row.latlon_int].append(row)
        country_city = (_norm(row.country), _norm(row.city))
        if all(country_city):
            by_city_451[country_city].append(row)

    matches: dict[tuple[int, int], dict[str, object]] = {}
    dc_matched: dict[int, set[int]] = defaultdict(set)
    row_451_matched: dict[int, set[int]] = defaultdict(set)

    def add_match(dc: Row, other: Row, method: str, score: float, reason: str) -> None:
        key = (dc.idx, other.idx)
        if key in matches:
            return
        address, name, owner, city = combined_score(dc, other)
        matches[key] = {
            "method": method,
            "score": round(score, 4),
            "reason": reason,
            "address_score": round(address, 4),
            "name_score": round(name, 4),
            "owner_score": round(owner, 4),
            "city_score": round(city, 4),
            "dc_excel_row": dc.excel_row,
            "dc_owner": dc.owner,
            "dc_name": dc.name,
            "dc_country": dc.country,
            "dc_city": dc.city,
            "dc_address": dc.address,
            "dc_lat": dc.lat,
            "dc_long": dc.lon,
            "451_excel_row": other.excel_row,
            "451_owner": other.owner,
            "451_name": other.name,
            "451_country": other.country,
            "451_city": other.city,
            "451_address": other.address,
            "451_lat": other.lat,
            "451_long": other.lon,
        }
        dc_matched[dc.idx].add(other.idx)
        row_451_matched[other.idx].add(dc.idx)

    # Stage 1: exact equality after rounding latitude and longitude to two decimals.
    for dc in dc_rows:
        if dc.latlon_2dp is None:
            continue
        for other in by_2dp_451.get(dc.latlon_2dp, []):
            add_match(
                dc,
                other,
                "01_lat_long_2dp",
                1.0,
                f"Rounded lat/long key {dc.latlon_2dp[0]}, {dc.latlon_2dp[1]}",
            )

    def pair_needs_work(dc: Row, other: Row) -> bool:
        return dc.idx not in dc_matched or other.idx not in row_451_matched

    # Stage 2: same integer-degree lat/long block, then high address similarity.
    for dc in dc_rows:
        if dc.latlon_int is None:
            continue
        for other in by_int_451.get(dc.latlon_int, []):
            if not pair_needs_work(dc, other):
                continue
            address, name, owner, city = combined_score(dc, other)
            if address >= 0.88 or (address >= 0.80 and city >= 0.80 and name >= 0.35):
                add_match(
                    dc,
                    other,
                    "02_same_integer_lat_long_address",
                    address,
                    f"Same integer lat/long {dc.latlon_int}; address similarity {address:.2f}",
                )

    # Stage 3: strong data-center-name similarity inside the same city/country or integer-degree block.
    for dc in dc_rows:
        candidates: dict[int, Row] = {}
        country_city = (_norm(dc.country), _norm(dc.city))
        if all(country_city):
            candidates.update({row.idx: row for row in by_city_451.get(country_city, [])})
        if dc.latlon_int is not None:
            candidates.update({row.idx: row for row in by_int_451.get(dc.latlon_int, [])})

        for other in candidates.values():
            if not pair_needs_work(dc, other):
                continue
            if not same_country(dc, other):
                continue
            address, name, owner, city = combined_score(dc, other)
            if name >= 0.90 and city >= 0.82:
                add_match(
                    dc,
                    other,
                    "03_name_city_similarity",
                    name,
                    f"Name similarity {name:.2f}; city similarity {city:.2f}",
                )
            elif name >= 0.84 and city >= 0.90 and owner >= 0.45:
                add_match(
                    dc,
                    other,
                    "03_name_city_similarity",
                    (name + owner) / 2,
                    f"Name similarity {name:.2f}; owner similarity {owner:.2f}; city similarity {city:.2f}",
                )

    # Stage 4: owner similarity, but only when city also lines up and names have some overlap.
    for dc in dc_rows:
        country_city = (_norm(dc.country), _norm(dc.city))
        if not all(country_city):
            continue
        for other in by_city_451.get(country_city, []):
            if not pair_needs_work(dc, other):
                continue
            address, name, owner, city = combined_score(dc, other)
            same_int = dc.latlon_int is not None and dc.latlon_int == other.latlon_int
            if owner >= 0.90 and city >= 0.90 and name >= 0.55:
                add_match(
                    dc,
                    other,
                    "04_owner_name_city_similarity",
                    (owner + name + city) / 3,
                    f"Owner similarity {owner:.2f}; name similarity {name:.2f}; city similarity {city:.2f}",
                )
            elif same_int and owner >= 0.84 and city >= 0.90 and name >= 0.45:
                add_match(
                    dc,
                    other,
                    "04_owner_name_city_similarity",
                    (owner + name + city) / 3,
                    f"Same integer lat/long; owner similarity {owner:.2f}; name similarity {name:.2f}",
                )

    return list(matches.values()), dc_matched, row_451_matched


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def row_summary(row: Row, matches_by_source_idx: dict[int, set[int]], all_rows: dict[int, Row]) -> dict[str, object]:
    matched_ids = sorted(matches_by_source_idx.get(row.idx, set()))
    matched_rows = [all_rows[idx] for idx in matched_ids]
    return {
        "excel_row": row.excel_row,
        "source": row.source,
        "owner": row.owner,
        "dc_name": row.name,
        "country": row.country,
        "city": row.city,
        "address": row.address,
        "lat": row.lat,
        "long": row.lon,
        "match_count": len(matched_rows),
        "matched_excel_rows": "; ".join(str(m.excel_row) for m in matched_rows),
        "matched_names": "; ".join(m.name for m in matched_rows),
        "matched_owners": "; ".join(m.owner for m in matched_rows),
    }


def main() -> None:
    raw_rows = _read_xlsx_rows(WORKBOOK)
    if not raw_rows:
        raise SystemExit("Workbook is empty")

    rows: list[Row] = []
    for idx, raw in enumerate(raw_rows[1:], start=1):
        padded = raw + [""] * 9
        rows.append(
            Row(
                idx=idx,
                excel_row=idx + 1,
                owner=padded[0],
                name=padded[1],
                country=padded[2],
                city=padded[3],
                address=padded[4],
                lat=_to_float(padded[5]),
                lon=_to_float(padded[6]),
                source=padded[8],
            )
        )

    source_counts = Counter(row.source for row in rows)
    matches, dc_matched, row_451_matched = match_rows(rows)
    matches.sort(key=lambda item: (str(item["method"]), int(item["dc_excel_row"]), int(item["451_excel_row"])))

    all_rows = {row.idx: row for row in rows}
    dc_rows = [row for row in rows if row.source == "DC Byte"]
    rows_451 = [row for row in rows if row.source == "451"]
    matched_dc = {row.idx for row in dc_rows if row.idx in dc_matched}
    matched_451 = {row.idx for row in rows_451 if row.idx in row_451_matched}
    unmatched_dc = [row for row in dc_rows if row.idx not in matched_dc]
    unmatched_451 = [row for row in rows_451 if row.idx not in matched_451]

    OUTPUT_DIR.mkdir(exist_ok=True)
    match_fields = [
        "method",
        "score",
        "reason",
        "address_score",
        "name_score",
        "owner_score",
        "city_score",
        "dc_excel_row",
        "dc_owner",
        "dc_name",
        "dc_country",
        "dc_city",
        "dc_address",
        "dc_lat",
        "dc_long",
        "451_excel_row",
        "451_owner",
        "451_name",
        "451_country",
        "451_city",
        "451_address",
        "451_lat",
        "451_long",
    ]
    write_csv(OUTPUT_DIR / "matched_pairs.csv", matches, match_fields)

    summary_fields = [
        "excel_row",
        "source",
        "owner",
        "dc_name",
        "country",
        "city",
        "address",
        "lat",
        "long",
        "match_count",
        "matched_excel_rows",
        "matched_names",
        "matched_owners",
    ]
    write_csv(
        OUTPUT_DIR / "dc_byte_rows_with_matches.csv",
        [row_summary(row, dc_matched, all_rows) for row in dc_rows],
        summary_fields,
    )
    write_csv(
        OUTPUT_DIR / "451_rows_with_matches.csv",
        [row_summary(row, row_451_matched, all_rows) for row in rows_451],
        summary_fields,
    )
    write_csv(
        OUTPUT_DIR / "unmatched_dc_byte.csv",
        [row_summary(row, dc_matched, all_rows) for row in unmatched_dc],
        summary_fields,
    )
    write_csv(
        OUTPUT_DIR / "unmatched_451.csv",
        [row_summary(row, row_451_matched, all_rows) for row in unmatched_451],
        summary_fields,
    )

    method_counts = Counter(str(match["method"]) for match in matches)
    matched_dc_by_method: dict[str, set[int]] = defaultdict(set)
    matched_451_by_method: dict[str, set[int]] = defaultdict(set)
    for match in matches:
        method = str(match["method"])
        matched_dc_by_method[method].add(int(match["dc_excel_row"]))
        matched_451_by_method[method].add(int(match["451_excel_row"]))

    summary_lines = [
        "# DC Byte vs 451 matching summary",
        "",
        f"Workbook: `{WORKBOOK.name}`",
        "",
        "## Source row counts",
        "",
        "| Source | Rows |",
        "|---|---:|",
    ]
    for source, count in source_counts.most_common():
        summary_lines.append(f"| {source or '(blank)'} | {count:,} |")

    summary_lines.extend(
        [
            "",
            "## DC Byte vs 451 association view",
            "",
            "| View | Count |",
            "|---|---:|",
            f"| DC Byte rows total | {len(dc_rows):,} |",
            f"| DC Byte rows associated with at least one 451 row | {len(matched_dc):,} |",
            f"| DC Byte rows only in DC Byte / unmatched | {len(unmatched_dc):,} |",
            f"| 451 rows total | {len(rows_451):,} |",
            f"| 451 rows associated with at least one DC Byte row | {len(matched_451):,} |",
            f"| 451 rows only in 451 / unmatched | {len(unmatched_451):,} |",
            f"| Matched DC Byte-451 row pairs | {len(matches):,} |",
            "",
            "## Match stages",
            "",
            "| Stage | Pair count | DC Byte rows touched | 451 rows touched |",
            "|---|---:|---:|---:|",
        ]
    )
    for method in sorted(method_counts):
        summary_lines.append(
            f"| {method} | {method_counts[method]:,} | "
            f"{len(matched_dc_by_method[method]):,} | {len(matched_451_by_method[method]):,} |"
        )

    summary_lines.extend(
        [
            "",
            "## Matching logic",
            "",
            "1. `01_lat_long_2dp`: match every DC Byte row to every 451 row whose latitude and longitude round to the same two decimal places.",
            "2. `02_same_integer_lat_long_address`: for remaining unassociated rows, look inside the same integer-degree latitude/longitude block and require high address similarity.",
            "3. `03_name_city_similarity`: use strong data-center-name similarity within the same city/country or integer-degree coordinate block.",
            "4. `04_owner_name_city_similarity`: use owner similarity only when city also matches and there is meaningful data-center-name overlap.",
            "",
            "The matching is many-to-many: one DC Byte row can be associated with multiple 451 rows, and one 451 row can be associated with multiple DC Byte rows.",
            "",
            "## Output files",
            "",
            "- `matched_pairs.csv`: detailed pair-level matches and scores.",
            "- `dc_byte_rows_with_matches.csv`: every DC Byte row with matched 451 rows.",
            "- `451_rows_with_matches.csv`: every 451 row with matched DC Byte rows.",
            "- `unmatched_dc_byte.csv`: DC Byte rows with no 451 association.",
            "- `unmatched_451.csv`: 451 rows with no DC Byte association.",
        ]
    )
    (OUTPUT_DIR / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print("\n".join(summary_lines[:35]))
    print(f"\nWrote outputs to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
