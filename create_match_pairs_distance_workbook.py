#!/usr/bin/env python3
"""Create a match_pairs-only workbook with coordinate distances.

The repository intentionally avoids third-party Excel packages, so this script
emits the minimal XLSX parts directly with the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import math
from datetime import UTC, datetime
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile


INPUT_CSV = Path("matching_output/matched_pairs.csv")
OUTPUT_XLSX = Path("matching_output/match_pairs_with_distances.xlsx")
SHEET_NAME = "match_pairs"
EARTH_RADIUS_METERS = 6_371_008.8
FEET_PER_METER = 3.280839895

NUMERIC_COLUMNS = {
    "score",
    "address_score",
    "name_score",
    "owner_score",
    "city_score",
    "dc_excel_row",
    "dc_lat",
    "dc_long",
    "451_excel_row",
    "451_lat",
    "451_long",
    "distance_meters",
    "distance_feet",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create an XLSX workbook containing only the match_pairs sheet."
    )
    parser.add_argument("--input", type=Path, default=INPUT_CSV, help="Input matched_pairs CSV")
    parser.add_argument("--output", type=Path, default=OUTPUT_XLSX, help="Output XLSX path")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it already exists",
    )
    return parser.parse_args()


def to_float(value: object) -> float | None:
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def haversine_meters(
    lat1: float | None,
    lon1: float | None,
    lat2: float | None,
    lon2: float | None,
) -> float | None:
    if None in (lat1, lon1, lat2, lon2):
        return None

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_METERS * c


def add_distances(row: dict[str, str]) -> dict[str, str | float]:
    distance_meters = haversine_meters(
        to_float(row.get("dc_lat")),
        to_float(row.get("dc_long")),
        to_float(row.get("451_lat")),
        to_float(row.get("451_long")),
    )
    enriched: dict[str, str | float] = dict(row)
    if distance_meters is None:
        enriched["distance_meters"] = ""
        enriched["distance_feet"] = ""
    else:
        enriched["distance_meters"] = round(distance_meters, 3)
        enriched["distance_feet"] = round(distance_meters * FEET_PER_METER, 3)
    return enriched


def excel_col_name(index: int) -> str:
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def numeric_text(value: object) -> str | None:
    parsed = to_float(value)
    if parsed is None:
        return None
    return f"{parsed:.12g}"


def sheet_xml(rows: list[dict[str, str | float]], headers: list[str]) -> str:
    max_col = excel_col_name(len(headers))
    max_row = len(rows) + 1
    parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
        f'<dimension ref="A1:{max_col}{max_row}"/>',
        '<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" '
        'activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>',
        "<sheetData>",
    ]

    parts.append('<row r="1">')
    for col_index, header in enumerate(headers, start=1):
        cell_ref = f"{excel_col_name(col_index)}1"
        parts.append(
            f'<c r="{cell_ref}" t="inlineStr" s="1"><is><t>{escape(header)}</t></is></c>'
        )
    parts.append("</row>")

    for row_index, row in enumerate(rows, start=2):
        parts.append(f'<row r="{row_index}">')
        for col_index, header in enumerate(headers, start=1):
            value = row.get(header, "")
            if value == "":
                continue
            cell_ref = f"{excel_col_name(col_index)}{row_index}"
            if header in NUMERIC_COLUMNS:
                number = numeric_text(value)
                if number is not None:
                    parts.append(f'<c r="{cell_ref}"><v>{number}</v></c>')
                    continue
            text = escape(str(value))
            parts.append(f'<c r="{cell_ref}" t="inlineStr"><is><t>{text}</t></is></c>')
        parts.append("</row>")

    parts.extend(
        [
            "</sheetData>",
            f'<autoFilter ref="A1:{max_col}{max_row}"/>',
            "</worksheet>",
        ]
    )
    return "".join(parts)


def write_xlsx(path: Path, rows: list[dict[str, str | float]], headers: list[str]) -> None:
    created_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    sheet = sheet_xml(rows, headers)

    path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(path, "w", ZIP_DEFLATED) as zf:
        zf.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
            '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
            "</Types>",
        )
        zf.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
            '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
            "</Relationships>",
        )
        zf.writestr(
            "docProps/core.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
            'xmlns:dc="http://purl.org/dc/elements/1.1/" '
            'xmlns:dcterms="http://purl.org/dc/terms/" '
            'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
            "<dc:creator>Cursor</dc:creator>"
            f"<dcterms:created xsi:type=\"dcterms:W3CDTF\">{created_at}</dcterms:created>"
            f"<dcterms:modified xsi:type=\"dcterms:W3CDTF\">{created_at}</dcterms:modified>"
            "</cp:coreProperties>",
        )
        zf.writestr(
            "docProps/app.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
            'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
            "<Application>Python</Application>"
            "</Properties>",
        )
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            "<sheets>"
            f'<sheet name="{SHEET_NAME}" sheetId="1" r:id="rId1"/>'
            "</sheets>"
            "</workbook>",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
            "</Relationships>",
        )
        zf.writestr(
            "xl/styles.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            "<fonts count=\"2\"><font><sz val=\"11\"/><name val=\"Calibri\"/></font>"
            "<font><b/><sz val=\"11\"/><name val=\"Calibri\"/></font></fonts>"
            "<fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills>"
            "<borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>"
            "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>"
            "<cellXfs count=\"2\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/>"
            "<xf numFmtId=\"0\" fontId=\"1\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyFont=\"1\"/></cellXfs>"
            "</styleSheet>",
        )
        zf.writestr("xl/worksheets/sheet1.xml", sheet)


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise SystemExit(f"Input CSV not found: {args.input}")
    if args.output.exists() and not args.force:
        raise SystemExit(f"Output already exists; use --force to overwrite: {args.output}")

    with args.input.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise SystemExit(f"Input CSV has no header: {args.input}")
        headers = [*reader.fieldnames, "distance_meters", "distance_feet"]
        rows = [add_distances(row) for row in reader]

    write_xlsx(args.output, rows, headers)
    print(f"Wrote {len(rows):,} rows to {args.output}")


if __name__ == "__main__":
    main()
