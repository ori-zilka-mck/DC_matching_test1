#!/usr/bin/env python3
"""Add a search-friendly company-name column to facility_owners_summary.xlsx.

The output is meant for human database searches: prefer the common company or
brand name when it is clear, otherwise remove legal suffixes while preserving
enough of the owner name to avoid broad/ambiguous searches.
"""

from __future__ import annotations

import csv
import re
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile


INPUT_FILE = Path("facility_owners_summary.xlsx")
OUTPUT_CSV = Path("facility_owners_summary_with_search_name.csv")
OUTPUT_XLSX = Path("facility_owners_summary_with_search_name.xlsx")
NEW_COLUMN = "Search Company Name"

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


ALIAS_RULES: list[tuple[str, str]] = [
    (r"\bdigital\s+realty\b|\bmc\s+digital\s+realty\b|\bdigital\s+core\s+reit\b", "Digital Realty"),
    (r"\bamazon(?:\.com)?\b", "Amazon"),
    (r"\bequinix\b", "Equinix"),
    (r"\bmicrosoft\b", "Microsoft"),
    (r"\bchina\s+telecom\b", "China Telecom"),
    (r"\bntt\s+global\s+data\s+centers?\b", "NTT Global Data Centers"),
    (r"\bntt\s+data\b", "NTT DATA"),
    (r"\bnippon\s+telegraph\s+and\s+telephone\s+east\b", "NTT East"),
    (r"\bnippon\s+telegraph\s+and\s+telephone\s+west\b", "NTT West"),
    (r"\bnippon\s+telegraph\s+and\s+telephone\b", "NTT"),
    (r"\balphabet\b|\bgoogle\b", "Google"),
    (r"\blumen\s+technologies\b|\blevel\s*3\b", "Lumen"),
    (r"\bchina\s+mobile\b", "China Mobile"),
    (r"\bchina\s+united\s+network\b|\bchina\s+unicom\b", "China Unicom"),
    (r"\bgds\s+holdings\b|\bgds\b", "GDS"),
    (r"\bmeta\s+platforms\b|\bfacebook\b", "Meta"),
    (r"\bcogent\s+communications\b", "Cogent Communications"),
    (r"\bvantage\s+data\s+centers?\b", "Vantage Data Centers"),
    (r"\bqts\b", "QTS"),
    (r"\bstack\s+infrastructure\b", "STACK Infrastructure"),
    (r"\bcyrusone\b", "CyrusOne"),
    (r"\bkddi\b", "KDDI"),
    (r"\bmapletree\s+industrial\s+trust\b", "Mapletree Industrial Trust"),
    (r"\bverizon\b", "Verizon"),
    (r"\bdatabank\b", "DataBank"),
    (r"\bedgeconnex\b", "EdgeConneX"),
    (r"\bvnet\b", "VNET"),
    (r"\bcolt\s+data\s+centre\s+services\b", "Colt DCS"),
    (r"\bcolt\s+technology\s+services\b", "Colt"),
    (r"\bcompass\s+datacenters?\b", "Compass Datacenters"),
    (r"\bexa\s+infrastructure\b", "EXA Infrastructure"),
    (r"\btencent\b", "Tencent"),
    (r"\biron\s+mountain\b", "Iron Mountain"),
    (r"\bcloudhq\b", "cloudHQ"),
    (r"\balibaba\b", "Alibaba"),
    (r"\bdata4\b", "Data4"),
    (r"\baligned\s+data\s+centers?\b", "Aligned Data Centers"),
    (r"\bcoresite\b", "CoreSite"),
    (r"\bglp\b", "GLP"),
    (r"\bglobal\s+switch\b", "Global Switch"),
    (r"\bcologix\b", "Cologix"),
    (r"\bsinnet\b", "Sinnet"),
    (r"\bflexential\b", "Flexential"),
    (r"\bkhazna\b", "Khazna"),
    (r"\bscala\b", "Scala Data Centers"),
    (r"\bcentersquare\b", "Centersquare"),
    (r"\b@hub\b|\bat\s*hub\b", "@hub"),
    (r"\bkeppel\b", "Keppel"),
    (r"\bnlighten\b", "nLighten"),
    (r"\bcrown\s+castle\b", "Crown Castle"),
    (r"^switch\s+datacenters?\b", "Switch Datacenters"),
    (r"^switch(?:\s+inc)?$", "Switch"),
    (r"\bascenty\b", "Ascenty"),
    (r"\bchindata\b", "Chindata"),
    (r"\batnorth\b", "atNorth"),
    (r"\bretelit\b", "Retelit"),
    (r"\bairtrunk\b", "AirTrunk"),
    (r"\bnextdc\b", "NEXTDC"),
    (r"\bapple\b", "Apple"),
    (r"\bprime\s+data\s+centers?\b", "Prime Data Centers"),
    (r"\borange\s+business\b", "Orange Business"),
    (r"\bprinceton\s+digital\s+group\b", "Princeton Digital Group"),
    (r"\batlasedge\b", "AtlasEdge"),
    (r"\blg\s+uplus\b", "LG Uplus"),
    (r"\btelstra\b", "Telstra"),
    (r"\btelia\b", "Telia"),
    (r"\bcirion\b", "Cirion"),
    (r"\bt5\s+data\s+centers?\b", "T5 Data Centers"),
    (r"\bbridge\s+data\s+centres?\b", "Bridge Data Centres"),
    (r"\bcopt\b", "COPT Defense Properties"),
    (r"\bgreen\s+mountain\b", "Green Mountain"),
    (r"\bst\s+telemedia\b", "ST Telemedia"),
    (r"\bstt\s+global\s+data\s+centres?\b", "STT GDC"),
    (r"\bsify\b", "Sify"),
    (r"\bh5\s+data\s+centers?\b", "H5 Data Centers"),
    (r"\beunetworks\b", "euNetworks"),
    (r"\bsoftbank\b", "SoftBank"),
    (r"\bpt\s+telekomunikasi\s+indonesia\b", "Telkom Indonesia"),
    (r"\btelkom\s+sa\b", "Telkom SA"),
    (r"\bcellnex\b", "Cellnex"),
    (r"\binternet\s+initiative\s+japan\b|\biij\b", "IIJ"),
    (r"\bnorthc\b", "NorthC"),
    (r"\bzayo\b", "Zayo"),
    (r"\bnxtra\b", "Nxtra"),
    (r"\bcrusoe\b", "Crusoe"),
    (r"\bbaidu\b", "Baidu"),
    (r"\brostelecom\b", "Rostelecom"),
    (r"\b365\s+data\s+centers?\b", "365 Data Centers"),
    (r"\bserverfarm\b", "Serverfarm"),
    (r"\bctrls\b", "CtrlS"),
    (r"\btata\s+communications\b", "Tata Communications"),
    (r"\bvirtus\b", "VIRTUS"),
    (r"\bnec\b", "NEC"),
    (r"\bcapitaland\b", "CapitaLand"),
    (r"\bcdc\s+data\s+centres?\b", "CDC Data Centres"),
    (r"\bsk\s+broadband\b", "SK Broadband"),
    (r"\baims\s+data\s+centre\b", "AIMS"),
    (r"\bedged\s+energy\b", "Edged Energy"),
    (r"\bvocus\b", "Vocus"),
    (r"\bedgecore\b", "EdgeCore"),
    (r"\bkaris\s+critical\b", "Karis Critical"),
    (r"\bnetrality\b", "Netrality"),
    (r"\bmara\s+holdings\b", "MARA"),
    (r"\bfujitsu\b", "Fujitsu"),
    (r"\bbulk\s+infrastructure\b", "Bulk Infrastructure"),
    (r"\bgulf\s+data\s+hub\b", "Gulf Data Hub"),
    (r"\bfifteenfortyseven\b|\b1547\b", "1547"),
    (r"\bpulsant\b", "Pulsant"),
    (r"\btm\s+one\b", "TM One"),
    (r"\bmtn\b", "MTN"),
    (r"\bark\s+data\s+centres?\b", "Ark Data Centres"),
    (r"\bdigital\s+edge\b", "Digital Edge"),
    (r"\blg\s+cns\b", "LG CNS"),
    (r"\bestructure\b", "eStruxture"),
    (r"\bcleanspark\b", "CleanSpark"),
    (r"\biadvantage\b", "iAdvantage"),
    (r"\bsegro\b", "SEGRO"),
    (r"\bdartpoints\b", "DartPoints"),
    (r"\bexpedient\b", "Expedient"),
    (r"\browan\s+digital\b", "Rowan Digital Infrastructure"),
    (r"\bspark\s+new\s+zealand\b", "Spark"),
    (r"\bpenta\s+infra\b", "Penta Infra"),
    (r"\bdc\s+blox\b", "DC BLOX"),
    (r"\bvodacom\b", "Vodacom"),
    (r"\brogers\s+communications\b", "Rogers"),
    (r"\byotta\b", "Yotta"),
    (r"\betix\s+everywhere\b", "Etix Everywhere"),
    (r"\bytl\b", "YTL"),
    (r"\bsungard\b", "Sungard Availability Services"),
    (r"\bwindstream\b", "Windstream"),
    (r"\bgoodman\b", "Goodman"),
    (r"\blightedge\b", "LightEdge"),
    (r"\bbitfarms\b", "Bitfarms"),
    (r"\bbeacon\s+ai\b", "Beacon AI Centers"),
    (r"\bgenesis\s+digital\s+assets\b", "Genesis Digital Assets"),
    (r"\byondr\b", "Yondr"),
    (r"\bhut\s*8\b", "Hut 8"),
    (r"\bnoovle\b", "Noovle"),
    (r"\batman\b", "Atman"),
    (r"\bbt\s+group\b", "BT"),
    (r"\bborealis\s+data\s+center\b", "Borealis Data Center"),
    (r"\bkt\s+cloud\b", "KT Cloud"),
    (r"\bchinacache\b", "ChinaCache"),
    (r"\bepldt\b", "ePLDT"),
    (r"\bnovva\b", "Novva"),
    (r"\bafrica\s+data\s+centres?\b", "Africa Data Centres"),
    (r"\bcentrilogic\b", "CentriLogic"),
    (r"\bavaio\b", "AVAIO Digital"),
    (r"\breliance\s+communications\b", "Reliance Communications"),
    (r"\bgtd\s+grupo\b", "GTD"),
    (r"\bedge\s+centres\b", "Edge Centres"),
    (r"\belea\s+digital\b", "Elea Digital"),
    (r"\bat\s+tokyo\b", "AT TOKYO"),
    (r"\bevocative\b", "Evocative"),
    (r"\betihad\s+etisalat\b", "Mobily"),
    (r"\beurofiber\b", "Eurofiber"),
    (r"\bapplied\s+digital\b", "Applied Digital"),
    (r"\bblackstone\b", "Blackstone"),
    (r"\bnorthern\s+data\b", "Northern Data"),
    (r"\boneneck\b", "OneNeck"),
    (r"\bdci\s+indonesia\b", "DCI Indonesia"),
    (r"\btelecom\s+italia\s+sparkle\b", "Sparkle"),
    (r"\btelecom\s+italia\b", "Telecom Italia"),
    (r"\bcore\s+scientific\b", "Core Scientific"),
    (r"\bhetzner\b", "Hetzner"),
    (r"\bgreen\s+datacenter\b", "Green Datacenter"),
    (r"\badaniconnex\b", "AdaniConneX"),
    (r"\bsabey\s+data\s+center", "Sabey Data Centers"),
    (r"\bstream\s+data\s+centers?", "Stream Data Centers"),
    (r"\bmegafon\b", "MegaFon"),
    (r"\bmobile\s+telesystems\b", "MTS"),
    (r"\bsoftware\s+technology\s+park\s+of\s+india\b", "STPI"),
    (r"\bitenos\b|i\s*t\s*e\s*n\s*o\s*s", "ITENOS"),
    (r"\binternational\s+business\s+machines\b|\bibm\b", "IBM"),
    (r"\bamerica\s+movil\b", "America Movil"),
    (r"\bvodafone\b", "Vodafone"),
    (r"\booredoo\b", "Ooredoo"),
    (r"\baruba\b", "Aruba"),
    (r"\bverne\s+global\b", "Verne Global"),
    (r"\biomart\b", "iomart"),
    (r"\bnebius\b", "Nebius"),
    (r"\braxio\b", "Raxio"),
    (r"\bclaranet\b", "Claranet"),
    (r"\bsociete\s+francaise\s+de\s+radiotelephone\b|\bsfr\b", "SFR"),
    (r"\bdws\b", "DWS"),
    (r"\bpure\s+data\s+centres?\b", "Pure Data Centres"),
]


LEGAL_SUFFIX_PATTERNS = [
    r"incorporated",
    r"inc",
    r"corporation",
    r"corp",
    r"management\s+company",
    r"company",
    r"co",
    r"limited",
    r"ltd",
    r"llc",
    r"l\s*\.?\s*l\s*\.?\s*c",
    r"l\s*l\s*c",
    r"llp",
    r"lp",
    r"l\s*\.?\s*p",
    r"plc",
    r"holdings?",
    r"trust",
    r"reit",
    r"preparatory\s+corporation",
    r"public\s+company\s+limited",
    r"private\s+limited",
    r"pte\s+limited",
    r"pte\s+ltd",
    r"pte",
    r"pty",
    r"berhad",
    r"sdn\s+bhd",
    r"sa\s+de\s+cv",
    r"s\s*\.?\s*a\s*\.?\s+de\s+c\s*\.?\s*v",
    r"de\s+cv",
    r"de\s+c\s*\.?\s*v",
    r"s\s*a",
    r"s\s*\.?\s*a",
    r"s\s*a\s*s",
    r"s\s*\.?\s*a\s*\.?\s*s",
    r"s\s*a\s+r\s*l",
    r"sarl",
    r"sas",
    r"sac",
    r"s\s*\.?\s*a\s*\.?\s*c",
    r"s\s*p\s*a",
    r"s\s*\.?\s*p\s*\.?\s*a",
    r"spa",
    r"srl",
    r"s\s*\.?\s*r\s*\.?\s*l",
    r"gmbh",
    r"g\s*\.?\s*m\s*\.?\s*b\s*\.?\s*h",
    r"ag",
    r"a\s*g",
    r"bv",
    r"b\s*\.?\s*v",
    r"b\s*v",
    r"nv",
    r"n\s*\.?\s*v",
    r"n\s*v",
    r"ab",
    r"a\s*\.?\s*b",
    r"as",
    r"a\s*\.?\s*s",
    r"a\s*s",
    r"asa",
    r"oy",
    r"aps",
    r"ehf",
    r"kk",
    r"k\s*\.?\s*k",
    r"k\s*k",
    r"tbk",
    r"sp\s+z\s+o\s+o",
    r"sp\s*\.?\s*z\s*\.?\s*o\s*\.?\s*o",
    r"oao",
    r"doo",
    r"d\s*\.?\s*o\s*\.?\s*o",
    r"pjsc",
    r"jsc",
    r"j\s*\.?\s*s\s*\.?\s*c",
    r"ooo",
    r"o\s*\.?\s*o\s*\.?\s*o",
]

LEGAL_SUFFIX_RE = re.compile(
    r"(?:[\s,.\-]+(?:" + "|".join(LEGAL_SUFFIX_PATTERNS) + r"))+$",
    re.IGNORECASE,
)


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def column_index(cell_ref: str) -> int:
    letters = re.match(r"[A-Z]+", cell_ref).group(0)
    index = 0
    for char in letters:
        index = index * 26 + ord(char) - 64
    return index - 1


def column_letter(index: int) -> str:
    index += 1
    letters = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def read_xlsx(path: Path) -> list[list[str]]:
    with ZipFile(path) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall(f"{{{MAIN_NS}}}si"):
                shared_strings.append(
                    "".join(t.text or "" for t in si.iter() if local_name(t.tag) == "t")
                )

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rid_to_target = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        sheet = workbook.find(f".//{{{MAIN_NS}}}sheet")
        if sheet is None:
            raise ValueError("Workbook has no worksheets")

        target = rid_to_target[sheet.attrib[f"{{{REL_NS}}}id"]].lstrip("/")
        if not target.startswith("xl/"):
            target = f"xl/{target}"
        root = ET.fromstring(zf.read(target))

        def cell_value(cell: ET.Element) -> str:
            cell_type = cell.attrib.get("t")
            value = cell.find(f"{{{MAIN_NS}}}v")
            if cell_type == "s":
                if value is None or value.text is None:
                    return ""
                return shared_strings[int(value.text)]
            if cell_type == "inlineStr":
                return "".join(t.text or "" for t in cell.iter() if local_name(t.tag) == "t")
            return "" if value is None or value.text is None else value.text

        rows: list[list[str]] = []
        for row in root.findall(f".//{{{MAIN_NS}}}sheetData/{{{MAIN_NS}}}row"):
            values: dict[int, str] = {}
            for cell in row.findall(f"{{{MAIN_NS}}}c"):
                values[column_index(cell.attrib["r"])] = cell_value(cell)
            if values:
                rows.append([values.get(i, "") for i in range(max(values) + 1)])
        return rows


def normalize_for_matching(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower().replace("&", " and ")
    value = re.sub(r"[^a-z0-9@]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def cleanup_owner_name(owner: str) -> str:
    name = owner.strip()
    dash_parts = re.split(r"\s+-\s+", name, maxsplit=1)
    if len(dash_parts) == 2 and not re.search(r"[A-Za-z]", dash_parts[1]):
        name = dash_parts[0]
    name = re.sub(r"\([^)]*\)", " ", name)
    name = name.replace("&", " and ")
    name = re.sub(r"[\"']", "", name)
    name = re.sub(r"\s+", " ", name).strip(" ,.;:-")

    previous = None
    while previous != name and name:
        previous = name
        name = LEGAL_SUFFIX_RE.sub("", name).strip(" ,.;:-")
        name = re.sub(r"\s+", " ", name)

    return name or owner.strip()


def search_company_name(owner: str) -> str:
    normalized = normalize_for_matching(owner)
    for pattern, alias in ALIAS_RULES:
        if re.search(pattern, normalized, flags=re.IGNORECASE):
            return alias
    return cleanup_owner_name(owner)


def write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def write_xlsx(path: Path, rows: list[list[str]]) -> None:
    shared_index: dict[str, int] = {}
    shared_values: list[str] = []

    def shared_id(value: str) -> int:
        if value not in shared_index:
            shared_index[value] = len(shared_values)
            shared_values.append(value)
        return shared_index[value]

    sheet_rows: list[str] = []
    for row_idx, row in enumerate(rows, start=1):
        cells: list[str] = []
        for col_idx, value in enumerate(row):
            ref = f"{column_letter(col_idx)}{row_idx}"
            cells.append(f'<c r="{ref}" t="s"><v>{shared_id(str(value))}</v></c>')
        sheet_rows.append(f'<row r="{row_idx}">{"".join(cells)}</row>')

    last_ref = f"{column_letter(len(rows[0]) - 1)}{len(rows)}"
    worksheet = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<worksheet xmlns="{MAIN_NS}" '
        f'xmlns:r="{REL_NS}">'
        f'<dimension ref="A1:{last_ref}"/>'
        "<sheetViews><sheetView workbookViewId=\"0\"/></sheetViews>"
        "<sheetFormatPr defaultRowHeight=\"15\"/>"
        f"<sheetData>{''.join(sheet_rows)}</sheetData>"
        "</worksheet>"
    )

    shared_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<sst xmlns="{MAIN_NS}" count="{sum(len(row) for row in rows)}" '
        f'uniqueCount="{len(shared_values)}">'
        + "".join(f"<si><t>{escape(value)}</t></si>" for value in shared_values)
        + "</sst>"
    )

    with ZipFile(path, "w", ZIP_DEFLATED) as zf:
        zf.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            '<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
            '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
            "</Types>",
        )
        zf.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            "</Relationships>",
        )
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<workbook xmlns="{MAIN_NS}" xmlns:r="{REL_NS}">'
            "<sheets><sheet name=\"facility_owners_summary\" sheetId=\"1\" r:id=\"rId1\"/></sheets>"
            "</workbook>",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>'
            '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
            "</Relationships>",
        )
        zf.writestr("xl/worksheets/sheet1.xml", worksheet)
        zf.writestr("xl/sharedStrings.xml", shared_xml)
        zf.writestr(
            "xl/styles.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<styleSheet xmlns="{MAIN_NS}">'
            "<fonts count=\"1\"><font><sz val=\"11\"/><name val=\"Calibri\"/></font></fonts>"
            "<fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills>"
            "<borders count=\"1\"><border/></borders>"
            "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>"
            "<cellXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/></cellXfs>"
            "</styleSheet>",
        )


def main() -> None:
    rows = read_xlsx(INPUT_FILE)
    if not rows:
        raise SystemExit(f"{INPUT_FILE} is empty")

    header = rows[0] + [NEW_COLUMN]
    facility_owner_idx = rows[0].index("Facility Owner")
    output_rows = [header]
    for row in rows[1:]:
        padded = row + [""] * (len(rows[0]) - len(row))
        owner = padded[facility_owner_idx]
        output_rows.append(padded[: len(rows[0])] + [search_company_name(owner)])

    write_csv(OUTPUT_CSV, output_rows)
    write_xlsx(OUTPUT_XLSX, output_rows)
    print(f"Wrote {len(output_rows) - 1:,} rows to {OUTPUT_CSV} and {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
