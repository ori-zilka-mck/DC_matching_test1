# DC Byte vs 451 matching summary

Workbook: `Sanitized Reconsilation.xlsx`

## Source row counts

| Source | Rows |
|---|---:|
| 451 | 14,261 |
| DC Byte | 8,178 |
| Global data | 1,996 |

## DC Byte vs 451 association view

| View | Count |
|---|---:|
| DC Byte rows total | 8,178 |
| DC Byte rows associated with at least one 451 row | 5,525 |
| DC Byte rows only in DC Byte / unmatched | 2,653 |
| 451 rows total | 14,261 |
| 451 rows associated with at least one DC Byte row | 9,478 |
| 451 rows only in 451 / unmatched | 4,783 |
| Matched DC Byte-451 row pairs | 19,064 |

## Match stages

| Stage | Pair count | DC Byte rows touched | 451 rows touched |
|---|---:|---:|---:|
| 01_lat_long_2dp | 16,205 | 4,413 | 7,247 |
| 02_same_integer_lat_long_address | 938 | 620 | 780 |
| 03_name_city_similarity | 1,779 | 1,008 | 1,608 |
| 04_owner_name_city_similarity | 142 | 91 | 136 |

## Matching logic

1. `01_lat_long_2dp`: match every DC Byte row to every 451 row whose latitude and longitude round to the same two decimal places.
2. `02_same_integer_lat_long_address`: for remaining unassociated rows, look inside the same integer-degree latitude/longitude block and require high address similarity.
3. `03_name_city_similarity`: use strong data-center-name similarity within the same city/country or integer-degree coordinate block.
4. `04_owner_name_city_similarity`: use owner similarity only when city also matches and there is meaningful data-center-name overlap.

The matching is many-to-many: one DC Byte row can be associated with multiple 451 rows, and one 451 row can be associated with multiple DC Byte rows.

## Output files

- `matched_pairs.csv`: detailed pair-level matches and scores.
- `dc_byte_rows_with_matches.csv`: every DC Byte row with matched 451 rows.
- `451_rows_with_matches.csv`: every 451 row with matched DC Byte rows.
- `unmatched_dc_byte.csv`: DC Byte rows with no 451 association.
- `unmatched_451.csv`: 451 rows with no DC Byte association.
