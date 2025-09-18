---
title: Documentation
summary: Tomorrow Now GAP
  - Jeff Osundwa
date: 2025-09-16
some_url: https://github.com/kartoza/tomorrownow_gap.git
copyright: Copyright 2025, Kartoza
contact:
license: This program is free software; you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
---

# DCAS GeoParquet

The **Dynamic Crop Advisory System (DCAS)** dataset provides daily crop advice, growth-stage monitoring, and climate-related metrics.  
It is stored as partitioned **GeoParquet** files in the GAP bucket.

---

## Prerequisites

Python 3.9+  
[DuckDB](https://duckdb.org/) with extensions:

  - `httpfs` (S3/HTTP access)  
  - `spatial` (GEOMETRY support)  

Read-only GAP object storage credentials

### Environment variables

```bash
export S3_ENDPOINT_URL="https://fra1.digitaloceanspaces.com"
export S3_ACCESS_KEY_ID="YOUR_READ_ONLY_KEY"
export S3_SECRET_ACCESS_KEY="YOUR_READ_ONLY_SECRET"
export S3_BUCKET_NAME="bucket-name"
```

## Path layout

DCAS is partitioned by country (iso_a3), year, month, and day:

```bash
s3://$S3_BUCKET_NAME/staging/dcas_output/iso_a3=KEN/year=/month=/day=*/*.parquet
```

Example:

```bash
s3://gap-products/staging/dcas_output/iso_a3=KEN/year=2025/month=9/day=10/*.parquet
```

Always pass `hive_partitioning=true` to make iso_a3, year, month, and day available as columns.

## Column descriptions

The following columns are available in DCAS GeoParquet.  

| Column name                  | Type       | Description |
|------------------------------|-----------|-------------|
| `planting_date_epoch`        | UINTEGER  | Epoch timestamp of planting date for the farm. |
| `crop_id`                    | USMALLINT | Numeric identifier of the crop type. |
| `crop_stage_type_id`         | USMALLINT | Identifier for the type of growth stage. |
| `group_id`                   | UINTEGER  | Internal group identifier. |
| `farm_id`                    | BIGINT    | Internal numeric farm identifier. |
| `farm_unique_id`             | VARCHAR   | External farm identifier (used for joining/filtering). |
| `geometry`                   | GEOMETRY  | Spatial geometry of the farm/grid (polygon or point). |
| `grid_id`                    | UINTEGER  | Numeric identifier for grid cell. |
| `grid_unique_id`             | VARCHAR   | Unique identifier for grid cell. |
| `registry_id`                | BIGINT    | Identifier from farm registry. |
| `crop`                       | VARCHAR   | Crop name (e.g. “maize”). |
| `country_id`                 | UINTEGER  | Country identifier (numeric). |
| `grid_crop_key`              | VARCHAR   | Composite key linking grid and crop. |
| `county`                     | VARCHAR   | County name (administrative boundary). |
| `subcounty`                  | VARCHAR   | Subcounty name (administrative boundary). |
| `ward`                       | VARCHAR   | Ward name (administrative boundary). |
| `preferred_language`         | VARCHAR   | Preferred language for communication/advisories. |
| `date`                       | TIMESTAMP | Record timestamp. |
| `prev_growth_stage_id`       | USMALLINT | Previous growth stage identifier. |
| `prev_growth_stage_start_date` | UINTEGER | Epoch timestamp when previous growth stage started. |
| `config_id`                  | USMALLINT | Identifier for model/config version used. |
| `growth_stage_start_date`    | UINTEGER  | Epoch timestamp when current growth stage started. |
| `growth_stage_id`            | USMALLINT | Identifier for current growth stage. |
| `total_gdd`                  | DOUBLE    | Accumulated Growing Degree Days (GDD). |
| `seasonal_precipitation`     | DOUBLE    | Total precipitation recorded in current season (mm). |
| `temperature`                | DOUBLE    | Mean temperature for record period (°C). |
| `humidity`                   | DOUBLE    | Mean relative humidity (%) for record period. |
| `p_pet`                      | DOUBLE    | Precipitation / Potential Evapotranspiration ratio. |
| `growth_stage_precipitation` | DOUBLE    | Precipitation during current growth stage (mm). |
| `message`                    | UINTEGER  | Encoded advisory message ID. |
| `message_2`                  | UINTEGER  | Secondary message code (if available). |
| `message_3`                  | UINTEGER  | Tertiary message code. |
| `message_4`                  | UINTEGER  | Quaternary message code. |
| `message_5`                  | UINTEGER  | Quinary message code. |
| `is_empty_message`           | VARCHAR   | Flag if message is empty (text flag). |
| `has_repetitive_message`     | VARCHAR   | Flag if advisory is repetitive (text flag). |
| `final_message`              | UINTEGER  | Final consolidated advisory message ID. |
| `prev_week_message`          | UINTEGER  | Advisory message from the previous week. |
| `growth_stage`               | VARCHAR   | Human-readable crop growth stage name. |
| `__null_dask_index__`        | BIGINT    | Index column generated by Dask (ignore in most queries). |
| `day`                        | BIGINT    | Day partition (from file path). |
| `iso_a3`                     | VARCHAR   | ISO Alpha-3 country code (e.g. KEN). |
| `month`                      | BIGINT    | Month partition (from file path). |
| `year`                       | BIGINT    | Year partition (from file path). |

---

## Example queries

### Stage a subset

```python
import os, duckdb

conn = duckdb.connect()
conn.install_extension("httpfs"); conn.load_extension("httpfs")
conn.install_extension("spatial"); conn.load_extension("spatial")

dcas_path = (
  f"s3://{os.environ['S3_BUCKET_NAME']}/"
  "staging/dcas_output/iso_a3=KEN/year=*/month=*/day=*/*.parquet"
)

conn.sql(f"""
CREATE OR REPLACE TABLE dcas AS
SELECT *
FROM read_parquet('{dcas_path}', hive_partitioning=true)
WHERE year=2025 AND month=9 AND day=10
""")
```

### Quick checks

```sql
-- Count rows
SELECT COUNT(*) FROM dcas;

-- Schema overview
DESCRIBE dcas;
```

### Distinct messages

```sql
SELECT DISTINCT final_message FROM dcas;
```

### Messages distribution

```sql
SELECT final_message, COUNT(*) AS n
FROM dcas
GROUP BY final_message
ORDER BY n DESC;
```

### Per-farm history

```sql
SELECT date, farm_unique_id, crop, growth_stage, final_message
FROM dcas
WHERE farm_unique_id = '4023361'
ORDER BY date DESC
LIMIT 20;
```
