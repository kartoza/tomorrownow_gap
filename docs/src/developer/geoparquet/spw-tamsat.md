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

# SPW Tamsat GeoParquet

The **SPW Tamsat** dataset is a variant of the Standard Planting Window (SPW) product generated using **TAMSAT rainfall estimates**.  
Data is stored as partitioned **GeoParquet** files in the GAP bucket.

---

## Prerequisites

Same as [SPW](spw.md).

Python 3.9+

DuckDB with: `httpfs` and `spatial` extensions

Read-only object storage credentials (`S3_ENDPOINT_URL`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_BUCKET_NAME`)

---

## Path layout

Partitioned by **year** and **month**:

```bash
s3://$S3_BUCKET_NAME/staging/tamsat_spw_geoparquet/year=/month=.parquet
```

Example:

```bash
s3://gap-products/staging/tamsat_spw_geoparquet/year=2025/month=9.parquet
```

---

## Column descriptions

Observed schema (from `DESCRIBE SELECT * FROM read_parquet(... ) LIMIT 1`):

| #  | Column name             | Type     | Description |
|----|-------------------------|----------|-------------|
| 0  | `date`                  | DATE     | Record date for the daily TAMSAT-SPW output. |
| 1  | `farm_id`               | BIGINT   | Internal numeric farm identifier. |
| 2  | `farm_unique_id`        | VARCHAR  | External farm identifier used for joins/filters. |
| 3  | `country`               | VARCHAR  | Country name. |
| 4  | `farm_group_id`         | BIGINT   | Internal group identifier. |
| 5  | `farm_group`            | VARCHAR  | Farm group label (e.g., “KALRO 2025 A”). |
| 6  | `grid_id`               | BIGINT   | Numeric grid identifier. |
| 7  | `grid_unique_id`        | VARCHAR  | Unique grid identifier. |
| 8  | `geometry`              | GEOMETRY | Farm/grid geometry (polygon/point). |
| 9  | `latitude`              | DOUBLE   | Latitude of farm/grid centroid. |
| 10 | `longitude`             | DOUBLE   | Longitude of farm/grid centroid. |
| 11 | `sm_25`                 | DOUBLE   | Soil moisture proxy/indicator at 25th percentile (TAMSAT-derived workflow). |
| 12 | `sm_50`                 | DOUBLE   | Soil moisture proxy/indicator at 50th percentile. |
| 13 | `sm_70`                 | DOUBLE   | Soil moisture proxy/indicator at 70th percentile. |
| 14 | `spw_20`                | DOUBLE   | SPW decision score at 20% threshold scenario. |
| 15 | `spw_40`                | DOUBLE   | SPW decision score at 40% threshold scenario. |
| 16 | `spw_60`                | DOUBLE   | SPW decision score at 60% threshold scenario. |
| 17 | `pfc_user_probability`  | DOUBLE   | User/algorithm probability for PFC (Planting Feasibility Criteria). |
| 18 | `wrsi_user_probability` | DOUBLE   | User/algorithm probability for WRSI (Water Requirement Satisfaction Index). |
| 19 | `pfc_user_decision`     | DOUBLE   | Decision value derived from PFC probability. |
| 20 | `wrsi_user_decision`    | DOUBLE   | Decision value derived from WRSI probability. |
| 21 | `sm_user_decision`      | DOUBLE   | Decision contribution from soil moisture indicator(s). |
| 22 | `pfc_thresh`            | DOUBLE   | Threshold applied for PFC decisioning. |
| 23 | `pfc_prob_thresh`       | DOUBLE   | Probability threshold applied for PFC. |
| 24 | `wrsi_thresh_factor`    | DOUBLE   | Factor used in WRSI thresholding. |
| 25 | `wrsi_prob_thresh`      | DOUBLE   | Probability threshold applied for WRSI. |
| 26 | `year`                  | BIGINT   | Partition year (from folder structure). |

> Notes:
> - `sm_*` and `spw_*` fields capture soil-moisture and scenario-specific SPW indicators derived from TAMSAT inputs.
> - Partition columns (`year` via hive partitioning; month is in the path) should be used for performant queries.

---

## Example queries

### Filter by date

```sql
SELECT date, farm_unique_id, spw_20, spw_40, spw_60,
       pfc_user_probability, wrsi_user_probability
FROM read_parquet(
  's3://gap-products/staging/tamsat_spw_geoparquet/year=2025/month=9.parquet',
  hive_partitioning=true
)
WHERE date = DATE '2025-09-10'
LIMIT 20;
```

### Filter by farm

```python
import os, duckdb
conn = duckdb.connect()
conn.install_extension("httpfs"); conn.load_extension("httpfs")
conn.install_extension("spatial"); conn.load_extension("spatial")

farm_unique_id = '4023361'
path = (
  f"s3://{os.environ['S3_BUCKET_NAME']}/"
  "staging/tamsat_spw_geoparquet/year=2025/month=9.parquet"
)

conn.sql(f"""
CREATE OR REPLACE TABLE spw_tamsat AS
SELECT *
FROM read_parquet('{path}', hive_partitioning=true)
WHERE date BETWEEN '2025-09-05' AND '2025-09-10'
""")

conn.sql(f"""
SELECT date, farm_unique_id,
       spw_20, spw_40, spw_60,
       pfc_user_probability, wrsi_user_probability,
       pfc_user_decision, wrsi_user_decision, sm_user_decision
FROM spw_tamsat
WHERE farm_unique_id = '{farm_unique_id}'
ORDER BY date DESC
""").show()
```
