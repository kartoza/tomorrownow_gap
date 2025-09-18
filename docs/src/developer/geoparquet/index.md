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

# GAP GeoParquet Products

The TomorrowNow GAP platform publishes processed products as **GeoParquet** files in the object storage bucket.  
These datasets can be queried directly using [DuckDB](https://duckdb.org), [Dask](https://www.dask.org/), or other parquet-compatible libraries.

## Available Products

- [**SPW**](spw.md)  
  Standard Planting Window dataset. Provides daily and seasonal planting recommendations for farmers.

- [**SPW Tamsat**](spw-tamsat.md)  
  Variant of SPW dataset using TAMSAT rainfall estimates.

- [**DCAS**](dcas.md)  
  Dynamic Crop Advisory System outputs combining crop models, GDD thresholds, and registry data.

---

## Next Steps

For each product you will find:

1. **Pre-requisites** — setting up S3 credentials and DuckDB access.  
2. **Column Descriptions** — schema and meaning of each field.  
3. **Example Queries** — how to query GeoParquet files for specific use cases.
