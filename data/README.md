# Vendored sample data

`overture_indiana_addresses.csv` is the default local input for the address-first demo.

## Source

- Dataset: Overture open address data
- Release used for the current sample: `2026-04-15.0`
- Access date for the vendored refresh used in this repo: `2026-04-20`
- Primary citation:
  `Overture Maps Foundation, overturemaps.org`

## How this sample is filtered

The refresh script keeps only U.S. address rows that:

- have non-null `street` and `number`
- are in the Indianapolis-metro city list used by this accelerator
- show Indiana in `address_levels`
- are deduplicated by formatted address

The exported CSV is then deterministically shuffled by `address_id` hash so the first
10k or 40k rows still represent a city mix instead of a single town.

## Why the sample is vendored

The runtime demo path should not depend on public Overture downloads. The repo should work
with a local address file plus setup-time Photon / OSRM downloads only.

## Refreshing the sample

Run from the repo root:

```bash
python3 -m pip install duckdb
python3 data/refresh_overture_indiana_addresses.py
```

Optional flags:

```bash
python3 data/refresh_overture_indiana_addresses.py --release 2026-04-15.0 --row-limit 55000
```

## Columns

- `address_id`
- `country`
- `street`
- `number`
- `unit`
- `postcode`
- `postal_city`
- `address_levels_json`
- `formatted_address`

## Attribution notes

Overture's address theme is assembled from multiple permissively licensed data sources,
some of which require attribution. See the Overture attribution page for the source-level
details when adapting this sample to a different geography or redistributing a larger
subset.

For the United States, Overture documents the address theme as including the National
Address Database (NAD) from the U.S. Department of Transportation and participating
states. Treat that lineage as part of the attribution chain for this vendored Indiana
sample.

## Licenses

© 2026 Databricks, Inc. All rights reserved. The source in this accelerator is provided
subject to the Databricks License. All included or referenced third party libraries are
subject to the licenses set forth below.

| library | description | license | source |
| --- | --- | --- | --- |
| OSRM Backend Server | High performance routing engine written in C++ designed to run on OpenStreetMap data | BSD 2-Clause "Simplified" License | https://github.com/Project-OSRM/osrm-backend |
| Photon | Open-source geocoder for OpenStreetMap data used by the address-to-coordinate stage | Apache License 2.0 | https://github.com/komoot/photon |
| ortools | Operations research tools developed at Google for combinatorial optimization | Apache License 2.0 | https://github.com/google/or-tools |
| folium | Visualize data in Python on interactive Leaflet.js maps | MIT License | https://github.com/python-visualization/folium |
| dash | Python framework for building analytical web applications and dashboards | MIT License | https://github.com/plotly/dash |
| branca | Library for generating complex HTML and JavaScript pages in Python; provides shared helpers for folium | MIT License | https://github.com/python-visualization/branca |
| plotly | Open-source Python library for interactive charts and graphs | MIT License | https://github.com/plotly/plotly.py |
| ray | Flexible distributed execution framework for scaling Python workflows | Apache License 2.0 | https://github.com/ray-project/ray |
| Databricks SDK for Python | Python SDK for Databricks workspace APIs used by the dashboard and setup helper | Apache License 2.0 | https://github.com/databricks/databricks-sdk-py |
| DuckDB | In-process analytical database used to refresh the vendored Overture sample | MIT License | https://github.com/duckdb/duckdb |
| cuOpt | GPU-accelerated combinatorial optimization solver from NVIDIA | Apache License 2.0 | https://docs.nvidia.com/cuopt/user-guide/latest/license.html |

The vendored address sample derives from Overture open address data. Review the
attribution section above before redistributing a larger or retargeted subset.
