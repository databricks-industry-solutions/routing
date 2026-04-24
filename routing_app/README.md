# Route Optimization Dashboard

This Dash app reads the final route table from Databricks SQL and renders each route as an
interactive map.

## Default table

The app now defaults to a CPU/GPU switcher backed by:

```bash
export CPU_ROUTES_TABLE="demos.routing.optimized_routes"
export GPU_ROUTES_TABLE="demos.routing.optimized_routes_gpu_10000"
export DEFAULT_ROUTE_SOURCE="cpu"
```

For backward compatibility, `ROUTES_TABLE` still acts as the CPU table default.

There is intentionally no repo-wide default for `DATABRICKS_WAREHOUSE_ID`. Pick the SQL
warehouse that exists in the workspace where you run the app.

## Setup

1. Install the app dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Authenticate to Databricks with the profile you want the app to use:

```bash
databricks auth login --profile <your-profile>
export DATABRICKS_CONFIG_PROFILE="<your-profile>"
```

3. Set the SQL warehouse and optional route tables:

```bash
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
export CPU_ROUTES_TABLE="demos.routing.optimized_routes"
export GPU_ROUTES_TABLE="demos.routing.optimized_routes_gpu_10000"
```

4. Start the app:

```bash
python app.py
```

Then open `http://localhost:8050`.

## Smoke-test recipe

One Azure smoke-test recipe is:

```bash
export DATABRICKS_CONFIG_PROFILE="<your-azure-profile>"
export DATABRICKS_WAREHOUSE_ID="<workspace-warehouse-id>"
```

CPU output:

```bash
export DEFAULT_ROUTE_SOURCE="cpu"
python app.py
```

GPU output:

```bash
export DEFAULT_ROUTE_SOURCE="gpu"
python app.py
```

For a quick smoke test, confirm that:

- the page loads without a traceback
- the CPU/GPU switch updates the route list
- the route dropdown populates
- the default route shows stats
- the Plotly map renders route points on the basemap

## Expected schema

The route table should expose:

- `cluster_id`
- `truck_type`
- `route_index`
- `package_id`
- `latitude`
- `longitude`

## Notes

- The accelerator's CPU path writes `demos.routing.optimized_routes` by default.
- The GPU path writes `demos.routing.optimized_routes_gpu_10000` by default.
- The app uses a built-in CPU/GPU switcher; set `CPU_ROUTES_TABLE`, `GPU_ROUTES_TABLE`, and
  `DEFAULT_ROUTE_SOURCE` if you want different defaults.
- If `DATABRICKS_WAREHOUSE_ID` is unset, startup fails fast with:
  `Set DATABRICKS_WAREHOUSE_ID to a Databricks SQL warehouse ID before starting the app`
- If you deploy this as a Databricks App, replace `REPLACE_ME` in `app.yaml` or inject
  `DATABRICKS_WAREHOUSE_ID` during deployment.
- The app relies on Databricks SQL connectivity; it is not part of the bundle workflow.

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

The vendored address sample derives from Overture open address data. See `../data/README.md`
for source release details and attribution notes.
