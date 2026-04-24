# Stage 0: Introduction

This is the fast path for learning the accelerator and validating serverless compute.
It uses the checked-in `../utils/shipments.csv` sample, builds synthetic haversine
travel times, and avoids Photon, OSRM, init scripts, and Ray.

## Notebooks

- `01_cpu_intro.py`
  Runs a small capacity-constrained OR-Tools solve on serverless CPU
  (environment version 5).
- `02_gpu_intro.py`
  Runs a small cuOpt solve on serverless GPU (environment version 4 on A10) with the
  same repo-local sample.

## Default outputs

- `demos.routing.intro_shipments`
- `demos.routing.intro_routes_cpu`
- `demos.routing.intro_routes_gpu`

Both route outputs keep the same core schema shape used elsewhere in the repo:
`cluster_id`, `truck_type`, `route_index`, `package_id`, `latitude`, `longitude`.

Both intro notebooks default to the same 50-row subset so `intro_shipments` stays
consistent across the CPU and GPU examples.

## When to use it

- First validation in a blank workspace
- Quick regression check for serverless support
- Learning the routing table contract before the Photon + OSRM workflow

## Manual run notes

- Run `01_cpu_intro.py` on serverless CPU (environment version 5).
- Run `02_gpu_intro.py` on serverless GPU, preferably `GPU_1xA10` with environment
  version 4.
- The bundle config wires the needed serverless environments for job runs.
- The GPU notebook installs `cuopt-cu12==25.8.0` itself because scheduled
  serverless GPU jobs do not support bundle-managed Python dependencies.
- For an interactive run, add dependencies in the notebook Environment panel first:
  - CPU intro: `ortools==9.14.6206`
  - GPU intro: none required beyond the notebook's built-in
    `%pip install --extra-index-url=https://pypi.nvidia.com cuopt-cu12==25.8.0`

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
