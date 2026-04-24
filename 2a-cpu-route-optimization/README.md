# Stage 2a: CPU route optimization

This stage keeps the original CPU-oriented pattern, but the bundle now splits it across
serverless Spark prep and classic Ray/OSRM work:

1. cluster shipments with a deterministic depot-anchored sweep
2. resolve OSRM duration matrices per cluster
3. solve each cluster independently with OR-Tools on CPU

## Notebooks

- `01_cluster_shipments.py`
  Serverless CPU step (environment version 5) that loads shipments and writes the
  deterministic `shipments_by_route_cpu_*` handoff table.
- `02_resolve_osrm_distances.py`
  Classic OSRM step on `cpu_distance_cluster` that writes
  `distances_by_route_cpu_*`.
- `03_build_routing_table.py`
  Serverless CPU step (environment version 5) that writes
  `routing_unified_by_cluster_cpu_*`.
- `04_route_optimization.py`
  Classic Ray + OR-Tools step on `cpu_optimize_cluster` that writes
  `demos.routing.optimized_routes`.
- `legacy_distance_calculation.py`
  Older all-in-one classic notebook retained as a manual fallback.

## Serverless boundary

- Serverless:
  `01_cluster_shipments.py` and `03_build_routing_table.py`
- Classic:
  `02_resolve_osrm_distances.py` and `04_route_optimization.py`

## Main outputs

- `demos.routing.shipments_by_route_cpu_40000`
- `demos.routing.distances_by_route_cpu_40000`
- `demos.routing.routing_unified_by_cluster_cpu_40000`
- `demos.routing.optimized_routes`

## Default assumptions

- input table: `demos.routing.raw_shipments`
- read size: `40000`
- output table: `demos.routing.optimized_routes`
- fallback if no shipment table exists: `../utils/shipments.csv`

## Main knobs

- `num_shipments`
- `num_routes`
- `max_van`
- `solver_thinking_time`
- depot latitude / longitude

## Clustering behavior

- The stage rewrites `shipments_by_route_cpu_*` on every run so cluster assignments stay in
  sync with the current input table and widget values.
- `cluster_id` is a dense zero-padded string such as `001`, `002`, ... so downstream SQL
  ordering stays stable in the Dash app.
- The sweep heuristic respects `max_van` while keeping geographically adjacent stops near
  each other around the depot.

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
