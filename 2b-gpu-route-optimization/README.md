# Stage 2b: GPU route optimization

This stage keeps the original GPU-oriented pattern, but the bundle now splits it across
serverless Spark prep, classic OSRM resolution, and serverless GPU solve:

1. build a deterministic sparse neighbor graph with Databricks geospatial functions
2. resolve sparse OSRM durations for that graph
3. solve the capacity-constrained multi-vehicle routing problem with cuOpt on a GPU cluster

## Notebooks

- `01_prepare_sparse_graph.py`
  Serverless CPU step (environment version 5) that builds the deterministic mapping and
  sparse-neighbor tables.
- `02_resolve_sparse_osrm.py`
  Classic OSRM step on `gpu_distance_cluster` that writes
  `distances_by_route_gpu_*`.
- `03_route_optimization.py`
  Serverless GPU step on the A10-backed `serverless_gpu` environment that converts the
  sparse graph to a cuOpt waypoint matrix and writes
  `demos.routing.optimized_routes_gpu_10000` while enforcing van capacity inside the
  solve.
- `legacy_distance_calculation.py`
  Older all-in-one classic notebook retained as a manual fallback.

## Serverless boundary

- Serverless:
  `01_prepare_sparse_graph.py` and `03_route_optimization.py`
- Classic:
  `02_resolve_sparse_osrm.py`

## Main outputs

- `demos.routing.shipment_ids_map_gpu_10000`
- `demos.routing.shipment_clusters_gpu_10000`
- `demos.routing.distances_by_route_gpu_10000`
- `demos.routing.optimized_routes_gpu_10000`

## Default assumptions

- input table: `demos.routing.raw_shipments`
- read size: `10000`
- output table: `demos.routing.optimized_routes_gpu_10000`
- fallback if no shipment table exists: `../utils/shipments.csv`

## Main knobs

- `num_shipments`
- `num_routes`
- `max_van`
- `max_route_seconds`
- `solver_minutes`
- depot latitude / longitude

## Graph and solver behavior

- `global_idx` values are assigned by `package_id` order, so repeated runs use the same
  node ids for the same shipment subset.
- Neighbor ranking uses geodesic distance in meters (`st_distancesphere` when available,
  otherwise a haversine fallback), not planar degrees.
- Sparse-origin detection uses the full origin set, so shipments with zero first-pass
  candidates still flow into the boost passes.
- cuOpt now enforces a `weight` capacity dimension using `max_van`; `max_ev` is only used
  after solving to label lighter routes as EV-capable.

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
