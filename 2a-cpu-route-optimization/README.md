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
