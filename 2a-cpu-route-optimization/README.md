# Stage 2a: CPU route optimization

This stage keeps the original CPU-oriented pattern:

1. cluster shipments with a deterministic depot-anchored sweep
2. resolve OSRM duration matrices per cluster
3. solve each cluster independently with OR-Tools on CPU

## Notebooks

- `01_distance_calculation.py`
  Reads `demos.routing.raw_shipments`, deterministically sweep-clusters the data,
  resolves OSRM durations, and writes the intermediate CPU tables.
- `02_route_optimization.py`
  Uses Ray plus OR-Tools to write `demos.routing.optimized_routes`.

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
