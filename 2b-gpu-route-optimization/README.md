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
