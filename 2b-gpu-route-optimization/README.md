# Stage 2b: GPU route optimization

This stage keeps the original GPU-oriented pattern:

1. build a deterministic sparse neighbor graph with Databricks geospatial functions
2. resolve sparse OSRM durations for that graph
3. solve the capacity-constrained multi-vehicle routing problem with cuOpt on a GPU cluster

## Notebooks

- `01_distance_calculation.py`
  Builds the GPU mapping, sparse-neighbor, and OSRM-duration tables with deterministic
  ordering and geodesic neighbor ranking.
- `02_route_optimization.py`
  Converts that sparse graph to a cuOpt waypoint matrix and writes
  `demos.routing.optimized_routes_gpu_10000` while enforcing van capacity inside the solve.

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
