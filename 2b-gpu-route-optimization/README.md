# Stage 2b: GPU route optimization

This stage keeps the original GPU-oriented pattern:

1. build a sparse neighbor graph with Databricks geospatial functions
2. resolve sparse OSRM durations for that graph
3. solve the multi-vehicle routing problem with cuOpt on a GPU cluster

## Notebooks

- `01_distance_calculation.py`
  Builds the GPU mapping, sparse-neighbor, and OSRM-duration tables.
- `02_route_optimization.py`
  Converts that sparse graph to a cuOpt waypoint matrix and writes
  `demos.routing.optimized_routes_gpu_10000`.

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
- `max_route_seconds`
- `solver_minutes`
- depot latitude / longitude
