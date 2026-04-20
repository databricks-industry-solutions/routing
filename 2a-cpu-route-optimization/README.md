# Stage 2a: CPU route optimization

This stage keeps the original CPU-oriented pattern:

1. cluster shipments spatially
2. resolve OSRM duration matrices per cluster
3. solve each cluster independently with OR-Tools on CPU

## Notebooks

- `01_distance_calculation.py`
  Reads `demos.routing.raw_shipments`, clusters the data, resolves OSRM durations, and
  writes the intermediate CPU tables.
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
