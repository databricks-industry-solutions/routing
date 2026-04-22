# Agent Notes

This repo is intentionally staged. Keep that structure intact unless the user explicitly
asks to change the architecture.

## Safe change order

1. Validate `0-introduction/` first if you are checking serverless behavior or a blank workspace.
2. Validate `1-preprocessing-geocoding/01_prepare_assets.py`.
3. Validate `1-preprocessing-geocoding/02_validate_services.py`.
4. Validate the split stage 1 path:
   `03_load_and_normalize_addresses.py` ->
   `04_geocode_addresses_photon.py` ->
   `05_build_shipments.py`.
5. Only then change the CPU path in `2a-cpu-route-optimization/`.
6. Only after CPU is stable, change the GPU path in `2b-gpu-route-optimization/`.
7. Re-run `databricks bundle run routing_intro` and
   `databricks bundle run routing_end_to_end` before opening a PR.

## Stage contracts

- Stage 1 output table:
  `demos.routing.raw_shipments`
- Required schema for downstream routing:
  `package_id`, `city`, `latitude`, `longitude`, `weight`
- CPU default output:
  `demos.routing.optimized_routes`
- GPU default output:
  `demos.routing.optimized_routes_gpu_10000`

Do not silently change these contracts. If you must change them, update:

- `README.md`
- `routing_app/config.py`
- `databricks.yml`

## Key files

- Serverless-first intro:
  `0-introduction/`
- Deployment defaults:
  `databricks.yml`
- Vendored sample refresh:
  `data/refresh_overture_indiana_addresses.py`
- Data provenance:
  `data/README.md`
- Stage 1 notebooks:
  `1-preprocessing-geocoding/`
- CPU notebooks:
  `2a-cpu-route-optimization/`
- GPU notebooks:
  `2b-gpu-route-optimization/`
- Coordinates-only fallback:
  `utils/shipments.csv`

## Environment defaults

- Databricks CLI profile:
  `DEFAULT`
- Azure target profile:
  `adb-984752964297111`
- Catalog / schema:
  `demos.routing`
- Shared managed volume:
  `demos.routing.routing_assets`
- Region:
  `indiana`

## Notebook pairings

- Serverless CPU environment version 5:
  `0-introduction/01_cpu_intro.py`,
  `1-preprocessing-geocoding/03_load_and_normalize_addresses.py`,
  `1-preprocessing-geocoding/05_build_shipments.py`,
  `2a-cpu-route-optimization/01_cluster_shipments.py`,
  `2a-cpu-route-optimization/03_build_routing_table.py`, and
  `2b-gpu-route-optimization/01_prepare_sparse_graph.py`
- Serverless GPU environment version 4 on A10:
  `0-introduction/02_gpu_intro.py` and
  `2b-gpu-route-optimization/03_route_optimization.py`
- Classic asset prep:
  `1-preprocessing-geocoding/01_prepare_assets.py` on `asset_prep_cluster`
- Classic Photon + OSRM validation/geocoding:
  `1-preprocessing-geocoding/02_validate_services.py` and
  `1-preprocessing-geocoding/04_geocode_addresses_photon.py` on `geocode_cluster`
- Classic CPU OSRM + optimization:
  `2a-cpu-route-optimization/02_resolve_osrm_distances.py` on `cpu_distance_cluster`
  and `2a-cpu-route-optimization/04_route_optimization.py` on
  `cpu_optimize_cluster`
- Classic GPU OSRM:
  `2b-gpu-route-optimization/02_resolve_sparse_osrm.py` on
  `gpu_distance_cluster`

## How to retarget the bundle

When moving this accelerator to a new cloud or workspace, update these two areas together
in `databricks.yml`:

- `targets.<target>.variables`
  for classic node types such as `cpu_node_type_id` and `geocode_node_type_id`
- `resources.jobs.routing_intro` and `resources.jobs.routing_end_to_end`
  for serverless task wiring, notebook paths, and the serverless GPU step
- `targets.<target>.resources.jobs.routing_end_to_end.job_clusters`
  for cloud-specific fields like `aws_attributes` vs `azure_attributes`, single-node
  settings, and any cluster-level init-script shape changes

After changing them, validate with `databricks bundle validate -t <target>` and rerun
stage 1 before touching CPU or GPU logic.

## Common adaptation points

- New geography:
  update the stage 1 widget defaults for `region`, `pbf_url`, `photon_db_url`,
  `photon_health_query`, and `osrm_health_route`
- New address sample:
  update `data/refresh_overture_indiana_addresses.py` and regenerate the CSV
- Already have coordinates:
  use `coordinates_source_table` in `1-preprocessing-geocoding/05_build_shipments.py`,
  use the older `legacy_geocode_addresses.py` fallback, or point the stage 2 notebooks
  at a shipments table directly
- New cloud / cluster shapes:
  adjust `targets.*.variables` plus
  `targets.*.resources.jobs.routing_end_to_end.job_clusters` in `databricks.yml`

## Important implementation notes

- The default local sample is intentionally checked in so the repo does not depend on
  runtime Overture access.
- The bundle is now hybrid:
  use serverless for Spark-native pre/post processing plus GPU library steps, and keep
  init-script, Photon, OSRM, and Ray tasks on classic compute.
- Scheduled serverless GPU jobs do not support bundle-managed Python dependency lists,
  so the cuOpt notebooks install `cuopt-cu12` with `%pip` inside the notebook.
- Photon and OSRM are executor-local. Preserve the localhost-per-executor design.
- The volume-backed init scripts are rendered by stage 1 into
  `/Volumes/<catalog>/<schema>/<volume>/init/`.
- `routing_app/config.py` and `routing_app/app.yaml` should keep the SQL warehouse
  environment-specific. Do not hardcode a personal warehouse ID into the repo.
- `utils/shipments.csv` is a fallback path, not the main story.
- The older all-in-one notebooks are still present as manual/classic fallbacks, but the
  bundle uses the split `03/04/05`, `01/02/03/04`, and `01/02/03` entrypoints.
- Keep CPU `cluster_id` values as zero-padded strings so `routing_app/app.py` can continue
  to sort them lexicographically.
- If you add or change routing constraints, mirror the new knobs in both the notebook
  widgets and `databricks.yml` so bundle runs stay reproducible.
