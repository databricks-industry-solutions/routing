# Agent Notes

This repo is intentionally staged. Keep that structure intact unless the user explicitly
asks to change the architecture.

## Safe change order

1. Validate `1-preprocessing-geocoding/01_prepare_assets.py`.
2. Validate `1-preprocessing-geocoding/02_validate_services.py`.
3. Validate `1-preprocessing-geocoding/03_geocode_addresses.py`.
4. Only then change the CPU path in `2a-cpu-route-optimization/`.
5. Only after CPU is stable, change the GPU path in `2b-gpu-route-optimization/`.
6. Re-run `databricks bundle run routing_end_to_end -p DEFAULT` before opening a PR.

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

## How to retarget the bundle

When moving this accelerator to a new cloud or workspace, update these two areas together
in `databricks.yml`:

- `targets.<target>.variables`
  for node types such as `cpu_node_type_id`, `geocode_node_type_id`, and
  `gpu_node_type_id`
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
  use `coordinates_source_table` in
  `1-preprocessing-geocoding/03_geocode_addresses.py`, or point the stage 2 notebooks at
  a shipments table directly
- New cloud / cluster shapes:
  adjust `targets.*.variables` plus
  `targets.*.resources.jobs.routing_end_to_end.job_clusters` in `databricks.yml`

## Important implementation notes

- The default local sample is intentionally checked in so the repo does not depend on
  runtime Overture access.
- Photon and OSRM are executor-local. Preserve the localhost-per-executor design.
- The volume-backed init scripts are rendered by stage 1 into
  `/Volumes/<catalog>/<schema>/<volume>/init/`.
- `routing_app/config.py` and `routing_app/app.yaml` should keep the SQL warehouse
  environment-specific. Do not hardcode a personal warehouse ID into the repo.
- `utils/shipments.csv` is a fallback path, not the main story.
- Keep CPU `cluster_id` values as zero-padded strings so `routing_app/app.py` can continue
  to sort them lexicographically.
- If you add or change routing constraints, mirror the new knobs in both the notebook
  widgets and `databricks.yml` so bundle runs stay reproducible.
