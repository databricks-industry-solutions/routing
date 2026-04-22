# Address-First Routing on Databricks

This solution accelerator starts with local address data, geocodes those addresses to
latitude/longitude with Photon, and then routes them with either a CPU or GPU solver.

The default sample is vendored into the repo, so the input data path works without any
runtime download from Overture. The only heavyweight setup-time downloads are the Photon
index and the Indiana OSRM graph assets.

## Tested targets

- AWS `dev` target via the `DEFAULT` Databricks CLI profile.
- Azure `azure` target via `adb-984752964297111`
  (`https://adb-984752964297111.11.azuredatabricks.net`).
- Azure node defaults validated in this repo:
  - asset prep / CPU distance / CPU optimize: `Standard_D16ds_v5`
  - geocode + service validation: `Standard_D32ds_v5`
  - GPU optimize: `Standard_NV36ads_A10_v5`

## Repo map

- `1-preprocessing-geocoding/`
  Builds Photon + OSRM assets, validates executor-local services, and geocodes the
  vendored Indianapolis-area address sample into `demos.routing.raw_shipments`.
- `2a-cpu-route-optimization/`
  Uses OSRM matrices plus OR-Tools on CPU to write `demos.routing.optimized_routes`.
- `2b-gpu-route-optimization/`
  Uses a sparse OSRM neighbor graph plus cuOpt on GPU to write
  `demos.routing.optimized_routes_gpu_10000`.
- `data/`
  Contains the vendored Overture-derived address sample and the refresh script used to
  regenerate it.
- `utils/shipments.csv`
  Explicit coordinates-only fallback for environments that want to skip Photon.
- `routing_app/`
  Dash app for browsing the final route table.
- `databricks.yml`
  Easy-button Asset Bundle with both the default AWS target and the Azure-validated
  `azure` target.

## Easy button

The default AWS-oriented environment is:

- Databricks CLI profile: `DEFAULT`
- Catalog: `demos`
- Schema: `routing`
- Volume: `routing_assets`
- Region: `indiana`

Deploy and run the full workflow on AWS / the default target:

```bash
databricks bundle deploy
databricks bundle run routing_end_to_end
```

Deploy and run the Azure-validated target:

```bash
databricks bundle deploy -t azure
databricks bundle run routing_end_to_end -t azure
```

The `azure` target already pins `targets.azure.workspace.profile`, so do not override it
with `--profile DEFAULT`.

The workflow order is:

1. `1_prepare_assets`
2. `1_validate_services`
3. `1_geocode_addresses`
4. `2a_cpu_distance`
5. `2a_cpu_optimize`
6. `2b_gpu_distance`
7. `2b_gpu_optimize`

CPU and GPU optimization run as separate branches after geocoding.

If the configured catalog does not exist, `1-preprocessing-geocoding/01_prepare_assets.py`
fails immediately with:

```text
Catalog `<catalog>` must already exist before running this notebook.
```

## Development loop

The first `1-preprocessing-geocoding/01_prepare_assets.py` run is the slow path because
the Photon archive is large and must be unpacked locally before it is copied into the
shared volume.

For iterative troubleshooting, use an interactive cluster for stage 1 until the init
scripts and geocoding logic are stable. Then switch back to the bundle-managed job
clusters and rerun:

```bash
databricks bundle run routing_end_to_end -p DEFAULT
```

## Observed runtimes

Successful Azure validation on `adb-984752964297111` produced these approximate
successful-task timings:

- `1_prepare_assets`: ~49 min cold path
- `1_validate_services`: ~6 min
- `1_geocode_addresses`: ~9 min
- `2a_cpu_distance`: ~16 min
- `2a_cpu_optimize`: ~15 min
- `2b_gpu_distance`: ~9 min
- `2b_gpu_optimize`: ~30 min

Because CPU and GPU branches run in parallel after geocoding, the observed cold-start
wall clock is roughly `100-105` minutes once the workflow is green.

## Manual notebook order

If you prefer to run notebook-by-notebook:

1. Run `1-preprocessing-geocoding/01_prepare_assets.py`.
2. Attach the rendered init scripts from `/Volumes/demos/routing/routing_assets/init/`
   to the cluster you will use for geocoding and distance resolution.
3. Restart that cluster.
4. Run `1-preprocessing-geocoding/02_validate_services.py`.
5. Run `1-preprocessing-geocoding/03_geocode_addresses.py`.
6. Run the CPU path in `2a-cpu-route-optimization/`.
7. Run the GPU path in `2b-gpu-route-optimization/`.

## Default tables

- `demos.routing.source_addresses`
  Local address input loaded from the vendored CSV.
- `demos.routing.geocoded_addresses`
  Audit table with Photon status and matched coordinates.
- `demos.routing.raw_shipments`
  Routing-ready shipments with `package_id`, `city`, `latitude`, `longitude`, `weight`.
- `demos.routing.optimized_routes`
  Default CPU output used by the Dash app.
- `demos.routing.optimized_routes_gpu_10000`
  Default GPU output.

## Sample sizing

The vendored sample contains about 55k Indianapolis-metro addresses so both solver paths
can read from the same file while keeping their original scale targets:

- CPU defaults to `40000` shipments.
- GPU defaults to `10000` shipments.

Stage 1 synthesizes a deterministic `weight` column because Overture address records do
not include shipment weights.

## Coordinates-only escape hatch

If you already have trusted coordinates:

- Skip Photon and point the CPU or GPU notebooks at your own shipments table, or
- Use `1-preprocessing-geocoding/03_geocode_addresses.py` with
  `coordinates_source_table` set to a table that already exposes:
  `package_id`, `city`, `latitude`, `longitude`, `weight`.

If no shipment table exists at all, the staged CPU and GPU notebooks fall back to
`utils/shipments.csv`.

## Adapting the accelerator

Change these places first:

- Different catalog / schema / volume:
  `databricks.yml` defaults and the widget defaults in
  `1-preprocessing-geocoding/01_prepare_assets.py`.
- Different region:
  Stage 1 `region`, `pbf_url`, `photon_db_url`, `photon_health_query`,
  and `osrm_health_route`.
- Different input data:
  `1-preprocessing-geocoding/03_geocode_addresses.py` or `data/refresh_overture_indiana_addresses.py`.
- Different cluster shapes:
  `databricks.yml` variables `cpu_node_type_id`, `geocode_node_type_id`,
  `gpu_node_type_id`, `cpu_spark_version`, and `gpu_spark_version`.
- Different app defaults:
  `routing_app/config.py`, `routing_app/app.yaml`, and `routing_app/README.md`.

## Data provenance

The vendored address sample is derived from Overture's open address data and filtered to
an Indianapolis-metro subset that aligns with the original routing example.

See `data/README.md` for:

- source release details
- the exact city filter
- refresh instructions
- attribution notes

## For AI agents

Start with `AGENTS.md`. It explains the stage contracts, the safest change order, and the
small set of files that control data source, geography, and deployment defaults.
