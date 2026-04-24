# Address-First Routing on Databricks

This solution accelerator now has a staged `0 / 1 / 2a / 2b` story:

1. a small serverless introduction on repo-local coordinate data
2. address preprocessing plus Photon geocoding
3. CPU routing with OSRM + OR-Tools
4. GPU routing with a sparse graph + cuOpt

The main accelerator still starts with local address data, geocodes those addresses to
latitude/longitude with Photon, and then routes them with either a CPU or GPU solver.

The default sample is vendored into the repo, so the input data path works without any
runtime download from Overture. The only heavyweight setup-time downloads are the Photon
index and the Indiana OSRM graph assets.

## Tested targets

- AWS `dev` target via the `DEFAULT` Databricks CLI profile.
- Azure `azure` target via a caller-supplied Databricks CLI profile, for example:
  `databricks bundle deploy -t azure --profile <your-azure-profile>`.
- Azure classic-cluster defaults validated in an internal Azure test workspace:
  - asset prep / CPU OSRM / CPU optimize: `Standard_D16ds_v5`
  - geocode + service validation: `Standard_D32ds_v5`
- GPU optimization now runs on serverless GPU with `GPU_1xA10` in the bundle.
- The serverless GPU notebooks install `cuopt-cu12==25.8.0` in-notebook because
  scheduled serverless GPU jobs do not support bundle-managed dependency lists.

## Repo map

- `0-introduction/`
  Fast serverless-first learning path that uses `utils/shipments.csv` plus synthetic
  haversine travel times to validate CPU and GPU routing without Photon or OSRM.
- `1-preprocessing-geocoding/`
  Builds Photon + OSRM assets, validates executor-local services, then uses a split
  serverless/classic flow to materialize `demos.routing.raw_shipments`.
- `2a-cpu-route-optimization/`
  Uses a split serverless/classic CPU flow for clustering and OSRM prep, then Ray +
  OR-Tools on classic compute to write `demos.routing.optimized_routes`.
- `2b-gpu-route-optimization/`
  Uses a split serverless/classic GPU prep flow plus cuOpt on serverless GPU to write
  `demos.routing.optimized_routes_gpu_10000`.
- `data/`
  Contains the vendored Overture-derived address sample and the refresh script used to
  regenerate it.
- `utils/shipments.csv`
  Explicit coordinates-only fallback for environments that want to skip Photon.
- `routing_app/`
  Dash app for browsing the final route table.
- `databricks.yml`
  Easy-button Asset Bundle with both the default AWS target and an Azure-oriented
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
databricks bundle run routing_intro
databricks bundle run routing_end_to_end
```

Deploy and run the Azure-oriented target with your own Databricks CLI profile:

```bash
databricks bundle deploy -t azure --profile <your-azure-profile>
databricks bundle run routing_intro -t azure --profile <your-azure-profile>
databricks bundle run routing_end_to_end -t azure --profile <your-azure-profile>
```

The recommended workflow order is:

1. `routing_intro`
2. `routing_end_to_end`

The hybrid `routing_end_to_end` job then runs:

1. `1a_load_normalize_addresses`
2. `1_prepare_assets`
3. `1_validate_services`
4. `1b_geocode_addresses`
5. `1c_build_shipments`
6. `2a_cluster_shipments`
7. `2a_resolve_osrm`
8. `2a_finalize_routing`
9. `2a_cpu_optimize`
10. `2b_prepare_sparse_graph`
11. `2b_resolve_sparse_osrm`
12. `2b_gpu_optimize`

CPU and GPU branches still run in parallel after `1c_build_shipments`.

## Serverless boundary

Serverless is used only where it is a good fit in this repo:

- `0-introduction/`
- stage 1 address load + normalize
- stage 1 shipment finalization after Photon
- CPU clustering and routing-table finalization
- GPU sparse-graph preparation
- GPU cuOpt optimization

Classic compute is still required for:

- `1-preprocessing-geocoding/01_prepare_assets.py`
- `1-preprocessing-geocoding/02_validate_services.py`
- the Photon call notebook (`04_geocode_addresses_photon.py`)
- the CPU and GPU OSRM notebooks
- `2a-cpu-route-optimization/04_route_optimization.py` because it still depends on Ray

## Recommended notebook pairings

The bundle job wiring is the source of truth for compute selection. The checked-in
`.py` notebook source format does not preserve the UI-only notebook environment metadata
the same way Jupyter exports do, so the notebook headers and the mappings below document
the intended pairings for manual runs.

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
- Classic clusters:
  `1-preprocessing-geocoding/01_prepare_assets.py` on `asset_prep_cluster`,
  `1-preprocessing-geocoding/02_validate_services.py` and
  `1-preprocessing-geocoding/04_geocode_addresses_photon.py` on `geocode_cluster`,
  `2a-cpu-route-optimization/02_resolve_osrm_distances.py` on
  `cpu_distance_cluster`,
  `2a-cpu-route-optimization/04_route_optimization.py` on `cpu_optimize_cluster`,
  and `2b-gpu-route-optimization/02_resolve_sparse_osrm.py` on
  `gpu_distance_cluster`

If you want named interactive clusters for manual notebook runs, the repo includes a
Databricks Python SDK helper that creates or updates those classic clusters for you.

```bash
python3 -m pip install databricks-sdk
python3 setup_classic_clusters.py --profile <your-profile> --asset-prep-only --start
```

Run `1-preprocessing-geocoding/01_prepare_assets.py` on `asset_prep_cluster`, then create
the rest of the classic notebook clusters:

```bash
python3 setup_classic_clusters.py --profile <your-profile> --start
```

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
scripts and geocoding logic are stable. Then switch back to the bundle-managed jobs and
rerun:

```bash
databricks bundle run routing_intro
databricks bundle run routing_end_to_end
```

## Observed runtimes

Successful Azure validation in an internal test workspace produced these approximate
stage-level timings on the classic-first version of the workflow:

- asset prep cold path: ~49 min
- service validation: ~6 min
- geocode path combined: ~9 min
- CPU branch combined: ~31 min
- GPU branch combined: ~39 min

The hybrid split introduced here changes the task graph, but these numbers remain a good
order-of-magnitude guide for the heavy classic stages.

## Manual notebook order

If you prefer to run notebook-by-notebook:

1. Optional but recommended first run: `0-introduction/01_cpu_intro.py`, then
   `0-introduction/02_gpu_intro.py`.
2. Run `1-preprocessing-geocoding/03_load_and_normalize_addresses.py`.
3. Run `1-preprocessing-geocoding/01_prepare_assets.py`.
4. Attach the rendered init scripts from `/Volumes/demos/routing/routing_assets/init/`
   to the cluster you will use for geocoding and distance resolution.
5. Restart that cluster.
6. Run `1-preprocessing-geocoding/02_validate_services.py`.
7. Run `1-preprocessing-geocoding/04_geocode_addresses_photon.py`.
8. Run `1-preprocessing-geocoding/05_build_shipments.py`.
9. Run the CPU path in `2a-cpu-route-optimization/`.
10. Run the GPU path in `2b-gpu-route-optimization/`.

The older all-in-one notebooks are still in the repo as classic/manual fallbacks:

- `1-preprocessing-geocoding/legacy_geocode_addresses.py`
- `2a-cpu-route-optimization/legacy_distance_calculation.py`
- `2b-gpu-route-optimization/legacy_distance_calculation.py`

## Default tables

- `demos.routing.intro_shipments`
  Repo-local intro subset used by the serverless-first notebook path.
- `demos.routing.intro_routes_cpu`
  Intro CPU routing output.
- `demos.routing.intro_routes_gpu`
  Intro GPU routing output.
- `demos.routing.source_addresses`
  Local address input loaded from the vendored CSV.
- `demos.routing.normalized_addresses`
  Deterministic Photon request handoff table produced by the serverless step 3 notebook.
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
- Use `1-preprocessing-geocoding/legacy_geocode_addresses.py` with
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
  `1-preprocessing-geocoding/03_load_and_normalize_addresses.py`,
  `1-preprocessing-geocoding/05_build_shipments.py`, or
  `data/refresh_overture_indiana_addresses.py`.
- Different cluster shapes:
  `databricks.yml` variables `cpu_node_type_id`, `geocode_node_type_id`, and
  `cpu_spark_version`, plus the serverless GPU accelerator in
  `resources.jobs.*.tasks.*.compute.hardware_accelerator`.
- Different serverless/classic boundaries:
  `resources.jobs.routing_intro`, `resources.jobs.routing_end_to_end`, and
  `targets.*.resources.jobs.routing_end_to_end.job_clusters` in `databricks.yml`.
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

The vendored address sample derives from Overture open address data. See `data/README.md`
for source release details and attribution notes.
