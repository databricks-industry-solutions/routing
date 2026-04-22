# Stage 1: Preprocessing and geocoding

This stage does three things:

1. builds the shared Photon + OSRM assets in a Unity Catalog volume
2. validates that both services run on the cluster nodes that will process data
3. runs a split serverless/classic geocoding flow that ends at `demos.routing.raw_shipments`

## Notebooks

- `01_prepare_assets.py`
  Classic asset-prep step on `asset_prep_cluster`. Downloads Photon assets, builds
  OSRM, preprocesses the Indiana graph, writes `demo_env.sh`, and renders the init
  scripts into the shared volume.
- `02_validate_services.py`
  Classic validation step on `geocode_cluster`. Confirms Photon and OSRM respond on
  the driver and executors.
- `03_load_and_normalize_addresses.py`
  Serverless CPU step (environment version 5) that loads the vendored CSV, writes
  `source_addresses`, and materializes `normalized_addresses`.
- `04_geocode_addresses_photon.py`
  Classic Photon step on `geocode_cluster` that geocodes `normalized_addresses` into
  `geocoded_addresses`.
- `05_build_shipments.py`
  Serverless CPU step (environment version 5) that turns the Photon audit table into
  `raw_shipments`.
- `legacy_geocode_addresses.py`
  Older all-in-one classic notebook retained for manual fallback and debugging.

## Serverless boundary

- Serverless:
  `03_load_and_normalize_addresses.py` and `05_build_shipments.py`
- Classic:
  `01_prepare_assets.py`, `02_validate_services.py`, and
  `04_geocode_addresses_photon.py`

## Main outputs

- `/Volumes/demos/routing/routing_assets/config/demo_env.sh`
- `/Volumes/demos/routing/routing_assets/init/photon-worker.sh`
- `/Volumes/demos/routing/routing_assets/init/osrm-worker.sh`
- `demos.routing.source_addresses`
- `demos.routing.normalized_addresses`
- `demos.routing.geocoded_addresses`
- `demos.routing.raw_shipments`

## Most common edits

- Heavy first run:
  use an interactive cluster while iterating on asset prep or geocode quality, then move
  back to job clusters for final end-to-end validation
- New region:
  update the widget defaults in `01_prepare_assets.py`
- Force a fresh Photon dataset:
  set `force_refresh_photon=true` in `01_prepare_assets.py`
- Debug OSRM startup from the notebook:
  set `debug_start_osrm=true` in `01_prepare_assets.py`
- New local sample:
  replace the vendored CSV or point `03_load_and_normalize_addresses.py` at `source_table`
- Already have coordinates:
  set `coordinates_source_table` in `05_build_shipments.py`
