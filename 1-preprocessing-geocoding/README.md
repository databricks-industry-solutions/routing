# Stage 1: Preprocessing and geocoding

This stage does three things:

1. builds the shared Photon + OSRM assets in a Unity Catalog volume
2. validates that both services run on the cluster nodes that will process data
3. geocodes the local address sample into routing-ready shipments

## Notebooks

- `01_prepare_assets.py`
  Downloads Photon assets, builds OSRM, preprocesses the Indiana graph, writes
  `demo_env.sh`, and renders the init scripts into the shared volume.
- `02_validate_services.py`
  Confirms Photon and OSRM respond on the driver and executors.
- `03_geocode_addresses.py`
  Reads the vendored CSV from `../data/overture_indiana_addresses.csv`, geocodes it, and
  writes `demos.routing.raw_shipments`.

## Main outputs

- `/Volumes/demos/routing/routing_assets/config/demo_env.sh`
- `/Volumes/demos/routing/routing_assets/init/photon-worker.sh`
- `/Volumes/demos/routing/routing_assets/init/osrm-worker.sh`
- `demos.routing.source_addresses`
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
  replace the vendored CSV or point `03_geocode_addresses.py` at `source_table`
- Already have coordinates:
  set `coordinates_source_table` in `03_geocode_addresses.py`
