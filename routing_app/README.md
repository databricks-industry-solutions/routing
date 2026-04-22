# Route Optimization Dashboard

This Dash app reads the final route table from Databricks SQL and renders each route as an
interactive map.

## Default table

The app now defaults to:

```bash
export ROUTES_TABLE="demos.routing.optimized_routes"
```

Override that environment variable if you want to inspect a different output, such as the
GPU table.

There is intentionally no repo-wide default for `DATABRICKS_WAREHOUSE_ID`. Pick the SQL
warehouse that exists in the workspace where you run the app.

## Setup

1. Install the app dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Authenticate to Databricks with the profile you want the app to use:

```bash
databricks auth login --profile <your-profile>
export DATABRICKS_CONFIG_PROFILE="<your-profile>"
```

3. Set the SQL warehouse and optional route table:

```bash
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
export ROUTES_TABLE="demos.routing.optimized_routes"
```

4. Start the app:

```bash
python app.py
```

Then open `http://localhost:8050`.

## Smoke-test recipe

The Azure validation pass for this repo used:

```bash
export DATABRICKS_CONFIG_PROFILE="adb-984752964297111"
export DATABRICKS_WAREHOUSE_ID="<workspace-warehouse-id>"
```

CPU output:

```bash
export ROUTES_TABLE="demos.routing.optimized_routes"
python app.py
```

GPU output:

```bash
export ROUTES_TABLE="demos.routing.optimized_routes_gpu_10000"
python app.py
```

For a quick smoke test, confirm that:

- the page loads without a traceback
- the route dropdown populates
- the default route shows stats
- the Plotly map renders route points

## Expected schema

The route table should expose:

- `cluster_id`
- `truck_type`
- `route_index`
- `package_id`
- `latitude`
- `longitude`

## Notes

- The accelerator's CPU path writes `demos.routing.optimized_routes` by default.
- The GPU path writes `demos.routing.optimized_routes_gpu_10000` by default.
- If `DATABRICKS_WAREHOUSE_ID` is unset, startup fails fast with:
  `Set DATABRICKS_WAREHOUSE_ID to a Databricks SQL warehouse ID before starting the app`
- If you deploy this as a Databricks App, replace `REPLACE_ME` in `app.yaml` or inject
  `DATABRICKS_WAREHOUSE_ID` during deployment.
- The app relies on Databricks SQL connectivity; it is not part of the bundle workflow.
