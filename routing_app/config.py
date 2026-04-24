import os

# Warehouse selection is environment-specific; do not bake a repo-specific ID into source.
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()


# Table Configuration
ROUTES_TABLE = os.getenv("ROUTES_TABLE", "demos.routing.optimized_routes").strip()
CPU_ROUTES_TABLE = os.getenv("CPU_ROUTES_TABLE", ROUTES_TABLE).strip()
GPU_ROUTES_TABLE = os.getenv(
    "GPU_ROUTES_TABLE", "demos.routing.optimized_routes_gpu_10000"
).strip()
DEFAULT_ROUTE_SOURCE = os.getenv("DEFAULT_ROUTE_SOURCE", "cpu").strip().lower()

# Expected table schema:
# - cluster_id: identifier for each route
# - truck_type: type of truck used for the route
# - route_index: order of stops within a route (INT)
# - package_id: identifier for each package/stop (STRING)
# - latitude: decimal latitude coordinates (DOUBLE)
# - longitude: decimal longitude coordinates (DOUBLE) 