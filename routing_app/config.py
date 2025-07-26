import os

# Set environment variables
DATABRICKS_WAREHOUSE_ID = os.getenv('DATABRICKS_WAREHOUSE_ID', '148ccb90800933a1')


# Table Configuration
ROUTES_TABLE = os.getenv('ROUTES_TABLE', 'josh_melton.routing.optimized_routes')

# Expected table schema:
# - cluster_id: identifier for each route
# - truck_type: type of truck used for the route
# - route_index: order of stops within a route (INT)
# - package_id: identifier for each package/stop (STRING)
# - latitude: decimal latitude coordinates (DOUBLE)
# - longitude: decimal longitude coordinates (DOUBLE) 