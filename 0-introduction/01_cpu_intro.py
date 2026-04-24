# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction: serverless CPU routing
# MAGIC
# MAGIC This notebook is the fastest way to validate the accelerator on serverless CPU.
# MAGIC It reads the repo-local `utils/shipments.csv` sample, builds a synthetic haversine
# MAGIC travel-time matrix, solves a small capacity-constrained VRP with OR-Tools, and
# MAGIC writes a route table without Photon, OSRM, init scripts, or Ray.
# MAGIC
# MAGIC Recommended compute: serverless CPU notebook. The bundle runs this as `intro_cpu`
# MAGIC on the serverless CPU environment (serverless environment version 5).

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("sample_table", "intro_shipments", "Prepared intro shipment table")
dbutils.widgets.text("output_table", "intro_routes_cpu", "Output intro CPU routes table")
dbutils.widgets.text(
    "local_csv_relative_path",
    "../utils/shipments.csv",
    "Relative path to repo-local shipment sample",
)
dbutils.widgets.text("num_shipments", "50", "Shipment count to read")
dbutils.widgets.text("num_routes", "6", "Requested vehicle count")
dbutils.widgets.text("max_ev", "4000", "Maximum EV capacity")
dbutils.widgets.text("max_van", "8000", "Maximum van capacity")
dbutils.widgets.text("max_route_seconds", "14400", "Maximum route seconds")
dbutils.widgets.text("solver_seconds", "30", "OR-Tools time limit in seconds")
dbutils.widgets.text("avg_speed_kph", "35", "Synthetic average road speed in km/h")
dbutils.widgets.text("depot_lat", "39.7685", "Depot latitude")
dbutils.widgets.text("depot_lon", "-86.1580", "Depot longitude")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
sample_table_name = dbutils.widgets.get("sample_table")
output_table_name = dbutils.widgets.get("output_table")
local_csv_relative_path = dbutils.widgets.get("local_csv_relative_path")
num_shipments = int(dbutils.widgets.get("num_shipments"))
num_routes = int(dbutils.widgets.get("num_routes"))
max_ev = float(dbutils.widgets.get("max_ev"))
max_van = float(dbutils.widgets.get("max_van"))
max_route_seconds = int(dbutils.widgets.get("max_route_seconds"))
solver_seconds = int(dbutils.widgets.get("solver_seconds"))
avg_speed_kph = float(dbutils.widgets.get("avg_speed_kph"))
depot_lat = float(dbutils.widgets.get("depot_lat"))
depot_lon = float(dbutils.widgets.get("depot_lon"))

if num_shipments < 1:
    raise ValueError("num_shipments must be >= 1")
if num_routes < 1:
    raise ValueError("num_routes must be >= 1")
if max_van <= 0:
    raise ValueError("max_van must be positive")
if avg_speed_kph <= 0:
    raise ValueError("avg_speed_kph must be positive")

sample_table = f"{catalog}.{schema}.{sample_table_name}"
output_table = f"{catalog}.{schema}.{output_table_name}"

# COMMAND ----------

# DBTITLE 1,Repo paths and imports
from pathlib import Path
import sys

repo_root = Path.cwd().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import math
import numpy as np
import pandas as pd
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

local_csv_path = (Path.cwd() / local_csv_relative_path).resolve()
print(f"Repo root: {repo_root}")
print(f"Intro sample path: {local_csv_path}")

# COMMAND ----------

# DBTITLE 1,Load repo-local sample
from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

if not local_csv_path.exists():
    raise FileNotFoundError(f"Repo-local shipment sample not found at {local_csv_path}")

shipments_pdf = (
    pd.read_csv(local_csv_path)
    .sort_values("package_id", kind="mergesort")
    .head(num_shipments)
    .reset_index(drop=True)
)
required_columns = {"package_id", "city", "latitude", "longitude", "weight"}
missing_columns = required_columns - set(shipments_pdf.columns)
if missing_columns:
    raise ValueError(f"Intro sample is missing columns: {sorted(missing_columns)}")

shipments_pdf = shipments_pdf.loc[
    shipments_pdf["package_id"].notna()
    & shipments_pdf["latitude"].notna()
    & shipments_pdf["longitude"].notna()
].copy()
shipments_pdf["package_id"] = shipments_pdf["package_id"].astype(str)
shipments_pdf["city"] = shipments_pdf["city"].fillna("").astype(str)
shipments_pdf["latitude"] = shipments_pdf["latitude"].astype(float)
shipments_pdf["longitude"] = shipments_pdf["longitude"].astype(float)
shipments_pdf["weight"] = shipments_pdf["weight"].fillna(0.0).astype(float)

if shipments_pdf.empty:
    raise ValueError("The intro sample did not produce any valid shipments.")

shipments_sdf = spark.createDataFrame(shipments_pdf)
shipments_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    sample_table
)
display(spark.read.table(sample_table).limit(10))

# COMMAND ----------

# DBTITLE 1,Build synthetic travel-time matrix
def haversine_matrix_meters(latitudes: np.ndarray, longitudes: np.ndarray) -> np.ndarray:
    lat_rad = np.radians(latitudes)
    lon_rad = np.radians(longitudes)
    dlat = lat_rad[:, None] - lat_rad[None, :]
    dlon = lon_rad[:, None] - lon_rad[None, :]
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat_rad)[:, None] * np.cos(lat_rad)[None, :] * (
        np.sin(dlon / 2.0) ** 2
    )
    meters = 6371008.8 * (2.0 * np.arcsin(np.minimum(1.0, np.sqrt(a))))
    np.fill_diagonal(meters, 0.0)
    return meters


order_demands = np.ceil(shipments_pdf["weight"].to_numpy(dtype=float)).astype(np.int64)
max_van_capacity = int(math.ceil(max_van))
max_ev_capacity = int(math.ceil(max_ev))

heaviest = int(order_demands.max()) if len(order_demands) else 0
if heaviest > max_van_capacity:
    raise ValueError(
        f"At least one shipment demand ({heaviest}) exceeds max_van ({max_van_capacity})."
    )

min_routes_by_weight = max(1, math.ceil(order_demands.sum() / max_van_capacity))
effective_num_routes = min(
    len(shipments_pdf),
    max(1, max(min(int(num_routes), len(shipments_pdf)), min_routes_by_weight)),
)
if effective_num_routes != num_routes:
    print(
        f"Requested {num_routes} routes but the intro sample requires {effective_num_routes} "
        "vehicle(s) after shipment-count and capacity checks."
    )

all_latitudes = np.concatenate([[depot_lat], shipments_pdf["latitude"].to_numpy(dtype=float)])
all_longitudes = np.concatenate([[depot_lon], shipments_pdf["longitude"].to_numpy(dtype=float)])
distance_meters = haversine_matrix_meters(all_latitudes, all_longitudes)
speed_mps = avg_speed_kph * (1000.0 / 3600.0)
matrix_seconds = np.rint(distance_meters / speed_mps).astype(np.int64)
matrix_seconds = np.maximum(matrix_seconds, 0)
np.fill_diagonal(matrix_seconds, 0)

# COMMAND ----------

# DBTITLE 1,Solve with OR-Tools
n_locations = len(shipments_pdf) + 1
manager = pywrapcp.RoutingIndexManager(n_locations, effective_num_routes, 0)
routing = pywrapcp.RoutingModel(manager)


def transit_cb(from_index: int, to_index: int) -> int:
    from_node = manager.IndexToNode(from_index)
    to_node = manager.IndexToNode(to_index)
    return int(matrix_seconds[from_node, to_node])


def demand_cb(from_index: int) -> int:
    node = manager.IndexToNode(from_index)
    if node == 0:
        return 0
    return int(order_demands[node - 1])


transit_idx = routing.RegisterTransitCallback(transit_cb)
demand_idx = routing.RegisterUnaryTransitCallback(demand_cb)
routing.SetArcCostEvaluatorOfAllVehicles(transit_idx)
routing.AddDimensionWithVehicleCapacity(
    demand_idx,
    0,
    [max_van_capacity] * effective_num_routes,
    True,
    "Capacity",
)
routing.AddDimension(
    transit_idx,
    0,
    max_route_seconds,
    True,
    "Time",
)

search_params = pywrapcp.DefaultRoutingSearchParameters()
search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
search_params.local_search_metaheuristic = (
    routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
)
search_params.time_limit.FromSeconds(max(1, solver_seconds))

solution = routing.SolveWithParameters(search_params)
if solution is None:
    raise ValueError(
        "OR-Tools did not find a feasible intro solution. Increase num_routes, max_van, "
        "or max_route_seconds and rerun."
    )

# COMMAND ----------

# DBTITLE 1,Write intro route table
active_routes: list[list[int]] = []
for vehicle_id in range(effective_num_routes):
    index = routing.Start(vehicle_id)
    route_nodes = []
    while not routing.IsEnd(index):
        node = manager.IndexToNode(index)
        if node != 0:
            route_nodes.append(node)
        index = solution.Value(routing.NextVar(index))
    if route_nodes:
        active_routes.append(route_nodes)

if not active_routes:
    raise ValueError("OR-Tools returned no non-depot intro routes.")

route_rows = []
for route_number, route_nodes in enumerate(active_routes, start=1):
    route_demand = int(order_demands[[node - 1 for node in route_nodes]].sum())
    truck_type = "EV" if route_demand <= max_ev_capacity else "VAN"
    arrival_seconds = 0
    previous_node = 0

    for route_index, node in enumerate(route_nodes):
        arrival_seconds += int(matrix_seconds[previous_node, node])
        shipment = shipments_pdf.iloc[node - 1]
        route_rows.append(
            {
                "cluster_id": f"{route_number:03d}",
                "truck_type": truck_type,
                "route_index": int(route_index),
                "package_id": str(shipment["package_id"]),
                "latitude": float(shipment["latitude"]),
                "longitude": float(shipment["longitude"]),
                "arrival_seconds": int(arrival_seconds),
            }
        )
        previous_node = node

routes_pdf = pd.DataFrame(route_rows).sort_values(
    ["cluster_id", "route_index"], kind="mergesort"
)
routes_sdf = spark.createDataFrame(routes_pdf)
routes_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    output_table
)
display(spark.read.table(output_table))

summary_df = (
    spark.read.table(output_table)
    .groupBy("cluster_id", "truck_type")
    .agg(F.count("*").alias("stops"))
    .orderBy("cluster_id")
)
display(summary_df)

print(f"Wrote intro CPU routes to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Licenses
# MAGIC
# MAGIC © 2026 Databricks, Inc. All rights reserved. The source in this notebook is provided
# MAGIC subject to the Databricks License. All included or referenced third party libraries
# MAGIC are subject to the licenses set forth below.
# MAGIC
# MAGIC | library | description | license | source |
# MAGIC | --- | --- | --- | --- |
# MAGIC | OSRM Backend Server | High performance routing engine written in C++ designed to run on OpenStreetMap data | BSD 2-Clause "Simplified" License | https://github.com/Project-OSRM/osrm-backend |
# MAGIC | Photon | Open-source geocoder for OpenStreetMap data used by the address-to-coordinate stage | Apache License 2.0 | https://github.com/komoot/photon |
# MAGIC | ortools | Operations research tools developed at Google for combinatorial optimization | Apache License 2.0 | https://github.com/google/or-tools |
# MAGIC | folium | Visualize data in Python on interactive Leaflet.js maps | MIT License | https://github.com/python-visualization/folium |
# MAGIC | dash | Python framework for building analytical web applications and dashboards | MIT License | https://github.com/plotly/dash |
# MAGIC | branca | Library for generating complex HTML and JavaScript pages in Python; provides shared helpers for folium | MIT License | https://github.com/python-visualization/branca |
# MAGIC | plotly | Open-source Python library for interactive charts and graphs | MIT License | https://github.com/plotly/plotly.py |
# MAGIC | ray | Flexible distributed execution framework for scaling Python workflows | Apache License 2.0 | https://github.com/ray-project/ray |
# MAGIC | Databricks SDK for Python | Python SDK for Databricks workspace APIs used by the dashboard and setup helper | Apache License 2.0 | https://github.com/databricks/databricks-sdk-py |
# MAGIC | DuckDB | In-process analytical database used to refresh the vendored Overture sample | MIT License | https://github.com/duckdb/duckdb |
# MAGIC | cuOpt | GPU-accelerated combinatorial optimization solver from NVIDIA | Apache License 2.0 | https://docs.nvidia.com/cuopt/user-guide/latest/license.html |
# MAGIC
# MAGIC The vendored address sample derives from Overture open address data. See
# MAGIC `../data/README.md` for source release details and attribution notes.
