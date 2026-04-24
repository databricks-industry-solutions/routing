# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction: serverless GPU routing
# MAGIC
# MAGIC This notebook is the smallest GPU path in the accelerator. It reads the checked-in
# MAGIC `utils/shipments.csv` sample, builds a dense haversine travel-time matrix, solves a
# MAGIC small capacity-constrained VRP with cuOpt on serverless GPU, and writes a route
# MAGIC table without Photon, OSRM, init scripts, or the sparse graph prep used later in the
# MAGIC full workflow.
# MAGIC
# MAGIC Recommended compute: serverless GPU notebook on an A10-backed environment. The
# MAGIC bundle runs this as `intro_gpu` on the serverless GPU environment (version 4).

# COMMAND ----------

# DBTITLE 1,Install cuOpt for serverless GPU jobs
# MAGIC %pip install -q --extra-index-url=https://pypi.nvidia.com cuopt-cu12==25.8.0 nvidia-nccl-cu12==2.26.2

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("sample_table", "intro_shipments", "Prepared intro shipment table")
dbutils.widgets.text("output_table", "intro_routes_gpu", "Output intro GPU routes table")
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
dbutils.widgets.text("solver_minutes", "2", "cuOpt solver time limit in minutes")
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
solver_minutes = int(dbutils.widgets.get("solver_minutes"))
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
import ctypes
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
import sys


def preload_nccl_libraries() -> None:
    try:
        nccl_dist = distribution("nvidia-nccl-cu12")
    except PackageNotFoundError:
        return

    for relative_path in nccl_dist.files or []:
        if str(relative_path).endswith("libnccl.so.2"):
            resolved = Path(nccl_dist.locate_file(relative_path)).resolve()
            ctypes.CDLL(str(resolved), mode=ctypes.RTLD_GLOBAL)


def patch_local_cuda_cluster() -> None:
    try:
        import dask_cuda
        from dask_cuda import local_cuda_cluster
    except ImportError:
        return

    original_cluster = local_cuda_cluster.LocalCUDACluster

    class LocalhostTcpCUDACluster(original_cluster):
        def __init__(self, *args, **kwargs):
            kwargs["host"] = kwargs.get("host") or "127.0.0.1"
            kwargs["protocol"] = "tcp"
            kwargs["enable_tcp_over_ucx"] = False
            kwargs["enable_nvlink"] = False
            kwargs["enable_infiniband"] = False
            kwargs["enable_rdmacm"] = False
            super().__init__(*args, **kwargs)

    dask_cuda.LocalCUDACluster = LocalhostTcpCUDACluster
    local_cuda_cluster.LocalCUDACluster = LocalhostTcpCUDACluster


# Serverless GPU installs NCCL inside the Python environment rather than on the default
# loader path, and cuOpt uses LocalCUDACluster internally on single-node runs.
preload_nccl_libraries()
patch_local_cuda_cluster()

repo_root = Path.cwd().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import math
import cudf
import numpy as np
import pandas as pd
from cuopt import routing

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

# DBTITLE 1,Build dense cost matrix
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


order_demands = np.ceil(shipments_pdf["weight"].to_numpy(dtype=float)).astype(np.int32)
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
matrix_seconds = np.rint(distance_meters / speed_mps).astype(np.int32)
matrix_seconds = np.maximum(matrix_seconds, 0)
np.fill_diagonal(matrix_seconds, 0)

cost_matrix = cudf.DataFrame(matrix_seconds.astype(np.float32))
time_matrix = cudf.DataFrame(matrix_seconds.astype(np.float32))

# COMMAND ----------

# DBTITLE 1,Solve with cuOpt
n_locations = len(shipments_pdf) + 1
n_orders = len(shipments_pdf)

data_model = routing.DataModel(n_locations, effective_num_routes, n_orders)
data_model.set_vehicle_locations(
    cudf.Series([0] * effective_num_routes, dtype="int32"),
    cudf.Series([0] * effective_num_routes, dtype="int32"),
)
data_model.set_order_locations(cudf.Series(np.arange(1, n_locations, dtype=np.int32)))
data_model.set_vehicle_max_times(
    cudf.Series(np.full(effective_num_routes, max_route_seconds, dtype=np.float32))
)
data_model.add_cost_matrix(cost_matrix)
data_model.add_transit_time_matrix(time_matrix)
data_model.add_capacity_dimension(
    "weight",
    cudf.Series(order_demands),
    cudf.Series(np.full(effective_num_routes, max_van_capacity, dtype=np.int32)),
)

solver_settings = routing.SolverSettings()
solver_settings.set_verbose_mode(True)
solver_settings.set_time_limit(max(1, solver_minutes) * 60)

solution = routing.Solve(data_model, solver_settings)
status = solution.get_status()
message = solution.get_message()
print(f"cuOpt status: {status}")
print(f"cuOpt message: {message}")

route_pdf = solution.get_route().to_pandas()
if route_pdf.empty:
    raise ValueError(
        "cuOpt returned no intro GPU routes. Increase num_routes, max_van, or "
        "max_route_seconds and rerun."
    )

# COMMAND ----------

# DBTITLE 1,Write intro route table
mapping_pdf = pd.DataFrame(
    {
        "location": np.arange(n_locations, dtype=np.int32),
        "package_id": ["DEPOT"] + shipments_pdf["package_id"].tolist(),
        "latitude": [depot_lat] + shipments_pdf["latitude"].tolist(),
        "longitude": [depot_lon] + shipments_pdf["longitude"].tolist(),
        "weight": [0.0] + shipments_pdf["weight"].tolist(),
    }
)

routes_pdf = route_pdf.merge(mapping_pdf, on="location", how="left")
routes_pdf = routes_pdf.loc[routes_pdf["package_id"] != "DEPOT"].copy()
if routes_pdf.empty:
    raise ValueError("cuOpt produced only depot rows for the intro GPU solve.")

routes_pdf = routes_pdf.sort_values(
    ["truck_id", "arrival_stamp", "location"], kind="mergesort"
).reset_index(drop=True)

truck_weights = routes_pdf.groupby("truck_id")["weight"].sum().to_dict()
truck_order = sorted(routes_pdf["truck_id"].unique())
truck_id_map = {truck_id: f"{position:03d}" for position, truck_id in enumerate(truck_order, 1)}

routes_pdf["cluster_id"] = routes_pdf["truck_id"].map(truck_id_map)
routes_pdf["truck_type"] = routes_pdf["truck_id"].map(
    lambda truck_id: "EV" if truck_weights.get(truck_id, 0.0) <= max_ev_capacity else "VAN"
)
routes_pdf["route_index"] = routes_pdf.groupby("truck_id").cumcount()

output_pdf = routes_pdf[
    [
        "cluster_id",
        "truck_type",
        "route_index",
        "package_id",
        "latitude",
        "longitude",
        "arrival_stamp",
    ]
].copy()

output_sdf = spark.createDataFrame(output_pdf)
output_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
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

print(f"Wrote intro GPU routes to {output_table}")

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
