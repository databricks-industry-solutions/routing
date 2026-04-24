# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2b-3: GPU route optimization with cuOpt
# MAGIC
# MAGIC This notebook converts the sparse OSRM durations from stage 2b into a cuOpt waypoint
# MAGIC graph, solves the multi-vehicle routing problem on a GPU cluster, and writes the final
# MAGIC GPU routes to a table with the same core schema used by the Dash app.
# MAGIC
# MAGIC Recommended compute: serverless GPU notebook on an A10-backed environment. The
# MAGIC bundle runs this on the `serverless_gpu` environment (version 4).

# COMMAND ----------

# DBTITLE 1,Install cuOpt for serverless GPU jobs
# MAGIC %pip install -q --extra-index-url=https://pypi.nvidia.com cuopt-cu12==25.8.0 nvidia-nccl-cu12==2.26.2

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("shipments_table", "raw_shipments", "Input shipments table name")
dbutils.widgets.text("num_shipments", "10000", "Shipment count suffix")
dbutils.widgets.text("num_routes", "80", "Number of vehicles")
dbutils.widgets.text("max_ev", "4000", "Maximum EV capacity")
dbutils.widgets.text("max_van", "8000", "Maximum van capacity")
dbutils.widgets.text("max_route_seconds", "32400", "Maximum route seconds")
dbutils.widgets.text("solver_minutes", "20", "cuOpt solver time limit in minutes")
dbutils.widgets.text(
    "optimized_routes_table",
    "optimized_routes_gpu_10000",
    "Output table name for GPU routes",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
shipments_table_name = dbutils.widgets.get("shipments_table")
NUM_SHIPMENTS = int(dbutils.widgets.get("num_shipments"))
NUM_ROUTES = int(dbutils.widgets.get("num_routes"))
MAX_EV = float(dbutils.widgets.get("max_ev"))
MAX_VAN = float(dbutils.widgets.get("max_van"))
MAX_ROUTE_SECONDS = int(dbutils.widgets.get("max_route_seconds"))
SOLVER_MINUTES = int(dbutils.widgets.get("solver_minutes"))
optimized_routes_table_name = dbutils.widgets.get("optimized_routes_table")

shipments_table = f"{catalog}.{schema}.{shipments_table_name}"
mapping_table = f"{catalog}.{schema}.shipment_ids_map_gpu_{NUM_SHIPMENTS}"
distances_table = f"{catalog}.{schema}.distances_by_route_gpu_{NUM_SHIPMENTS}"
optimized_routes_table = f"{catalog}.{schema}.{optimized_routes_table_name}"

# COMMAND ----------

# DBTITLE 1,Imports
import ctypes
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path


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

import cudf
import numpy as np
import pandas as pd
from cuopt import distance_engine, routing
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Prepare sparse graph
DEPOT_ID = 0

distances_df = spark.read.table(distances_table).select(
    "global_idx_source", "global_idx_dest", "duration_seconds"
)

rev_to_depot = (
    distances_df.where(F.col("global_idx_dest") == DEPOT_ID)
    .select(
        F.lit(DEPOT_ID).alias("global_idx_source"),
        F.col("global_idx_source").alias("global_idx_dest"),
        F.col("duration_seconds"),
    )
)

distances_df = distances_df.unionByName(rev_to_depot)

pdf = (
    distances_df.select(
        "global_idx_source",
        "global_idx_dest",
        F.round("duration_seconds").cast("int").alias("duration_seconds"),
    ).toPandas()
)
if pdf.empty:
    raise ValueError(
        f"{distances_table} is empty. Run the GPU distance calculation stage successfully first."
    )

all_nodes = pd.Index(
    sorted(
        pd.unique(
            pd.concat([pdf["global_idx_source"], pdf["global_idx_dest"]], ignore_index=True)
        )
    )
)
node2pos = {int(global_idx): i for i, global_idx in enumerate(all_nodes)}

pdf["src_idx"] = pdf["global_idx_source"].map(node2pos).astype(np.int32)
pdf["dst_idx"] = pdf["global_idx_dest"].map(node2pos).astype(np.int32)
pdf["cost"] = pdf["duration_seconds"].astype(np.int32)
pdf = pdf.sort_values(["src_idx", "dst_idx"], kind="mergesort")

indices = pdf["dst_idx"].to_numpy(dtype=np.int32)
weights = pdf["cost"].to_numpy(dtype=np.int32)
counts = (
    pdf.groupby("src_idx").size().reindex(range(len(all_nodes)), fill_value=0).to_numpy(dtype=np.int32)
)
offsets = np.concatenate([[0], np.cumsum(counts, dtype=np.int64)]).astype(np.int32)

waypoint_graph = distance_engine.WaypointMatrix(offsets, indices, weights)
order_globals = [int(node) for node in all_nodes if int(node) != DEPOT_ID]
targets = np.array([node2pos[DEPOT_ID]] + [node2pos[node] for node in order_globals], dtype=np.int32)

cost = waypoint_graph.compute_cost_matrix(targets)
time = cost.copy(deep=True)

weight_lookup = (
    spark.read.table(mapping_table)
    .select(
        F.col("package_id").cast("string").alias("package_id"),
        F.col("global_idx").cast("int").alias("global_idx"),
    )
    .join(
        spark.read.table(shipments_table).select(
            F.col("package_id").cast("string").alias("package_id"),
            F.col("weight").cast("double").alias("weight"),
        ),
        on="package_id",
        how="left",
    )
    .select(
        "global_idx",
        F.ceil(F.coalesce(F.col("weight"), F.lit(0.0))).cast("int").alias("demand"),
    )
    .toPandas()
    .set_index("global_idx")["demand"]
    .to_dict()
)

# COMMAND ----------

# DBTITLE 1,Solve with cuOpt
n_locations = len(targets)
n_orders = len(order_globals)
if n_orders == 0:
    raise ValueError(
        f"{distances_table} did not contain any non-depot orders to optimize."
    )

order_demand = np.array(
    [int(weight_lookup.get(global_idx, 0)) for global_idx in order_globals],
    dtype=np.int32,
)

max_van_capacity = int(MAX_VAN)
if max_van_capacity <= 0:
    raise ValueError("max_van must be positive.")
heaviest = int(order_demand.max()) if n_orders else 0
if heaviest > max_van_capacity:
    raise ValueError(
        f"At least one shipment demand ({heaviest}) exceeds max_van ({max_van_capacity})."
    )

effective_num_routes = min(NUM_ROUTES, n_orders)
if effective_num_routes != NUM_ROUTES:
    print(
        f"Requested {NUM_ROUTES} vehicles but only found {n_orders} order(s); "
        f"solving with {effective_num_routes} vehicle(s) instead."
    )

data_model = routing.DataModel(n_locations, effective_num_routes, n_orders)
data_model.set_vehicle_locations(
    cudf.Series([0] * effective_num_routes),
    cudf.Series([0] * effective_num_routes),
)
data_model.set_order_locations(cudf.Series(np.arange(1, n_locations, dtype=np.int32)))
data_model.set_vehicle_max_times(
    cudf.Series(np.full(effective_num_routes, MAX_ROUTE_SECONDS, dtype=np.float32))
)
data_model.add_cost_matrix(cost)
data_model.add_transit_time_matrix(time)
data_model.add_capacity_dimension(
    "weight",
    cudf.Series(order_demand),
    cudf.Series(np.full(effective_num_routes, max_van_capacity, dtype=np.int32)),
)
data_model.set_min_vehicles(effective_num_routes)

solver_settings = routing.SolverSettings()
solver_settings.set_verbose_mode(True)
solver_settings.set_time_limit(SOLVER_MINUTES * 60)

solution = routing.Solve(data_model, solver_settings)
solution_status = solution.get_status()
solution_message = solution.get_message()
print(f"cuOpt status: {solution_status}")
print(f"cuOpt message: {solution_message}")
route_pdf = solution.get_route().to_pandas()
if route_pdf.empty:
    raise ValueError(
        "cuOpt returned no routes for the current GPU optimization inputs "
        f"(status={solution_status}, message={solution_message}). "
        "Increase num_routes, max_route_seconds, or solver_minutes, then rerun stage 2b-3."
    )

# COMMAND ----------

# DBTITLE 1,Map back to package ids
mapping_pdf = pd.DataFrame(
    {
        "location": np.arange(len(targets), dtype=np.int32),
        "full_pos": targets,
        "global_idx": all_nodes.take(targets),
    }
)

routes_out = route_pdf.merge(mapping_pdf, on="location", how="left")
routes_sdf = spark.createDataFrame(routes_out)

shipment_idx_df = (
    spark.read.table(mapping_table)
    .where(F.col("package_id") != "DEPOT")
    .select("package_id", "global_idx")
    .distinct()
)

shipment_details_df = (
    spark.read.table(shipments_table)
    .select("package_id", "latitude", "longitude", "weight")
    .join(shipment_idx_df, on="package_id", how="inner")
)

final_routes_df = routes_sdf.join(shipment_details_df, on="global_idx", how="inner")
truck_weights_df = final_routes_df.groupBy("truck_id").agg(
    F.sum("weight").alias("total_weight")
)

route_window = Window.partitionBy("truck_id").orderBy("arrival_stamp")
optimized_routes_df = (
    final_routes_df.join(truck_weights_df, on="truck_id", how="left")
    .withColumn(
        "truck_type",
        F.when(F.col("total_weight") <= MAX_EV, F.lit("EV")).otherwise(F.lit("VAN")),
    )
    .withColumn("route_index", F.row_number().over(route_window) - 1)
    .select(
        F.col("truck_id").cast("string").alias("cluster_id"),
        "truck_type",
        "route_index",
        "package_id",
        "latitude",
        "longitude",
        "arrival_stamp",
    )
    .orderBy("cluster_id", "route_index")
)

optimized_routes_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    optimized_routes_table
)
display(spark.read.table(optimized_routes_table))

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
