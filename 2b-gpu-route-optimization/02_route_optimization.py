# Databricks notebook source
# MAGIC %md
# MAGIC # GPU route optimization with cuOpt
# MAGIC
# MAGIC This notebook converts the sparse OSRM durations from stage 2b into a cuOpt waypoint
# MAGIC graph, solves the multi-vehicle routing problem on a GPU cluster, and writes the final
# MAGIC GPU routes to a table with the same core schema used by the Dash app.

# COMMAND ----------

# DBTITLE 1,Install libraries
# MAGIC %pip install -q folium
# MAGIC %pip install -q --extra-index-url=https://pypi.nvidia.com cuopt-server-cu12 cuopt-sh-client cuopt-cu12==25.8.*
# MAGIC %restart_python

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
        "Increase num_routes, max_route_seconds, or solver_minutes, then rerun stage 2b."
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
