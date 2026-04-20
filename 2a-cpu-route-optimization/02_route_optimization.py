# Databricks notebook source
# MAGIC %md
# MAGIC # CPU route optimization
# MAGIC
# MAGIC This notebook uses Ray plus OR-Tools to solve each clustered route independently and
# MAGIC writes the final CPU-optimized routes to `demos.routing.optimized_routes`.

# COMMAND ----------

# DBTITLE 1,Install libraries
# MAGIC %pip install protobuf==6.31.1 ortools==9.14.6206 folium==0.20.0 ray[default]==2.48.0
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("num_shipments", "40000", "Shipment count suffix")
dbutils.widgets.text("max_ev", "4000", "Maximum EV capacity")
dbutils.widgets.text("solver_thinking_time", "20", "Solver thinking multiplier")
dbutils.widgets.text("ray_worker_nodes", "4", "Ray worker nodes")
dbutils.widgets.text("ray_cpus_per_worker", "15", "Ray CPUs per worker")
dbutils.widgets.text("ray_cpus_driver", "15", "Ray CPUs on driver")
dbutils.widgets.text("temp_volume", "routing_assets", "Shared volume for Ray temp data")
dbutils.widgets.text(
    "optimized_routes_table",
    "optimized_routes",
    "Output table name for CPU routes",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
NUM_SHIPMENTS = int(dbutils.widgets.get("num_shipments"))
MAX_EV = float(dbutils.widgets.get("max_ev"))
SOLVER_THINKING_TIME = int(dbutils.widgets.get("solver_thinking_time"))
ray_worker_nodes = int(dbutils.widgets.get("ray_worker_nodes"))
ray_cpus_per_worker = int(dbutils.widgets.get("ray_cpus_per_worker"))
ray_cpus_driver = int(dbutils.widgets.get("ray_cpus_driver"))
temp_volume = dbutils.widgets.get("temp_volume")
optimized_routes_table_name = dbutils.widgets.get("optimized_routes_table")

routing_table = f"{catalog}.{schema}.routing_unified_by_cluster_cpu_{NUM_SHIPMENTS}"
optimized_routes_table = f"{catalog}.{schema}.{optimized_routes_table_name}"

# COMMAND ----------

# DBTITLE 1,Imports
from pathlib import Path
import sys

repo_root = Path.cwd().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import math
import os
import pandas as pd
import ray
from mlflow.utils.databricks_utils import get_databricks_env_vars
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
from pyspark.sql import functions as F
from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster

clustered_df = spark.read.table(routing_table).where("cluster_id is not null")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{temp_volume}`")
ray_temp_dir = f"/Volumes/{catalog}/{schema}/{temp_volume}/ray-temp"
dbutils.fs.mkdirs(ray_temp_dir)

# COMMAND ----------

# DBTITLE 1,Start Ray
for shutdown in [shutdown_ray_cluster, ray.shutdown]:
    try:
        shutdown()
    except Exception:
        pass

mlflow_dbrx_creds = get_databricks_env_vars("databricks")
os.environ["DATABRICKS_HOST"] = mlflow_dbrx_creds["DATABRICKS_HOST"]
os.environ["DATABRICKS_TOKEN"] = mlflow_dbrx_creds["DATABRICKS_TOKEN"]
# Ray's Databricks table writer has used both env var names across releases.
# Set both before the cluster starts so worker processes inherit the temp path.
os.environ["_RAY_UC_VOLUMES_FUSE_TEMP_DIR"] = ray_temp_dir
os.environ["RAY_UC_VOLUMES_FUSE_TEMP_DIR"] = ray_temp_dir

ray_conf = setup_ray_cluster(
    num_worker_nodes=ray_worker_nodes,
    num_cpus_head_node=ray_cpus_driver,
    num_cpus_per_node=ray_cpus_per_worker,
    num_gpus_head_node=0,
    num_gpus_worker_node=0,
)
os.environ["RAY_ADDRESS"] = ray_conf[0]
ray.init(ignore_reinit_error=True)

# COMMAND ----------

# DBTITLE 1,Solver helpers
def add_missing_depot_arcs(pdf: pd.DataFrame) -> pd.DataFrame:
    depot_idx = int(
        pdf.loc[pdf["destination_id"] == "DEPOT", "destination_index"].unique()[0]
    )
    reversed_rows = (
        pdf.loc[
            pdf["destination_index"] == depot_idx,
            ["origin_index", "destination_index", "duration_seconds"],
        ]
        .rename(
            columns={
                "origin_index": "destination_index",
                "destination_index": "origin_index",
            }
        )
    )
    reversed_rows = (
        reversed_rows.merge(
            pdf[["origin_index", "destination_index"]],
            on=["origin_index", "destination_index"],
            how="left",
            indicator=True,
        )
        .loc[lambda frame: frame["_merge"] == "left_only"]
        .drop(columns=["_merge"])
    )
    return pd.concat([pdf, reversed_rows], ignore_index=True)


def solve_single_route_from_matrix(pdf: pd.DataFrame) -> pd.DataFrame:
    cluster_id = str(pdf["cluster_id"].iloc[0])
    total_weight = float(pdf["weight"].fillna(0).sum())
    truck_type = "EV" if total_weight <= MAX_EV else "VAN"

    n_locations = (
        int(max(pdf["origin_index"].max(), pdf["destination_index"].max())) + 1
    )
    pdf_aug = add_missing_depot_arcs(pdf)
    big = 10**5

    matrix_df = (
        pd.pivot_table(
            pdf_aug,
            index="origin_index",
            columns="destination_index",
            values="duration_seconds",
            aggfunc="min",
        )
        .reindex(index=range(n_locations), columns=range(n_locations))
        .fillna(big)
        .astype(float)
    )
    duration_matrix = matrix_df.round().astype(int).values.tolist()

    manager = pywrapcp.RoutingIndexManager(n_locations, 1, 0)
    routing = pywrapcp.RoutingModel(manager)

    def transit_cb(i, j):
        ni = manager.IndexToNode(i)
        nj = manager.IndexToNode(j)
        return duration_matrix[ni][nj]

    transit_idx = routing.RegisterTransitCallback(transit_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_idx)

    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_params.guided_local_search_lambda_coefficient = 0.2
    search_params.time_limit.FromSeconds(min(60, round(math.sqrt(n_locations) * SOLVER_THINKING_TIME)))

    solution = routing.SolveWithParameters(search_params)
    if not solution:
        raise RuntimeError(f"No solution found for cluster {cluster_id}")

    route_pairs = []
    idx = routing.Start(0)
    step = 0
    while not routing.IsEnd(idx):
        current_node = manager.IndexToNode(idx)
        next_idx = solution.Value(routing.NextVar(idx))
        next_node = manager.IndexToNode(next_idx)
        if not routing.IsEnd(next_idx):
            route_pairs.append((step, current_node, next_node, duration_matrix[current_node][next_node]))
            step += 1
        idx = next_idx

    result_df = pd.DataFrame(
        route_pairs,
        columns=["raw_route_index", "origin_index", "destination_index", "duration_seconds"],
    )

    id_lookup = (
        pd.concat(
            [
                pdf[["origin_index", "origin_id"]]
                .drop_duplicates()
                .rename(columns={"origin_index": "idx", "origin_id": "id"}),
                pdf[["destination_index", "destination_id"]]
                .drop_duplicates()
                .rename(columns={"destination_index": "idx", "destination_id": "id"}),
            ]
        )
        .drop_duplicates(subset=["idx"])
        .set_index("idx")["id"]
        .reindex(range(n_locations))
        .to_dict()
    )
    id_lookup[0] = id_lookup.get(0, "DEPOT")

    coord_lookup = (
        pdf[["origin_index", "latitude", "longitude"]]
        .drop_duplicates("origin_index")
        .set_index("origin_index")[["latitude", "longitude"]]
        .reindex(range(n_locations))
        .to_dict(orient="index")
    )
    coord_lookup[0] = coord_lookup.get(0, {"latitude": None, "longitude": None})

    result_df["package_id"] = result_df["origin_index"].map(id_lookup)
    result_df["next_package_id"] = result_df["destination_index"].map(id_lookup)
    result_df["latitude"] = result_df["origin_index"].map(
        lambda i: coord_lookup[i]["latitude"]
    )
    result_df["longitude"] = result_df["origin_index"].map(
        lambda i: coord_lookup[i]["longitude"]
    )

    result_df = result_df.loc[result_df["package_id"] != "DEPOT"].copy().reset_index(
        drop=True
    )
    result_df["route_index"] = range(len(result_df))
    result_df["cluster_id"] = cluster_id
    result_df["truck_type"] = truck_type

    return result_df[
        [
            "cluster_id",
            "truck_type",
            "route_index",
            "package_id",
            "next_package_id",
            "duration_seconds",
            "latitude",
            "longitude",
        ]
    ]

# COMMAND ----------

# DBTITLE 1,Optimize routes with Ray
spark_df = clustered_df.repartition("cluster_id")
# Databricks recommends disabling the Spark chunk transfer path on spot-backed
# clusters because executor cache loss can break `from_spark`.
dataset = ray.data.from_spark(spark_df, use_spark_chunk_api=False)

result_ds = dataset.groupby("cluster_id").map_groups(
    solve_single_route_from_matrix,
    batch_format="pandas",
    num_cpus=1,
)

# COMMAND ----------

# DBTITLE 1,Write results
result_ds.write_databricks_table(
    optimized_routes_table,
    mode="overwrite",
    mergeSchema=True,
)

display(spark.read.table(optimized_routes_table))

# COMMAND ----------

# DBTITLE 1,Shutdown Ray
try:
    shutdown_ray_cluster()
except Exception:
    pass
