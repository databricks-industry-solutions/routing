# Databricks notebook source
# MAGIC %md
# MAGIC Make sure to use the cluster created in notebook `02_compute_setup`

# COMMAND ----------

# DBTITLE 1,Install Libraries
# MAGIC %pip install protobuf==6.31.1 ortools==9.14.6206 folium==0.20.0 ray[default]==2.41.0 
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Set Configs
NUM_SHIPMENTS = 40_000
num_routes = 320  # total trucks available
MAX_EV = 4000 # max capacity
MAX_VAN = 8000
DEPOT_LAT, DEPOT_LON = 39.7685, -86.1580 # start and end point for each route
SOLVER_THINKING_TIME = 10 # scale how long should the solver spend on each route


catalog = "default"
schema = "routing"
routing_table = f"{catalog}.{schema}.routing_unified_by_cluster"
optimized_routes_table = f"{catalog}.{schema}.optimized_routes"
clustered_df = spark.read.table(routing_table).where("cluster_id is not null")

# COMMAND ----------

# DBTITLE 1,Imports
import random
import math
import pandas as pd
import requests
import numpy as np
import ray
import os
import re
import time
from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster, MAX_NUM_WORKER_NODES
from mlflow.utils.databricks_utils import get_databricks_env_vars
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from ortools.constraint_solver import pywrapcp, routing_enums_pb2, routing_parameters_pb2
from ortools.util import optional_boolean_pb2 as ob

# COMMAND ----------

# DBTITLE 1,Start Ray
# Cluster cleanup
restart = True
if restart is True:
  try:
    shutdown_ray_cluster()
  except Exception:
    pass

  try:
    ray.shutdown()
  except Exception:
    pass

# Set configs based on your cluster size
num_cpu_cores_per_worker = 15 # total cpu to use in each worker node (total_cores - 1 to leave one core for spark)
num_cpus_head_node = 15 # Cores to use in driver node (up to total_cores - 1)

# Set databricks credentials as env vars
mlflow_dbrx_creds = get_databricks_env_vars("databricks")
os.environ["DATABRICKS_HOST"] = mlflow_dbrx_creds['DATABRICKS_HOST']
os.environ["DATABRICKS_TOKEN"] = mlflow_dbrx_creds['DATABRICKS_TOKEN']

ray_conf = setup_ray_cluster(
  # min_worker_nodes=min_node,
  # max_worker_nodes=max_node,
  num_worker_nodes=4,
  num_cpus_head_node= num_cpus_head_node,
  num_cpus_per_node=num_cpu_cores_per_worker,
  num_gpus_head_node=0,
  num_gpus_worker_node=0
)
os.environ['RAY_ADDRESS'] = ray_conf[0]

# COMMAND ----------

# DBTITLE 1,Solver Helper Function
def add_missing_depot_arcs(pdf: pd.DataFrame) -> pd.DataFrame:
    # 1) find the depot's index from the data (it only appears as destination in your case)
    depot_idx = int(pdf.loc[pdf["destination_id"]=="DEPOT", "destination_index"].unique()[0])

    # 2) create reversed rows for arcs that go TO depot (i -> depot) so we also have (depot -> i)
    rev = (pdf.loc[pdf["destination_index"] == depot_idx,
                   ["origin_index","destination_index","duration_seconds"]]
             .rename(columns={
                 "origin_index": "destination_index",
                 "destination_index": "origin_index"
             }))

    # 3) drop any reversed rows that already exist
    rev = (rev.merge(pdf[["origin_index","destination_index"]],
                     on=["origin_index","destination_index"],
                     how="left", indicator=True)
              .loc[lambda d: d["_merge"]=="left_only",
                   ["origin_index","destination_index","duration_seconds"]])

    # 4) append and return
    return pd.concat([pdf, rev], ignore_index=True)

# COMMAND ----------

# DBTITLE 1,Solve Route Function
def solve_single_route_from_matrix(pdf: pd.DataFrame):
    # ---------- 1) Determine N and build N×N duration matrix ----------
    N = int(max(pdf["origin_index"].max(), pdf["destination_index"].max())) + 1
    BIG = 10**5
    pdf_aug = add_missing_depot_arcs(pdf)

    M = pd.pivot_table(
        pdf_aug,
        index="origin_index",
        columns="destination_index",
        values="duration_seconds",
        aggfunc="min"
    ).reindex(index=range(N), columns=range(N))

    M = M.fillna(BIG).astype(float)
    duration_matrix = M.round().astype(int).values.tolist()

    # ---------- 2) OR-Tools model ----------
    manager = pywrapcp.RoutingIndexManager(N, 1, 0)  # depot is 0
    routing = pywrapcp.RoutingModel(manager)

    def transit_cb(i, j):
        ni = manager.IndexToNode(i)
        nj = manager.IndexToNode(j)
        return duration_matrix[ni][nj]

    transit_idx = routing.RegisterTransitCallback(transit_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_idx)

    # ---------- 3) Search params ----------
    sp = pywrapcp.DefaultRoutingSearchParameters()
    sp.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    sp.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    sp.guided_local_search_lambda_coefficient = 0.2
    limit = round(math.sqrt(N) * SOLVER_THINKING_TIME)
    sp.time_limit.FromSeconds(min(60, limit))

    sol = routing.SolveWithParameters(sp)
    if not sol:
        raise RuntimeError("No solution found for this cluster")

    # ---------- 4) Extract route ----------
    route_pairs = []
    idx = routing.Start(0)
    step = 0
    while not routing.IsEnd(idx):
        ni = manager.IndexToNode(idx)
        next_idx = sol.Value(routing.NextVar(idx))
        nj = manager.IndexToNode(next_idx)

        if not routing.IsEnd(next_idx):
            dur = duration_matrix[ni][nj]
            route_pairs.append((step, ni, nj, dur))
            step += 1

        idx = next_idx

    # ---------- 5) Build lookup tables ----------
    id_lookup = (
        pd.concat([
            pdf[["origin_index", "origin_id"]].drop_duplicates()
              .rename(columns={"origin_index": "idx", "origin_id": "id"}),
            pdf[["destination_index", "destination_id"]].drop_duplicates()
              .rename(columns={"destination_index": "idx", "destination_id": "id"})
        ])
        .drop_duplicates(subset=["idx"])
        .set_index("idx")["id"]
        .reindex(range(N))
        .to_dict()
    )
    id_lookup[0] = id_lookup.get(0, "DEPOT")

    # Coordinates (map by origin_index → lat/lon)
    coord_lookup = (
        pdf[["origin_index", "latitude", "longitude"]]
        .drop_duplicates("origin_index")
        .set_index("origin_index")[["latitude", "longitude"]]
        .reindex(range(N))
        .to_dict(orient="index")
    )
    coord_lookup[0] = coord_lookup.get(0, {"latitude": None, "longitude": None})

    # ---------- 6) Assemble output DataFrame ----------
    result_df = pd.DataFrame(route_pairs, columns=["route_index", "origin_index", "destination_index", "duration"])
    result_df["origin_id"] = result_df["origin_index"].map(id_lookup)
    result_df["destination_id"] = result_df["destination_index"].map(id_lookup)
    result_df["latitude"] = result_df["origin_index"].map(lambda i: coord_lookup[i]["latitude"])
    result_df["longitude"] = result_df["origin_index"].map(lambda i: coord_lookup[i]["longitude"])

    result_df = result_df[["route_index", "origin_id", "destination_id", "duration", "latitude", "longitude"]]

    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC Test on a single route. Remember that the depot in downtown Indianapolis (not visualized) is technically the start and end of the route, which is why the routes tend to start and end at otherwise odd locations

# COMMAND ----------

# DBTITLE 1,Solve Sample Route
from utils.plotter import plot_route_folium

SOLVER_THINKING_TIME = 1 # scale how long should the solver spend on each route
pdf = clustered_df.where('cluster_id = 1').toPandas() # TODO: not hardcoeded just get the first one
solved_pdf = solve_single_route_from_matrix(pdf)
print('Route duration in seconds:', sum(solved_pdf['duration']))
route_df = solved_pdf[solved_pdf['origin_id'] != 'DEPOT']

# COMMAND ----------

# DBTITLE 1,Visualize Sample Route
plot_route_folium(route_df) # Remember the depot (not visualized) is technically the start and end of the route

# COMMAND ----------

# DBTITLE 1,Ray Grouping
spark_df = clustered_df.repartition("cluster_id")   
ds = ray.data.from_spark(spark_df)


SOLVER_THINKING_TIME = 20 # scale how long should the solver spend on each route
result_ds = (
    ds.groupby("cluster_id")         # <‑‑ each group = one cluster
      .map_groups(
          solve_single_route_from_matrix,
          batch_format="pandas",
          num_cpus=1,                # reserve one CPU per group
      )
)

# COMMAND ----------

# DBTITLE 1,Optimize and Write Results
# can convert to pandas or write directly to delta 
# result_ds.to_pandas()

volume = "ray_temp"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

tmp_dir_fs = f"/Volumes/{catalog}/{schema}/{volume}/tempDoc"
dbutils.fs.mkdirs(tmp_dir_fs)
os.environ["RAY_UC_VOLUMES_FUSE_TEMP_DIR"] = tmp_dir_fs

result_ds.write_databricks_table(optimized_routes_table, mode="overwrite", mergeSchema=True)

# COMMAND ----------

# DBTITLE 1,Display Results
spark.read.table(optimized_routes_table).display()

# COMMAND ----------

# DBTITLE 1,Update Dashboard
dashboard_filepath = os.path.join(
    os.getcwd(), "Routing Optimization Dashboard.lvdash.json"
)
with open(dashboard_filepath, "r") as f:
    content = f.read()

pattern = re.compile(
    r"FROM\s+([A-Za-z_]\w*)\.([A-Za-z_]\w*)\.([A-Za-z_]\w*)",
    flags=re.IGNORECASE,
)

def _repl(_):
    return f"FROM {catalog}.{schema}.{optimized_routes_table}"
new_content, n_subs = pattern.subn(_repl, content)

with open(dashboard_filepath, "w") as f:
    f.write(new_content)

print(f"✔ Replaced {n_subs} occurrence(s) in {dashboard_filepath}")

# COMMAND ----------

try:
    shutdown_ray_cluster()
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC The *route* and *table* methods of the OSRM Backend Server are two of several methods available through the server's REST API.  The full list of methods include:
# MAGIC </p>
# MAGIC
# MAGIC * [route](http://project-osrm.org/docs/v5.5.1/api/#route-service) - finds the fastest route between coordinates in the supplied order
# MAGIC * [nearest](http://project-osrm.org/docs/v5.5.1/api/#nearest-service) - snaps a coordinate to the street network and returns the nearest n matches
# MAGIC * [table](http://project-osrm.org/docs/v5.5.1/api/#table-service) - computes the duration of the fastest route between all pairs of supplied coordinates
# MAGIC * [match](http://project-osrm.org/docs/v5.5.1/api/#match-service) - snaps given GPS points to the road network in the most plausible way
# MAGIC * [trip](http://project-osrm.org/docs/v5.5.1/api/#trip-service) - solves the Traveling Salesman Problem using a greedy heuristic (farthest-insertion algorithm)
# MAGIC * [tile](http://project-osrm.org/docs/v5.5.1/api/#tile-service) - generates Mapbox Vector Tiles that can be viewed with a vector-tile capable slippy-map viewer
# MAGIC
# MAGIC To make any of these accessible during Spark dataframe processing, simply construct a function around the HTTP REST API call and call via Pandas UDFs or Ray as demonstrated above.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC | library                | description                                                                                      | license      | source                                                    |
# MAGIC |------------------------|--------------------------------------------------------------------------------------------------|--------------|-----------------------------------------------------------|
# MAGIC | OSRM Backend Server    | High performance routing engine written in C++14 designed to run on OpenStreetMap data           | BSD 2-Clause "Simplified" License | https://github.com/Project-OSRM/osrm-backend              |
# MAGIC | osmnx                  | Download, model, analyze, and visualize street networks and other geospatial features from OpenStreetMap in Python | MIT License  | https://github.com/gboeing/osmnx                          |
# MAGIC | ortools                | Operations research tools developed at Google for combinatorial optimization                     | Apache License 2.0 | https://github.com/google/or-tools                        |
# MAGIC | folium                 | Visualize data in Python on interactive Leaflet.js maps                                          | MIT License  | https://github.com/python-visualization/folium            |
# MAGIC | dash                   | Python framework for building analytical web applications and dashboards; built on Flask, React, and Plotly.js | MIT License  | https://github.com/plotly/dash                            |
# MAGIC | branca                 | Library for generating complex HTML+JS pages in Python; provides non-map-specific features for folium | MIT License  | https://github.com/python-visualization/branca            |
# MAGIC | plotly                 | Open-source Python library for creating interactive, publication-quality charts and graphs        | MIT License  | https://github.com/plotly/plotly.py                       |
# MAGIC ray |	Flexible, high-performance distributed execution framework for scaling Python workflows |	Apache2.0 |	https://github.com/ray-project/ray
