# Databricks notebook source
# MAGIC %md
# MAGIC Make sure to use the cluster created in notebook `02_compute_setup`

# COMMAND ----------

# DBTITLE 1,Install Libraries
# MAGIC %pip install osmnx==2.0.4 protobuf==6.31.1 ortools==9.14.6206 folium==0.20.0 ray[default]==2.41.0 
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Set Configs
NUM_SHIPMENTS = 40_000
num_routes = 320  # total trucks available
MAX_EV = 4000 # max capacity
MAX_VAN = 8000
DEPOT_LAT, DEPOT_LON = 39.7685, -86.1580 # start and end point for each route
SOLVER_THINKING_TIME = 2 # how long should the solver spend on each route


catalog = "default"
schema = "routing"
shipments_table = f"{catalog}.{schema}.raw_shipments"
clustered_table = f"{catalog}.{schema}.shipments_by_route"
optimal_routes_table = f"{catalog}.{schema}.optimized_routes"

# COMMAND ----------

# DBTITLE 1,Imports
import osmnx as ox
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
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
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
num_cpus_head_node = 8 # Cores to use in driver node (up to total_cores - 1)

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

# DBTITLE 1,Read Shipment Data
try:
    shipments_df = spark.read.table(shipments_table) 
except Exception:
    # to see how this was generated, or generate different data yourself, check utils/data_generation.py
    sample_path = os.getcwd() + "/utils/shipments.csv"
    if not sample_path.startswith("file:"):
        sample_path = f"file:{sample_path}"
    pdf = pd.read_csv(sample_path)
    shipments_df = spark.createDataFrame(pdf) 
    shipments_df.write.mode("overwrite").saveAsTable(shipments_table)

# COMMAND ----------

# DBTITLE 1,Cluster Shipments
# 1. Helper: two-way median split for overweight routes
def median_bisect(sdf, cid, max_weight):
    # choose the axis with larger spread
    bounds = (sdf.agg(F.max("latitude").alias("lat_max"),
                      F.min("latitude").alias("lat_min"),
                      F.max("longitude").alias("lon_max"),
                      F.min("longitude").alias("lon_min"))
                  .collect()[0])
    lat_range = bounds.lat_max - bounds.lat_min
    lon_range = bounds.lon_max - bounds.lon_min
    axis      = "latitude" if lat_range >= lon_range else "longitude"

    # median along that axis (1-% error is fine)
    median = sdf.approxQuantile(axis, [0.5], 0.01)[0]

    # assign sub-cluster ids
    split_df = sdf.withColumn("cluster_id",
        F.when(F.col(axis) <= median, F.lit(f"{cid}-1"))
         .otherwise(                F.lit(f"{cid}-2"))
    )
    return split_df

# 2. Main builder
def build_clusters(shipments_df, num_routes, max_weight=MAX_VAN):
    # 2.1 initial spatial k-means
    vec = VectorAssembler(inputCols=["latitude", "longitude"],
                          outputCol="features").transform(shipments_df)

    base_model = KMeans(k=num_routes, seed=1).fit(vec.select("features"))
    clustered  = (
        base_model.transform(vec)
                  .withColumnRenamed("prediction", "cluster_id")
                  .drop("features")                    # no vector in final table
    )

    # 2.2 overweight cluster ids
    heavy_ids = (
        clustered.groupBy("cluster_id")
                 .agg(F.sum("weight").alias("total_wt"))
                 .filter(F.col("total_wt") > max_weight)
                 .select("cluster_id")
                 .distinct()
                 .collect()
    )
    heavy_ids = [row["cluster_id"] for row in heavy_ids]
    print("Over-capacity clusters →", heavy_ids or "none 🎉")

    from functools import reduce

    # 2.3 bisect each heavy cluster
    if heavy_ids:
        heavy_df = clustered.filter(F.col("cluster_id").isin(heavy_ids))
        keep_df  = clustered.filter(~F.col("cluster_id").isin(heavy_ids))

        # create one DataFrame per overweight cluster
        splits = [
            median_bisect(
                heavy_df.filter(F.col("cluster_id") == cid), cid, max_weight
            )
            for cid in heavy_ids
        ]

        # fold the list into a single DF: split_df = splits[0] ∪ splits[1] ∪ …
        split_df = reduce(lambda d1, d2: d1.unionByName(d2), splits)

        # final merged result
        clustered = keep_df.unionByName(split_df)

    # 2.4 cast id → string, save & return
    return clustered.withColumn("cluster_id", F.col("cluster_id").cast("string"))

# 3. Run, save, and sanity check
try :
    clustered_df = spark.read.table(clustered_table)
except Exception:
    clustered_df = build_clusters(
        shipments_df,
        num_routes=num_routes, 
        max_weight=MAX_VAN,
    )
    clustered_df.write.mode("overwrite").saveAsTable(clustered_table)

(
    clustered_df.groupBy("cluster_id")
    .agg(F.count("*").alias("num_deliveries"),
        F.sum("weight").alias("total_weight"))
    .orderBy(F.col("total_weight").desc())
).display()

# COMMAND ----------

# DBTITLE 1,Define Route Optimizer
def solve_cluster(pdf: pd.DataFrame) -> pd.DataFrame:
    # -------- 1. Build coordinate list -------------------------------------
    coords = [(DEPOT_LON, DEPOT_LAT)] + list(zip(pdf["longitude"], pdf["latitude"]))
    n      = len(coords)

    # -------- 2. Trivial cases ---------------------------------------------
    if n == 1:      # depot only  → no route rows
        return pd.DataFrame([], columns=[
            "cluster_id","truck_type","route_index",
            "package_id","latitude","longitude"
        ])

    if n == 2:      # depot + one stop
        r = pdf.iloc[0]
        return pd.DataFrame([{
            "cluster_id":  str(r["cluster_id"]),
            "truck_type":  "EV" if r["weight"] <= MAX_EV else "Van",
            "route_index": 0,
            "package_id":  r["package_id"],
            "latitude":    r["latitude"],
            "longitude":   r["longitude"],
        }])

    # -------- 3. Single OSRM /table call -----------------------------------
    coord_str = ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in coords)

    # can make this a ray actor if needed
    def osrm_table(coords, base_url="http://localhost:5000", retries=4, timeout_read=120):
        coord_str = ";".join([f"{x[0]},{x[1]}" for x in coords])
        url = f"{base_url}/table/v1/driving/{coord_str}"
        for attempt in range(retries):
            try:
                r = requests.get(url,
                                params={"annotations": "duration,distance"},
                                timeout=(3.05, timeout_read))
                r.raise_for_status()
                return r.json()
            except requests.exceptions.ReadTimeout as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)    # back-off
                else:
                    raise

    def osrm_table(radius=None):
        extra = ""
        if radius is not None:                         # optional wider snap
            radii = ";".join([str(radius)] * n)
            extra = f"&radiuses={radii}"
        url = (
            f"http://localhost:5000/table/v1/driving/{coord_str}"
            f"?annotations=duration{extra}"
        )
        retries = 5
        for attempt in range(retries):
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.ReadTimeout as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)    # back-off
                else:
                    raise

    data = osrm_table()          # first try (server default radius 100 m)
    if data.get("code") != "Ok":
        data = osrm_table(radius=400)  # fallback for off-graph points

    if data.get("code") != "Ok" or "durations" not in data:
        raise RuntimeError(f"OSRM error: {data}")

    full_dur = (np.array(data["durations"], dtype=float)).astype(int)

    # -------- 4. OR-Tools model --------------------------------------------
    demands  = [0] + pdf["weight"].tolist()
    capacity = MAX_VAN if sum(demands) > MAX_EV else MAX_EV

    manager  = pywrapcp.RoutingIndexManager(n, 1, 0)
    routing  = pywrapcp.RoutingModel(manager)

    def cost_cb(i, j):
        return full_dur[manager.IndexToNode(i)][manager.IndexToNode(j)]

    routing.SetArcCostEvaluatorOfAllVehicles(routing.RegisterTransitCallback(cost_cb))

    demand_cb = routing.RegisterUnaryTransitCallback(
        lambda idx: demands[manager.IndexToNode(idx)]
    )
    print("demands :", demands)              # list delivered at each stop
    print("total   :", sum(demands))         # vehicle must deliver at least this
    print("capacity:", capacity)             # max vehicle load you allow
    print("max indiv demand:", max(demands))

    # -------- 5. Search parameters ----------------------------------------
    sp = pywrapcp.DefaultRoutingSearchParameters()
    sp.first_solution_strategy    = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    sp.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH # try other heuristics
    # sp.time_limit.seconds = min(30, n)
    sp.solution_limit = n*10*SOLVER_THINKING_TIME
    sp.guided_local_search_lambda_coefficient = 0.2
    sp.log_search = True
    # ops = sp.local_search_operators
    # ops.use_full_path_lns  = ob.BOOL_TRUE
    # ops.use_two_opt        = ob.BOOL_TRUE
    # ops.use_or_opt         = ob.BOOL_TRUE
    # ops.use_lin_kernighan  = ob.BOOL_TRUE   

    # -------- 6. Solve & extract route -------------------------------------
    sol = routing.SolveWithParameters(sp)
    if sol is None:
        raise RuntimeError("No solution found for cluster")

    idx, visit = routing.Start(0), []
    while not routing.IsEnd(idx):
        node = manager.IndexToNode(idx)
        if node != 0:
            visit.append(node - 1)
        idx = sol.Value(routing.NextVar(idx))

    visited_pdf = pdf.iloc[visit]
    truck_type  = "EV" if capacity > MAX_EV else "Van"

    rows = []
    for i, (_, r) in enumerate(visited_pdf.iterrows()):
        rows.append({
            "cluster_id":  str(r["cluster_id"]),
            "truck_type":  truck_type,
            "route_index": i,        
            "package_id":  r["package_id"],
            "latitude":    r["latitude"],
            "longitude":   r["longitude"],
        })

    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC Test on a small example. Remember that the depot in downtown Indianapolis (not visualized) is technically the start and end of the route, which is why the routes tend to start and end at seemingly odd locations

# COMMAND ----------

# DBTITLE 1,Small Example
from utils.plotter import plot_route_folium

small_example = [
    # cluster_id, package_id, latitude,  longitude, weight
    (1, "N1", 39.8184, -86.1581, 12.0),
    (1, "N2", 39.8584, -86.2081,  8.5),
    (1, "S1", 39.7184, -86.1581, 15.0),
    (1, "S2", 39.6784, -86.1081,  6.0),
    (1, "S3", 39.6584, -86.2081, 10.0),
]

df_schema = StructType([
    StructField("cluster_id",  StringType(), False),
    StructField("package_id",  StringType(),  False),
    StructField("latitude",    DoubleType(),  False),
    StructField("longitude",   DoubleType(),  False),
    StructField("weight",  DoubleType(),  False),
])

small_example_pdf = spark.createDataFrame(small_example, df_schema).toPandas()
result_pdf = solve_cluster(small_example_pdf)
plot_route_folium(result_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Test on a single cluster. Remember that the depot in downtown Indianapolis (not visualized) is technically the start and end of the route, which is why the routes tend to start and end at seemingly odd locations

# COMMAND ----------

# DBTITLE 1,One Route Example
single_cluster_df = clustered_df.where(clustered_df.cluster_id == 1).toPandas()
result_df = solve_cluster(single_cluster_df)
plot_route_folium(result_df) # Remember the depot (not visualized) is technically the start and end of the route

# COMMAND ----------

# DBTITLE 1,Ray Grouping
spark_df = clustered_df.repartition("cluster_id")   
ds = ray.data.from_spark(spark_df)

result_ds = (
    ds.groupby("cluster_id")         # <‑‑ each group = one cluster
      .map_groups(
          solve_cluster,
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

result_ds.write_databricks_table(optimal_routes_table, mode="overwrite", mergeSchema=True)

# COMMAND ----------

# DBTITLE 1,Display Results
spark.read.table(optimal_routes_table).display()

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
    return f"FROM {catalog}.{schema}.{optimal_routes_table}"
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