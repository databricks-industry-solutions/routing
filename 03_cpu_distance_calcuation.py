# Databricks notebook source
# MAGIC %md
# MAGIC Make sure to use the cluster created in notebook `02_compute_setup`

# COMMAND ----------

# DBTITLE 1,Set Configs
NUM_SHIPMENTS = 40_000
num_routes = 320  # total trucks available
MAX_EV = 4000 # max capacity
MAX_VAN = 8000
DEPOT_LAT, DEPOT_LON = 39.7685, -86.1580 # start and end point for each route


catalog = "default"
schema = "routing"
shipments_table = f"{catalog}.{schema}.raw_shipments"
clustered_table = f"{catalog}.{schema}.shipments_by_route"
distances_table = f"{catalog}.{schema}.distances_by_route"
routing_table = f"{catalog}.{schema}.routing_unified_by_cluster"

# COMMAND ----------

# DBTITLE 1,Imports
import random
import math
import pandas as pd
import requests
import numpy as np
import os
import re
import time
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# DBTITLE 1,Read Shipment Data
if spark.catalog.tableExists(shipments_table):
    shipments_df = spark.read.table(shipments_table) 
else:
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
if spark.catalog.tableExists(clustered_table):
    clustered_df = spark.read.table(clustered_table)
else:
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

def get_driving_times(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Given a cluster of shipments as a pandas DataFrame with columns:
        - package_id
        - latitude
        - longitude
    returns a DataFrame of OSRM driving times (seconds) between every pair of points
    (including the depot as the first point).

    Output columns:
        - origin_id        (str; "DEPOT" for the depot, else package_id)
        - destination_id   (str; "DEPOT" for the depot, else package_id)
        - origin_index     (int; 0-based index in the OSRM matrix)
        - destination_index(int)
        - duration_seconds (float; NaN if unreachable)
    """
    # 1) Build coordinate list: depot first, then shipments (OSRM expects lon,lat)
    coords = [(DEPOT_LON, DEPOT_LAT)] + list(zip(pdf["longitude"].tolist(), pdf["latitude"].tolist()))
    n = len(coords)

    # Short-circuit trivial cases
    if n == 0:
        # No depot defined would be unexpected; return empty
        return pd.DataFrame(columns=["origin_id","destination_id","origin_index","destination_index","duration_seconds"])
    if n == 1:
        # Depot only → no pairwise trips
        return pd.DataFrame(columns=["origin_id","destination_id","origin_index","destination_index","duration_seconds"])

    # 2) Helper for OSRM /table call (with optional radius widening)
    coord_str = ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in coords)

    def osrm_table(radius=None):
        extra = ""
        if radius is not None:
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
            except requests.exceptions.ReadTimeout:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # exponential backoff
                else:
                    raise

    # 3) Query OSRM (fallback to wider snap if needed)
    data = osrm_table()
    if data.get("code") != "Ok":
        data = osrm_table(radius=400)

    if data.get("code") != "Ok" or "durations" not in data:
        raise RuntimeError(f"OSRM error: {data}")

    # 4) Build tidy pairwise table from the duration matrix
    # OSRM returns seconds as floats; can include null for unreachable.
    mat = np.array(data["durations"], dtype=float)  # shape (n, n)

    # IDs list aligned with the OSRM order
    ids = ["DEPOT"] + [str(x) for x in pdf["package_id"].tolist()]

    rows = []
    for i in range(n):
        for j in range(n):
            if i == j:
                continue  # usually not useful; remove if you prefer to keep zeros
            dur = mat[i, j]
            # Keep as float so NaN (unreachable) is preserved
            rows.append({
                "origin_id":          ids[i],
                "destination_id":     ids[j],
                "origin_index":       i,
                "destination_index":  j,
                "duration_seconds":   float(dur) if np.isfinite(dur) else np.nan,
            })

    return pd.DataFrame(rows)

# COMMAND ----------

cluster1 = clustered_df.where("cluster_id = 1").toPandas()
display(get_driving_times(cluster1))

# COMMAND ----------

driving_distances_df = clustered_df.groupBy("cluster_id").applyInPandas(
    get_driving_times, 
    schema="""
        origin_id STRING,
        destination_id STRING,
        origin_index INT,
        destination_index INT,
        duration_seconds DOUBLE
    """
)
driving_distances_df.write.saveAsTable(distances_table)
driving_distances_df = spark.read.table(distances_table)
driving_distances_df.display()

# COMMAND ----------

driving_distances_df = spark.read.table(distances_table)
clustered_df = spark.read.table(clustered_table)
joined_df = driving_distances_df.join(clustered_df, driving_distances_df.origin_id == clustered_df.package_id, "left")
joined_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(routing_table)
spark.read.table(routing_table).display()

# COMMAND ----------

# MAGIC %md
# MAGIC The *table* method of the OSRM Backend Server is one of several methods available through the server's REST API.  The full list of methods include:
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
# MAGIC | ortools                | Operations research tools developed at Google for combinatorial optimization                     | Apache 2.0 | https://github.com/google/or-tools                        |
# MAGIC | folium                 | Visualize data in Python on interactive Leaflet.js maps                                          | MIT License  | https://github.com/python-visualization/folium            |
# MAGIC | dash                   | Python framework for building analytical web applications and dashboards; built on Flask, React, and Plotly.js | MIT License  | https://github.com/plotly/dash                            |
# MAGIC | branca                 | Library for generating complex HTML+JS pages in Python; provides non-map-specific features for folium | MIT License  | https://github.com/python-visualization/branca            |
# MAGIC | plotly                 | Open-source Python library for creating interactive, publication-quality charts and graphs        | MIT License  | https://github.com/plotly/plotly.py                       |
# MAGIC ray |	Flexible, high-performance distributed execution framework for scaling Python workflows |	Apache 2.0 |	https://github.com/ray-project/ray
# MAGIC cuOpt | GPU-accelerated combinatorial optimization solver from NVIDIA | Apache 2.0 | https://docs.nvidia.com/cuopt/user-guide/latest/license.html
