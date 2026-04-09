# Databricks notebook source
# MAGIC %md
# MAGIC Make sure to use the cluster created in notebook `02_compute_setup`, and that you've compiled the OSRM server for your region in `01_osrm_server_setup`

# COMMAND ----------

# DBTITLE 1,Set Configs
NUM_SHIPMENTS = 10_000
num_routes = 320  # total trucks available
MAX_EV = 4000 # max capacity
MAX_VAN = 8000
DEPOT_LAT, DEPOT_LON = 39.7685, -86.1580 # start and end point for each route


catalog = "default"
schema = f"routing"
shipments_table = f"{catalog}.{schema}.raw_shipments_{NUM_SHIPMENTS}"
mapping_table = f"{catalog}.{schema}.shipment_ids_map_{NUM_SHIPMENTS}"
clustered_table = f"{catalog}.{schema}.shipment_clusters_gpu_{NUM_SHIPMENTS}"
distances_table = f"{catalog}.{schema}.distances_by_route_gpu_{NUM_SHIPMENTS}"
routing_table = f"{catalog}.{schema}.routing_unified_by_cluster_gpu_{NUM_SHIPMENTS}"

# COMMAND ----------

# DBTITLE 1,Imports
import random
from typing import Iterator
import math
import pandas as pd
import requests
import numpy as np
import os
import re
import time
from pyspark.sql import functions as F, Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# DBTITLE 1,Read Shipment Data
if spark.catalog.tableExists(shipments_table):
    shipments_df = spark.read.table(shipments_table).limit(NUM_SHIPMENTS)
else:
    # to see how this was generated, or generate different data yourself, check utils/data_generation.py
    sample_path = os.getcwd() + "/utils/shipments.csv"
    if not sample_path.startswith("file:"):
        sample_path = f"file:{sample_path}"
    pdf = pd.read_csv(sample_path)
    shipments_df = spark.createDataFrame(pdf).limit(NUM_SHIPMENTS)
    shipments_df.write.mode("overwrite").saveAsTable(shipments_table)

# COMMAND ----------

# DBTITLE 1,Add Global Indices
# Add global_idx to shipments_df, starting at 1
w = Window.orderBy(F.monotonically_increasing_id())
shipments_df_with_idx = shipments_df.withColumn("global_idx", F.row_number().over(w))

depot_row = [("DEPOT", "Indianapolis", DEPOT_LAT, DEPOT_LON, 0, 0)]
depot_schema = ["package_id", "city", "latitude", "longitude", "weight", "global_idx"]
depot_df = spark.createDataFrame(depot_row, depot_schema)

mapped_df = depot_df.unionByName(shipments_df_with_idx)
mapped_df.write.mode("overwrite").saveAsTable(mapping_table)
display(spark.read.table(mapping_table))

# COMMAND ----------

# DBTITLE 1,Geo Nearest Neighbors
# Geospatial kNN with global indices threaded through
from pyspark.sql import functions as F, Window
from pyspark.databricks.sql import functions as DBF  # Databricks spatial/H3

# ----- required inputs (same as before) -----
H3_RES = 8
K_RING = 5

TOP_K = 20
MIN_CAND = 5
BOOST_RING_DELTA = 5
PARENT_RING = 2

# If you already have a DataFrame called `global_index_df`, replace the next block with it.
global_index_df = (
    spark.table(mapping_table)
    .select(F.col("package_id").cast("string").alias("pid"),
            F.col("global_idx").cast("int").alias("global_idx"))
)

# Resolve depot global index (default to 0 if not found)
_depot_idx_row = (global_index_df.where(F.col("pid") == F.lit("DEPOT"))
                                 .select("global_idx").limit(1).collect())
DEPOT_IDX = int(_depot_idx_row[0][0]) if _depot_idx_row else 0

# 1) Load & index with H3, then attach global_idx to each point
pts = (
    shipments_df
    .select(F.col("package_id").cast("string").alias("pid"),
            F.col("latitude").cast("double").alias("lat"),
            F.col("longitude").cast("double").alias("lon"))
    .withColumn("h3", DBF.h3_longlatash3(F.col("lon"), F.col("lat"), F.lit(H3_RES)))
)

# attach global_idx; fail fast if any are missing
pts_idx = pts.join(global_index_df, "pid", "left")
_missing = pts_idx.where(F.col("global_idx").isNull()).limit(1).count()
if _missing:
    raise ValueError("Some package_ids in shipments_df are missing from the global index extract.")

# 2) Origins
orig = pts_idx.select(
    F.col("pid").alias("origin_id"),
    F.col("lat").alias("origin_lat"),
    F.col("lon").alias("origin_lon"),
    F.col("h3").alias("origin_h3"),
    F.col("global_idx").cast("int").alias("global_idx_source")
)

# 3) Candidate points in nearby H3 cells
origin_kring = orig.withColumn(
    "nbr_h3", F.explode(DBF.h3_kring(F.col("origin_h3"), F.lit(K_RING)))
)

cand = pts_idx.select(
    F.col("pid").alias("dest_id"),
    F.col("lat").alias("dest_lat"),
    F.col("lon").alias("dest_lon"),
    F.col("h3").alias("dest_h3"),
    F.col("global_idx").cast("int").alias("global_idx_dest")
)

pairs = (
  origin_kring.join(cand, F.col("dest_h3") == F.col("nbr_h3"), "inner")
              .where(F.col("origin_id") != F.col("dest_id"))
              .select("origin_id","origin_lat","origin_lon","origin_h3","global_idx_source",
                      "dest_id","dest_lat","dest_lon","dest_h3","global_idx_dest")
)

# 4) Exact distance per candidate
pairs = pairs.withColumn(
  "geo_dist",
  DBF.st_distance(
    DBF.st_point(F.col("origin_lon"), F.col("origin_lat")),
    DBF.st_point(F.col("dest_lon"),   F.col("dest_lat"))
  )
).select("origin_id","dest_id","geo_dist","global_idx_source","global_idx_dest")

# ---- 4a) Count baseline candidates per origin
cand_counts = pairs.groupBy("origin_id").agg(F.count("*").alias("cand_cnt"))
ok = cand_counts.where(F.col("cand_cnt") >= MIN_CAND).select("origin_id")
sparse = cand_counts.where(F.col("cand_cnt") < MIN_CAND).select("origin_id")

# ---- 4b) Booster 1: bigger ring at SAME resolution for sparse only
sparse_orig = orig.join(sparse, "origin_id")

sparse_kring_big = sparse_orig.withColumn(
    "nbr_h3",
    F.explode(DBF.h3_kring(F.col("origin_h3"), F.lit(K_RING + BOOST_RING_DELTA)))
)

pairs_boost1 = (
    sparse_kring_big
    .join(cand, F.col("dest_h3") == F.col("nbr_h3"), "inner")
    .where(F.col("origin_id") != F.col("dest_id"))
    .select("origin_id","origin_lat","origin_lon","global_idx_source",
            "dest_id","dest_lat","dest_lon","global_idx_dest")
    .withColumn(
        "geo_dist",
        DBF.st_distance(
            DBF.st_point(F.col("origin_lon"), F.col("origin_lat")),
            DBF.st_point(F.col("dest_lon"),   F.col("dest_lat"))
        )
    )
    .select("origin_id","dest_id","geo_dist","global_idx_source","global_idx_dest")
)

# Merge baseline + booster1 (dedupe)
pairs_aug1 = (
    pairs.unionByName(pairs_boost1)
         .dropDuplicates(["origin_id","dest_id"])
)

# ---- 4c) Still sparse?
post1_counts = pairs_aug1.groupBy("origin_id").agg(F.count("*").alias("cand_cnt"))
still_sparse = post1_counts.where(F.col("cand_cnt") < MIN_CAND).select("origin_id")

# ---- 4d) Booster 2: parent resolution for remaining sparse
parent_res = H3_RES - 1

sparse_orig2 = (
    orig.join(still_sparse, "origin_id")
        .withColumn("origin_parent_h3", DBF.h3_toparent(F.col("origin_h3"), F.lit(parent_res)))
)

cand_parent = (
    pts_idx.select(
        F.col("pid").alias("dest_id"),
        F.col("lat").alias("dest_lat"),
        F.col("lon").alias("dest_lon"),
        DBF.h3_toparent(F.col("h3"), F.lit(parent_res)).alias("dest_parent_h3"),
        F.col("global_idx").cast("int").alias("global_idx_dest")
    )
)

sparse_parent_kring = sparse_orig2.withColumn(
    "nbr_parent_h3",
    F.explode(DBF.h3_kring(F.col("origin_parent_h3"), F.lit(PARENT_RING)))
)

pairs_boost2 = (
    sparse_parent_kring
    .join(cand_parent, F.col("dest_parent_h3") == F.col("nbr_parent_h3"), "inner")
    .where(F.col("origin_id") != F.col("dest_id"))
    .select("origin_id","origin_lat","origin_lon","global_idx_source",
            "dest_id","dest_lat","dest_lon","global_idx_dest")
    .withColumn(
        "geo_dist",
        DBF.st_distance(
            DBF.st_point(F.col("origin_lon"), F.col("origin_lat")),
            DBF.st_point(F.col("dest_lon"),   F.col("dest_lat"))
        )
    )
    .select("origin_id","dest_id","geo_dist","global_idx_source","global_idx_dest")
)

# ---- 4e) Final candidate set
pairs_final = (
    pairs_aug1.unionByName(pairs_boost2)
              .dropDuplicates(["origin_id","dest_id"])
)

# ---- 4f) Top-K per origin (by geo distance)
w = Window.partitionBy("origin_id").orderBy(F.col("geo_dist").asc())
topk = pairs_final.withColumn("rn", F.row_number().over(w)).where(F.col("rn") <= TOP_K) \
                  .select("origin_id","dest_id","geo_dist","global_idx_source","global_idx_dest")

# ---- 5) Add depot once per origin
# carry origin's global_idx_source; set the depot's global_idx_dest
depot_dist = (
    orig.select("origin_id","origin_lat","origin_lon","global_idx_source")
        .withColumn(
            "geo_dist",
            DBF.st_distance(
                DBF.st_point(F.col("origin_lon"), F.col("origin_lat")),
                DBF.st_point(F.lit(DEPOT_LON), F.lit(DEPOT_LAT))
            )
        )
        .select(
            "origin_id",
            F.lit("DEPOT").alias("dest_id"),
            "geo_dist",
            "global_idx_source",
            F.lit(DEPOT_IDX).cast("int").alias("global_idx_dest")
        )
)

knn_plus_depot = topk.unionByName(depot_dist)

# ---- 6) Persist per-origin neighbor lists
# Keep origin_id + global_idx_source at the top level;
# neighbors holds dest_id, geo_dist, and global_idx_dest for each neighbor.
knn_lists = (
    knn_plus_depot.groupBy("origin_id", "global_idx_source")
                  .agg(F.collect_list(F.struct(
                      F.col("dest_id"),
                      F.col("geo_dist"),
                      F.col("global_idx_dest")
                  )).alias("neighbors"))
)

knn_lists.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(clustered_table)
display(spark.table(clustered_table))

# COMMAND ----------

spark.read.table(clustered_table)

# COMMAND ----------

# DBTITLE 1,Display and Neighbors Count
display(
  spark.read.table(clustered_table)
  .select("origin_id", F.array_size("neighbors").alias("neighbors_count"))
)

# COMMAND ----------

# DBTITLE 1,Get Distances
# === kNN-driven OSRM durations (applyInPandas; per-origin) ===
from typing import List, Dict, Tuple
from pyspark.sql import functions as F, types as T
import pandas as pd, numpy as np, requests, math

# ------------------ Inputs ------------------
OSRM_BASE = "http://127.0.0.1:5000"
MAX_TABLE = 500                          # must respect osrm-routed --max-table-size
SAFETY    = 0                            # optional headroom; keep 0 unless you see 413s
DEPOT_LAT, DEPOT_LON = float(DEPOT_LAT), float(DEPOT_LON)
DEPOT_IDX = int(DEPOT_IDX) if "DEPOT_IDX" in globals() else 0  # optional, if you carried it

# Tables:
# - shipments_table: package_id, latitude, longitude
# - clustered_table: origin_id, global_idx_source, neighbors (dest_id, geo_dist, global_idx_dest)
# - distances_table: output table

# ------------------ Prep: coords ------------------
# Load origins + neighbor lists (must include global indices now)
knn_df = spark.table(clustered_table).select("origin_id", "global_idx_source", "neighbors")

# Build the set of package_ids whose coordinates we need (origins + non-DEPOT neighbors)
origin_ids_df  = knn_df.select(F.col("origin_id").alias("pid"))
neighbor_ids_df = (
    knn_df
      .select(F.explode("neighbors").alias("n"))
      .select(F.col("n.dest_id").alias("pid"))
      .where(F.col("pid") != F.lit("DEPOT"))
)
needed_ids_df = origin_ids_df.unionByName(neighbor_ids_df).distinct()

# Join to shipments to obtain lon/lat for all package points
pts_df = (
    spark.table(shipments_table)
         .select(F.col("package_id").cast("string").alias("pid"),
                 F.col("latitude").cast("double").alias("lat"),
                 F.col("longitude").cast("double").alias("lon"))
)

needed_pts_df = needed_ids_df.join(pts_df, "pid", "left")
# Sanity: ensure no missing coords (non-DEPOT)
if needed_pts_df.where(F.col("lat").isNull() | F.col("lon").isNull()).limit(1).count():
    raise ValueError("Some IDs in kNN lists were not found in shipments_table (missing lat/lon).")

# Broadcast id -> (lon, lat) mapping (plus DEPOT)
id_lonlat_pdf = needed_pts_df.toPandas()
id_to_lonlat: Dict[str, Tuple[float, float]] = {r["pid"]: (float(r["lon"]), float(r["lat"])) for _, r in id_lonlat_pdf.iterrows()}
id_to_lonlat["DEPOT"] = (float(DEPOT_LON), float(DEPOT_LAT))
b_id_to_lonlat = sc.broadcast(id_to_lonlat)

# ------------------ Output schema (keeps old cols, adds global idx cols) ------------------
schema = T.StructType([
    T.StructField("origin_id", T.StringType()),
    T.StructField("destination_id", T.StringType()),
    T.StructField("origin_index", T.IntegerType()),        # request-local index (kept for compatibility)
    T.StructField("destination_index", T.IntegerType()),   # request-local index (kept for compatibility)
    T.StructField("duration_seconds", T.DoubleType()),
    T.StructField("global_idx_source", T.IntegerType()),   # NEW: stable global index
    T.StructField("global_idx_dest", T.IntegerType()),     # NEW: stable global index
])

# ------------------ Helper: call OSRM for one origin over chunks ------------------
def _osrm_one_origin(origin_id: str,
                     dest_ids: List[str],
                     coord_map: Dict[str, Tuple[float, float]],
                     chunk_cap: int) -> pd.DataFrame:
    """
    Calls OSRM /table for a single source -> many destinations, chunking if needed.
    Returns columns: origin_id, destination_id, origin_index, destination_index, duration_seconds
    origin_index/destination_index are request-local (0-based in each call).
    """
    if not dest_ids:
        return pd.DataFrame(columns=["origin_id","destination_id","origin_index","destination_index","duration_seconds"])

    # Verify coordinates exist
    if origin_id not in coord_map:
        raise ValueError(f"Missing lon/lat for origin_id {origin_id}")

    out = []
    # chunk to respect max table size: 1 x D <= cap
    for start in range(0, len(dest_ids), max(1, chunk_cap)):
        chunk = dest_ids[start:start+chunk_cap]
        # Build coord list: [origin, unique chunk destinations not equal to origin]
        coord_ids = [origin_id] + [d for d in chunk if d not in (origin_id,)]
        id2idx = {pid: i for i, pid in enumerate(coord_ids)}

        try:
            coords = [coord_map[_id] for _id in coord_ids]
        except KeyError as e:
            raise ValueError(f"Missing lon/lat for id {e} in coord_map.")

        src_idx = [id2idx[origin_id]]  # [0]
        dst_idx = [id2idx[d] for d in chunk if d in id2idx]  # keep only those in coord_ids

        coord_str = ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in coords)
        url = (f"{OSRM_BASE}/table/v1/driving/{coord_str}"
               f"?annotations=duration"
               f"&sources={';'.join(map(str, src_idx))}"
               f"&destinations={';'.join(map(str, dst_idx))}")

        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "Ok" or "durations" not in data:
            raise RuntimeError(f"OSRM error: {data}")

        durations = data["durations"]  # shape 1 x len(dst_idx)
        for j_local, d in enumerate([d for d in chunk if d in id2idx]):
            dur = durations[0][j_local]
            out.append((
                origin_id, d,
                int(src_idx[0]), int(dst_idx[j_local]),
                float(dur) if dur is not None and np.isfinite(dur) else np.nan
            ))

    return pd.DataFrame(out, columns=["origin_id","destination_id","origin_index","destination_index","duration_seconds"])

# ------------------ applyInPandas UDF (per origin/global_idx_source) ------------------
def osrm_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    pdf contains rows for a single global_idx_source.
    Columns present: origin_id, global_idx_source, neighbors (array of structs with dest_id, global_idx_dest, geo_dist)
    We will:
      1) collect unique neighbors (preserve first-seen order)
      2) call OSRM /table once (or in chunks) for origin -> neighbors
      3) attach global_idx_source/global_idx_dest to the output
    """
    if pdf.empty:
        return pd.DataFrame(columns=[f.name for f in schema])

    coord_map = b_id_to_lonlat.value
    cap = MAX_TABLE - SAFETY

    # Expect single origin per group; if duplicates, pick the first
    row0 = pdf.iloc[0]
    origin_id = str(row0["origin_id"])
    gsrc = int(row0["global_idx_source"])

    # Collect neighbors across any duplicate rows for this origin
    dest_ids: List[str] = []
    gid_dest_map: Dict[str, int] = {}
    seen = set()

    for _, r in pdf.iterrows():
        # neighbors is a list[dict-like] with keys dest_id, global_idx_dest
        for n in r["neighbors"]:
            dest_id = n.get("dest_id") if isinstance(n, dict) else getattr(n, "dest_id")
            gdst    = n.get("global_idx_dest") if isinstance(n, dict) else getattr(n, "global_idx_dest")
            if dest_id not in seen:
                seen.add(dest_id)
                dest_ids.append(dest_id)
            # keep/overwrite mapping (identical if consistent)
            if gdst is not None:
                gid_dest_map[dest_id] = int(gdst)

    # Ensure DEPOT has a global index even if not present in neighbor struct
    if "DEPOT" in seen and "DEPOT" not in gid_dest_map:
        gid_dest_map["DEPOT"] = DEPOT_IDX

    # Enforce cap (1 x D <= cap). If too many, function will chunk internally.
    chunk_cap = max(1, cap)

    # Call OSRM for this origin
    osrm_df = _osrm_one_origin(origin_id, dest_ids, coord_map, chunk_cap)

    # Attach global indices
    osrm_df["global_idx_source"] = gsrc
    osrm_df["global_idx_dest"]   = osrm_df["destination_id"].map(lambda d: gid_dest_map.get(d, np.nan)).astype("float64")

    # Final column order & dtypes
    osrm_df = osrm_df[["origin_id","destination_id","origin_index","destination_index","duration_seconds",
                       "global_idx_source","global_idx_dest"]]

    # Cast global_idx_dest to int where available (keep NaN if missing)
    if osrm_df["global_idx_dest"].notna().any():
        osrm_df["global_idx_dest"] = osrm_df["global_idx_dest"].astype("Int64")

    return osrm_df

# ------------------ Execute with applyInPandas ------------------
driver_df = knn_df.select("origin_id", "global_idx_source", "neighbors")

result_df = (
    driver_df
      .groupBy("global_idx_source")
      .applyInPandas(osrm_group, schema=schema)
)

result_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(distances_table)
display(spark.read.table(distances_table))

# COMMAND ----------

# DBTITLE 1,Save Results
# driving_distances_df = spark.read.table(distances_table)
# clustered_df = spark.read.table(clustered_table)
# joined_df = driving_distances_df.join(clustered_df, "origin_id", "left").select("origin_id", "destination_id", "duration_seconds")
# joined_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(routing_table)
# spark.read.table(routing_table).display()

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
# MAGIC | ortools                | Operations research tools developed at Google for combinatorial optimization                     | Apache License 2.0 | https://github.com/google/or-tools                        |
# MAGIC | folium                 | Visualize data in Python on interactive Leaflet.js maps                                          | MIT License  | https://github.com/python-visualization/folium            |
# MAGIC | dash                   | Python framework for building analytical web applications and dashboards; built on Flask, React, and Plotly.js | MIT License  | https://github.com/plotly/dash                            |
# MAGIC | branca                 | Library for generating complex HTML+JS pages in Python; provides non-map-specific features for folium | MIT License  | https://github.com/python-visualization/branca            |
# MAGIC | plotly                 | Open-source Python library for creating interactive, publication-quality charts and graphs        | MIT License  | https://github.com/plotly/plotly.py                       |
# MAGIC ray |	Flexible, high-performance distributed execution framework for scaling Python workflows |	Apache2.0 |	https://github.com/ray-project/ray
# MAGIC cuOpt | GPU-accelerated combinatorial optimization solver from NVIDIA | Apache 2.0 | https://docs.nvidia.com/cuopt/user-guide/latest/license.html
