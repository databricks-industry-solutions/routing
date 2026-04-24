# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2b-1: Prepare the sparse GPU routing graph
# MAGIC
# MAGIC This serverless-friendly notebook reads routing-ready shipments, falls back to the
# MAGIC checked-in coordinates sample if needed, assigns stable global indices, and writes the
# MAGIC existing mapping plus sparse-neighbor tables used by the classic OSRM handoff.
# MAGIC
# MAGIC Recommended compute: serverless CPU notebook. The bundle runs this on the
# MAGIC `serverless_spark` environment (serverless environment version 5).

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("shipments_table", "raw_shipments", "Input shipments table name")
dbutils.widgets.text("num_shipments", "10000", "Shipment count to read")
dbutils.widgets.text("num_routes", "80", "Number of vehicles")
dbutils.widgets.text("depot_lat", "39.7685", "Depot latitude")
dbutils.widgets.text("depot_lon", "-86.1580", "Depot longitude")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
shipments_table_name = dbutils.widgets.get("shipments_table")
num_shipments = int(dbutils.widgets.get("num_shipments"))
dbutils.widgets.get("num_routes")  # kept for interface parity with the monolithic notebook
depot_lat = float(dbutils.widgets.get("depot_lat"))
depot_lon = float(dbutils.widgets.get("depot_lon"))

shipments_table = f"{catalog}.{schema}.{shipments_table_name}"
mapping_table = f"{catalog}.{schema}.shipment_ids_map_gpu_{num_shipments}"
clustered_table = f"{catalog}.{schema}.shipment_clusters_gpu_{num_shipments}"

# COMMAND ----------

# DBTITLE 1,Repo paths and imports
from pathlib import Path
import sys

repo_root = Path.cwd().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import pandas as pd
from pyspark.databricks.sql import functions as DBF
from pyspark.sql import Window
from pyspark.sql import functions as F

fallback_path = (repo_root / "utils" / "shipments.csv").resolve()
print(f"Repo root: {repo_root}")
print(f"Coordinates-only fallback CSV: {fallback_path}")


def geo_distance_meters(lon1, lat1, lon2, lat2):
    st_distancesphere = getattr(DBF, "st_distancesphere", None)
    if st_distancesphere is not None:
        return st_distancesphere(DBF.st_point(lon1, lat1), DBF.st_point(lon2, lat2))

    half = F.lit(0.5)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat * half), F.lit(2.0)) + F.cos(F.radians(lat1)) * F.cos(
        F.radians(lat2)
    ) * F.pow(F.sin(dlon * half), F.lit(2.0))
    return F.lit(6371008.8) * (F.lit(2.0) * F.asin(F.sqrt(a)))


def candidate_counts_for_all_origins(orig_df, pairs_df):
    return (
        orig_df.select("origin_id")
        .join(
            pairs_df.groupBy("origin_id").agg(F.count("*").alias("cand_cnt")),
            on="origin_id",
            how="left",
        )
        .withColumn("cand_cnt", F.coalesce(F.col("cand_cnt"), F.lit(0)))
    )

# COMMAND ----------

# DBTITLE 1,Load shipments
if spark.catalog.tableExists(shipments_table):
    shipments_df = spark.read.table(shipments_table)
else:
    if not fallback_path.exists():
        raise FileNotFoundError(f"Fallback CSV not found at {fallback_path}")
    print(
        "Using coordinates-only fallback because "
        f"{shipments_table} does not exist yet."
    )
    shipments_df = spark.createDataFrame(pd.read_csv(fallback_path))

required_columns = {"package_id", "city", "latitude", "longitude", "weight"}
missing_columns = required_columns - set(shipments_df.columns)
if missing_columns:
    raise ValueError(f"shipments_df is missing columns: {sorted(missing_columns)}")

shipments_df = shipments_df.select(
    F.col("package_id").cast("string").alias("package_id"),
    F.col("city").cast("string").alias("city"),
    F.col("latitude").cast("double").alias("latitude"),
    F.col("longitude").cast("double").alias("longitude"),
    F.coalesce(F.col("weight").cast("double"), F.lit(1.0)).alias("weight"),
).where(
    F.col("package_id").isNotNull()
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
    & (~F.isnan("latitude"))
    & (~F.isnan("longitude"))
).orderBy("package_id").limit(num_shipments)
display(shipments_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Add stable global indices
w = Window.orderBy("package_id")
shipments_df_with_idx = shipments_df.withColumn("global_idx", F.row_number().over(w))

depot_row = [("DEPOT", "Indianapolis", depot_lat, depot_lon, 0.0, 0)]
depot_schema = ["package_id", "city", "latitude", "longitude", "weight", "global_idx"]
depot_df = spark.createDataFrame(depot_row, depot_schema)

mapped_df = depot_df.unionByName(shipments_df_with_idx)
mapped_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(mapping_table)
display(spark.read.table(mapping_table).limit(10))

# COMMAND ----------

# DBTITLE 1,Geospatial nearest neighbors
H3_RES = 8
K_RING = 5
TOP_K = 20
MIN_CAND = 5
BOOST_RING_DELTA = 5
PARENT_RING = 2

global_index_df = spark.table(mapping_table).select(
    F.col("package_id").cast("string").alias("pid"),
    F.col("global_idx").cast("int").alias("global_idx"),
)

depot_idx_row = (
    global_index_df.where(F.col("pid") == F.lit("DEPOT")).select("global_idx").limit(1).collect()
)
depot_idx = int(depot_idx_row[0][0]) if depot_idx_row else 0

pts = shipments_df.select(
    F.col("package_id").cast("string").alias("pid"),
    F.col("latitude").cast("double").alias("lat"),
    F.col("longitude").cast("double").alias("lon"),
).withColumn("h3", DBF.h3_longlatash3(F.col("lon"), F.col("lat"), F.lit(H3_RES)))

pts_idx = pts.join(global_index_df, "pid", "left")
if pts_idx.where(F.col("global_idx").isNull()).limit(1).count():
    raise ValueError("Some package_ids are missing from the global index mapping.")

orig = pts_idx.select(
    F.col("pid").alias("origin_id"),
    F.col("lat").alias("origin_lat"),
    F.col("lon").alias("origin_lon"),
    F.col("h3").alias("origin_h3"),
    F.col("global_idx").cast("int").alias("global_idx_source"),
)

origin_kring = orig.withColumn(
    "nbr_h3", F.explode(DBF.h3_kring(F.col("origin_h3"), F.lit(K_RING)))
)

cand = pts_idx.select(
    F.col("pid").alias("dest_id"),
    F.col("lat").alias("dest_lat"),
    F.col("lon").alias("dest_lon"),
    F.col("h3").alias("dest_h3"),
    F.col("global_idx").cast("int").alias("global_idx_dest"),
)

pairs = (
    origin_kring.join(cand, F.col("dest_h3") == F.col("nbr_h3"), "inner")
    .where(F.col("origin_id") != F.col("dest_id"))
    .select(
        "origin_id",
        "origin_lat",
        "origin_lon",
        "origin_h3",
        "global_idx_source",
        "dest_id",
        "dest_lat",
        "dest_lon",
        "dest_h3",
        "global_idx_dest",
    )
)

pairs = pairs.withColumn(
    "geo_dist",
    geo_distance_meters(
        F.col("origin_lon"),
        F.col("origin_lat"),
        F.col("dest_lon"),
        F.col("dest_lat"),
    ),
).select("origin_id", "dest_id", "geo_dist", "global_idx_source", "global_idx_dest")

cand_counts = candidate_counts_for_all_origins(orig, pairs)
sparse = cand_counts.where(F.col("cand_cnt") < MIN_CAND).select("origin_id")
sparse_orig = orig.join(sparse, "origin_id")

pairs_boost1 = (
    sparse_orig.withColumn(
        "nbr_h3", F.explode(DBF.h3_kring(F.col("origin_h3"), F.lit(K_RING + BOOST_RING_DELTA)))
    )
    .join(cand, F.col("dest_h3") == F.col("nbr_h3"), "inner")
    .where(F.col("origin_id") != F.col("dest_id"))
    .select(
        "origin_id",
        "origin_lat",
        "origin_lon",
        "global_idx_source",
        "dest_id",
        "dest_lat",
        "dest_lon",
        "global_idx_dest",
    )
    .withColumn(
        "geo_dist",
        geo_distance_meters(
            F.col("origin_lon"),
            F.col("origin_lat"),
            F.col("dest_lon"),
            F.col("dest_lat"),
        ),
    )
    .select("origin_id", "dest_id", "geo_dist", "global_idx_source", "global_idx_dest")
)

pairs_aug1 = pairs.unionByName(pairs_boost1).dropDuplicates(["origin_id", "dest_id"])
still_sparse = candidate_counts_for_all_origins(orig, pairs_aug1).where(
    F.col("cand_cnt") < MIN_CAND
).select("origin_id")
parent_res = H3_RES - 1

sparse_orig2 = (
    orig.join(still_sparse, "origin_id")
    .withColumn("origin_parent_h3", DBF.h3_toparent(F.col("origin_h3"), F.lit(parent_res)))
)

cand_parent = pts_idx.select(
    F.col("pid").alias("dest_id"),
    F.col("lat").alias("dest_lat"),
    F.col("lon").alias("dest_lon"),
    DBF.h3_toparent(F.col("h3"), F.lit(parent_res)).alias("dest_parent_h3"),
    F.col("global_idx").cast("int").alias("global_idx_dest"),
)

pairs_boost2 = (
    sparse_orig2.withColumn(
        "nbr_parent_h3",
        F.explode(DBF.h3_kring(F.col("origin_parent_h3"), F.lit(PARENT_RING))),
    )
    .join(cand_parent, F.col("dest_parent_h3") == F.col("nbr_parent_h3"), "inner")
    .where(F.col("origin_id") != F.col("dest_id"))
    .select(
        "origin_id",
        "origin_lat",
        "origin_lon",
        "global_idx_source",
        "dest_id",
        "dest_lat",
        "dest_lon",
        "global_idx_dest",
    )
    .withColumn(
        "geo_dist",
        geo_distance_meters(
            F.col("origin_lon"),
            F.col("origin_lat"),
            F.col("dest_lon"),
            F.col("dest_lat"),
        ),
    )
    .select("origin_id", "dest_id", "geo_dist", "global_idx_source", "global_idx_dest")
)

pairs_final = pairs_aug1.unionByName(pairs_boost2).dropDuplicates(["origin_id", "dest_id"])
topk = (
    pairs_final.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("origin_id").orderBy(
                F.col("geo_dist").asc(),
                F.col("dest_id").asc(),
                F.col("global_idx_dest").asc(),
            )
        ),
    )
    .where(F.col("rn") <= TOP_K)
    .select("origin_id", "dest_id", "geo_dist", "global_idx_source", "global_idx_dest")
)

depot_dist = (
    orig.select("origin_id", "origin_lat", "origin_lon", "global_idx_source")
    .withColumn(
        "geo_dist",
        geo_distance_meters(
            F.col("origin_lon"),
            F.col("origin_lat"),
            F.lit(depot_lon),
            F.lit(depot_lat),
        ),
    )
    .select(
        "origin_id",
        F.lit("DEPOT").alias("dest_id"),
        "geo_dist",
        "global_idx_source",
        F.lit(depot_idx).cast("int").alias("global_idx_dest"),
    )
)

knn_lists = (
    topk.unionByName(depot_dist)
    .groupBy("origin_id", "global_idx_source")
    .agg(
        F.sort_array(
            F.collect_list(
                F.struct(
                    F.col("geo_dist").alias("geo_dist"),
                    F.col("dest_id").alias("dest_id"),
                    F.col("global_idx_dest").alias("global_idx_dest"),
                )
            )
        ).alias("neighbors")
    )
)
knn_lists.write.mode("overwrite").option("overwriteSchema", "true").option(
    "mergeSchema", "true"
).saveAsTable(clustered_table)
display(spark.read.table(clustered_table).limit(10))

print(f"Wrote GPU mapping table to {mapping_table}")
print(f"Wrote GPU sparse neighbor graph to {clustered_table}")

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
