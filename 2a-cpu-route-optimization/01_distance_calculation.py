# Databricks notebook source
# MAGIC %md
# MAGIC # CPU distance calculation
# MAGIC
# MAGIC This notebook reads the routing-ready shipments produced by stage 1, clusters them
# MAGIC for route optimization, calls the local OSRM table API on each executor, and writes
# MAGIC the duration matrix used by CPU route optimization.
# MAGIC
# MAGIC If `demos.routing.raw_shipments` is missing, the explicit coordinates-only fallback is
# MAGIC `../utils/shipments.csv`.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("shipments_table", "raw_shipments", "Input shipments table name")
dbutils.widgets.text("num_shipments", "40000", "Shipment count to read")
dbutils.widgets.text("num_routes", "320", "Number of routes / clusters")
dbutils.widgets.text("max_van", "8000", "Maximum van capacity")
dbutils.widgets.text("depot_lat", "39.7685", "Depot latitude")
dbutils.widgets.text("depot_lon", "-86.1580", "Depot longitude")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
shipments_table_name = dbutils.widgets.get("shipments_table")
NUM_SHIPMENTS = int(dbutils.widgets.get("num_shipments"))
num_routes = int(dbutils.widgets.get("num_routes"))
MAX_VAN = float(dbutils.widgets.get("max_van"))
DEPOT_LAT = float(dbutils.widgets.get("depot_lat"))
DEPOT_LON = float(dbutils.widgets.get("depot_lon"))

shipments_table = f"{catalog}.{schema}.{shipments_table_name}"
clustered_table = f"{catalog}.{schema}.shipments_by_route_cpu_{NUM_SHIPMENTS}"
distances_table = f"{catalog}.{schema}.distances_by_route_cpu_{NUM_SHIPMENTS}"
routing_table = f"{catalog}.{schema}.routing_unified_by_cluster_cpu_{NUM_SHIPMENTS}"

# COMMAND ----------

# DBTITLE 1,Repo paths and imports
from pathlib import Path
import sys

repo_root = Path.cwd().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import numpy as np
import pandas as pd
import requests
import time
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

fallback_path = (repo_root / "utils" / "shipments.csv").resolve()
print(f"Repo root: {repo_root}")
print(f"Coordinates-only fallback CSV: {fallback_path}")

# COMMAND ----------

# DBTITLE 1,Load shipments
if spark.catalog.tableExists(shipments_table):
    shipments_df = spark.read.table(shipments_table).limit(NUM_SHIPMENTS)
else:
    if not fallback_path.exists():
        raise FileNotFoundError(f"Fallback CSV not found at {fallback_path}")

    print(
        "Using coordinates-only fallback because "
        f"{shipments_table} does not exist yet."
    )
    pdf = pd.read_csv(fallback_path).head(NUM_SHIPMENTS)
    shipments_df = spark.createDataFrame(pdf)

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
)
shipment_count = shipments_df.count()
if shipment_count == 0:
    raise ValueError(
        f"{shipments_table} did not contain any valid shipment coordinates after filtering."
    )

effective_num_routes = min(num_routes, shipment_count)
if effective_num_routes != num_routes:
    print(
        f"Requested {num_routes} routes but only found {shipment_count} valid shipments; "
        f"clustering into {effective_num_routes} route(s) instead."
    )

display(shipments_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Cluster shipments
def median_bisect(sdf, cid, max_weight):
    bounds = (
        sdf.agg(
            F.max("latitude").alias("lat_max"),
            F.min("latitude").alias("lat_min"),
            F.max("longitude").alias("lon_max"),
            F.min("longitude").alias("lon_min"),
        ).collect()[0]
    )
    lat_range = bounds.lat_max - bounds.lat_min
    lon_range = bounds.lon_max - bounds.lon_min
    axis = "latitude" if lat_range >= lon_range else "longitude"
    median = sdf.approxQuantile(axis, [0.5], 0.01)[0]

    return sdf.withColumn(
        "cluster_id",
        F.when(F.col(axis) <= median, F.lit(f"{cid}-1")).otherwise(F.lit(f"{cid}-2")),
    )


def build_clusters(source_df, requested_routes, max_weight=MAX_VAN):
    vec = VectorAssembler(
        inputCols=["latitude", "longitude"],
        outputCol="features",
        handleInvalid="skip",
    ).transform(source_df)

    base_model = KMeans(k=requested_routes, seed=1).fit(vec.select("features"))
    clustered = (
        base_model.transform(vec)
        .withColumnRenamed("prediction", "cluster_id")
        .withColumn("cluster_id", F.col("cluster_id").cast("string"))
        .drop("features")
    )

    heavy_ids = [
        row["cluster_id"]
        for row in (
            clustered.groupBy("cluster_id")
            .agg(F.sum("weight").alias("total_weight"))
            .filter(F.col("total_weight") > max_weight)
            .select("cluster_id")
            .distinct()
            .collect()
        )
    ]
    print("Over-capacity clusters:", heavy_ids or "none")

    if heavy_ids:
        from functools import reduce

        heavy_df = clustered.filter(F.col("cluster_id").isin(heavy_ids))
        keep_df = clustered.filter(~F.col("cluster_id").isin(heavy_ids))
        split_frames = [
            median_bisect(heavy_df.filter(F.col("cluster_id") == cid), cid, max_weight)
            for cid in heavy_ids
        ]
        split_df = reduce(lambda left, right: left.unionByName(right), split_frames)
        clustered = keep_df.unionByName(split_df)

    return clustered.withColumn("cluster_id", F.col("cluster_id").cast("string"))


if spark.catalog.tableExists(clustered_table):
    clustered_df = spark.read.table(clustered_table).withColumn(
        "cluster_id", F.col("cluster_id").cast("string")
    )
else:
    clustered_df = build_clusters(
        shipments_df,
        requested_routes=effective_num_routes,
        max_weight=MAX_VAN,
    )
    clustered_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        clustered_table
    )

display(
    clustered_df.groupBy("cluster_id")
    .agg(
        F.count("*").alias("num_deliveries"),
        F.round(F.sum("weight"), 2).alias("total_weight"),
    )
    .orderBy(F.col("total_weight").desc())
)

# COMMAND ----------

# DBTITLE 1,Resolve driving times with OSRM
def get_driving_times(pdf: pd.DataFrame) -> pd.DataFrame:
    coords = [(DEPOT_LON, DEPOT_LAT)] + list(
        zip(pdf["longitude"].tolist(), pdf["latitude"].tolist())
    )
    n = len(coords)
    if n <= 1:
        return pd.DataFrame(
            columns=[
                "origin_id",
                "destination_id",
                "origin_index",
                "destination_index",
                "duration_seconds",
            ]
        )

    coord_str = ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in coords)

    def osrm_table(radius=None):
        extra = ""
        if radius is not None:
            radii = ";".join([str(radius)] * n)
            extra = f"&radiuses={radii}"
        url = (
            f"http://127.0.0.1:5000/table/v1/driving/{coord_str}"
            f"?annotations=duration{extra}"
        )
        for attempt in range(5):
            try:
                response = requests.get(url, timeout=20)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.ReadTimeout:
                if attempt < 4:
                    time.sleep(2**attempt)
                else:
                    raise

    payload = osrm_table()
    if payload.get("code") != "Ok":
        payload = osrm_table(radius=400)
    if payload.get("code") != "Ok" or "durations" not in payload:
        raise RuntimeError(f"OSRM error: {payload}")

    matrix = np.array(payload["durations"], dtype=float)
    ids = ["DEPOT"] + [str(x) for x in pdf["package_id"].tolist()]
    rows = []
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            duration = matrix[i, j]
            rows.append(
                {
                    "origin_id": ids[i],
                    "destination_id": ids[j],
                    "origin_index": i,
                    "destination_index": j,
                    "duration_seconds": float(duration)
                    if np.isfinite(duration)
                    else np.nan,
                }
            )
    return pd.DataFrame(rows)


driving_distances_df = clustered_df.groupBy("cluster_id").applyInPandas(
    get_driving_times,
    schema="""
        origin_id STRING,
        destination_id STRING,
        origin_index INT,
        destination_index INT,
        duration_seconds DOUBLE
    """,
)
driving_distances_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    distances_table
)
display(spark.read.table(distances_table))

# COMMAND ----------

# DBTITLE 1,Join durations with shipment metadata
cluster_metadata = clustered_df.select(
    F.col("package_id").alias("origin_id"),
    "cluster_id",
    "city",
    "latitude",
    "longitude",
    "weight",
)

routing_df = (
    spark.read.table(distances_table)
    .join(cluster_metadata, on="origin_id", how="left")
    .select(
        "cluster_id",
        "origin_id",
        "destination_id",
        "origin_index",
        "destination_index",
        "duration_seconds",
        "city",
        "latitude",
        "longitude",
        "weight",
    )
)
routing_df.write.mode("overwrite").option("mergeSchema", "true").option(
    "overwriteSchema", "true"
).saveAsTable(
    routing_table
)
display(spark.read.table(routing_table))
