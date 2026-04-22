# Databricks notebook source
# MAGIC %md
# MAGIC # Legacy fallback: CPU distance calculation
# MAGIC
# MAGIC This notebook reads the routing-ready shipments produced by stage 1, clusters them
# MAGIC for route optimization, calls the local OSRM table API on each executor, and writes
# MAGIC the duration matrix used by CPU route optimization.
# MAGIC
# MAGIC If `demos.routing.raw_shipments` is missing, the explicit coordinates-only fallback is
# MAGIC `../utils/shipments.csv`.
# MAGIC
# MAGIC Recommended compute: classic single-user cluster with the OSRM init script.

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
import math
from pyspark.sql import functions as F

fallback_path = (repo_root / "utils" / "shipments.csv").resolve()
print(f"Repo root: {repo_root}")
print(f"Coordinates-only fallback CSV: {fallback_path}")

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
    pdf = pd.read_csv(fallback_path)
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
).orderBy("package_id").limit(NUM_SHIPMENTS)
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
def haversine_meters(lat1, lon1, lat2, lon2):
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(
        dlon / 2.0
    ) ** 2
    return 6371008.8 * (2.0 * np.arcsin(np.sqrt(a)))


def assign_sweep_clusters(weights, requested_routes, max_weight):
    if not len(weights):
        return []

    assignments = np.empty(len(weights), dtype=np.int32)
    current_cluster = 0
    current_weight = 0.0
    current_count = 0
    planned_routes = max(1, min(int(requested_routes), len(weights)))

    for idx, weight in enumerate(weights):
        if weight > max_weight:
            raise ValueError(
                f"Shipment weight {weight} exceeds the configured max_van {max_weight}."
            )

        remaining_items = len(weights) - idx
        remaining_planned_clusters = max(planned_routes - current_cluster, 1)
        target_count = math.ceil(remaining_items / remaining_planned_clusters)

        should_split_for_balance = (
            current_count > 0
            and current_cluster < planned_routes - 1
            and current_count >= target_count
        )
        should_split_for_capacity = (
            current_count > 0 and current_weight + float(weight) > max_weight
        )

        if should_split_for_balance or should_split_for_capacity:
            current_cluster += 1
            current_weight = 0.0
            current_count = 0

        assignments[idx] = current_cluster
        current_weight += float(weight)
        current_count += 1

    return assignments


def build_clusters(source_df, requested_routes, max_weight=MAX_VAN):
    source_pdf = source_df.toPandas()
    if source_pdf.empty:
        raise ValueError("No shipments available to cluster.")

    if max_weight <= 0:
        raise ValueError("max_van must be positive.")

    source_pdf["weight"] = source_pdf["weight"].fillna(0.0).astype(float)
    source_pdf["theta"] = np.arctan2(
        source_pdf["latitude"].to_numpy(dtype=float) - DEPOT_LAT,
        source_pdf["longitude"].to_numpy(dtype=float) - DEPOT_LON,
    )
    source_pdf["radius_m"] = haversine_meters(
        DEPOT_LAT,
        DEPOT_LON,
        source_pdf["latitude"].to_numpy(dtype=float),
        source_pdf["longitude"].to_numpy(dtype=float),
    )
    source_pdf = source_pdf.sort_values(
        ["theta", "radius_m", "package_id"], kind="mergesort"
    ).reset_index(drop=True)

    total_weight = float(source_pdf["weight"].sum())
    min_routes_by_weight = max(1, math.ceil(total_weight / max_weight))
    target_routes = max(1, min(int(requested_routes), len(source_pdf)))
    if target_routes < min_routes_by_weight:
        print(
            f"Requested {target_routes} routes but shipment weight requires at least "
            f"{min_routes_by_weight}; using the larger count."
        )
        target_routes = min_routes_by_weight

    source_pdf["cluster_num"] = assign_sweep_clusters(
        source_pdf["weight"].to_numpy(dtype=float),
        requested_routes=target_routes,
        max_weight=max_weight,
    )

    actual_routes = int(source_pdf["cluster_num"].max()) + 1
    cluster_width = max(3, len(str(max(actual_routes, target_routes))))
    source_pdf["cluster_id"] = source_pdf["cluster_num"].map(
        lambda cid: f"{int(cid) + 1:0{cluster_width}d}"
    )

    print(
        "Sweep-clustered "
        f"{len(source_pdf):,} shipments into {actual_routes} route(s) "
        f"with requested target {target_routes} and max_van {max_weight:.0f}."
    )

    return spark.createDataFrame(
        source_pdf[["package_id", "city", "latitude", "longitude", "weight", "cluster_id"]]
    )


clustered_df = build_clusters(
    shipments_df,
    requested_routes=effective_num_routes,
    max_weight=MAX_VAN,
).cache()
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
    pdf = pdf.sort_values(["package_id"], kind="mergesort").reset_index(drop=True)
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
