# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2a-1: Cluster shipments for CPU routing
# MAGIC
# MAGIC This serverless-friendly notebook reads routing-ready shipments, falls back to the
# MAGIC checked-in coordinates sample if needed, and writes the deterministic clustered handoff
# MAGIC table used by the classic OSRM matrix step.
# MAGIC
# MAGIC Recommended compute: serverless CPU notebook. The bundle runs this on the
# MAGIC `serverless_spark` environment (serverless environment version 5).

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
num_shipments = int(dbutils.widgets.get("num_shipments"))
num_routes = int(dbutils.widgets.get("num_routes"))
max_van = float(dbutils.widgets.get("max_van"))
depot_lat = float(dbutils.widgets.get("depot_lat"))
depot_lon = float(dbutils.widgets.get("depot_lon"))

shipments_table = f"{catalog}.{schema}.{shipments_table_name}"
clustered_table = f"{catalog}.{schema}.shipments_by_route_cpu_{num_shipments}"

# COMMAND ----------

# DBTITLE 1,Repo paths and imports
from pathlib import Path
import sys

repo_root = Path.cwd().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import math
import numpy as np
import pandas as pd
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


def build_clusters(source_df, requested_routes, max_weight=max_van):
    source_pdf = source_df.toPandas()
    if source_pdf.empty:
        raise ValueError("No shipments available to cluster.")

    if max_weight <= 0:
        raise ValueError("max_van must be positive.")

    source_pdf["weight"] = source_pdf["weight"].fillna(0.0).astype(float)
    source_pdf["theta"] = np.arctan2(
        source_pdf["latitude"].to_numpy(dtype=float) - depot_lat,
        source_pdf["longitude"].to_numpy(dtype=float) - depot_lon,
    )
    source_pdf["radius_m"] = haversine_meters(
        depot_lat,
        depot_lon,
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
    max_weight=max_van,
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

print(f"Wrote clustered CPU shipments to {clustered_table}")

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
