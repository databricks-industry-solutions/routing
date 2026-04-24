# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2a-2: Resolve CPU OSRM duration matrices
# MAGIC
# MAGIC This classic-compute notebook reads the clustered CPU handoff table, calls the
# MAGIC executor-local OSRM table API per cluster, and writes the duration matrix consumed by
# MAGIC the final serverless join step and the classic Ray optimizer.
# MAGIC
# MAGIC Recommended compute: classic single-user cluster named `cpu_distance_cluster` with
# MAGIC the OSRM init script.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("num_shipments", "40000", "Shipment count suffix")
dbutils.widgets.text("depot_lat", "39.7685", "Depot latitude")
dbutils.widgets.text("depot_lon", "-86.1580", "Depot longitude")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
num_shipments = int(dbutils.widgets.get("num_shipments"))
depot_lat = float(dbutils.widgets.get("depot_lat"))
depot_lon = float(dbutils.widgets.get("depot_lon"))

clustered_table = f"{catalog}.{schema}.shipments_by_route_cpu_{num_shipments}"
distances_table = f"{catalog}.{schema}.distances_by_route_cpu_{num_shipments}"

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd
import requests
import time

if not spark.catalog.tableExists(clustered_table):
    raise ValueError(
        f"{clustered_table} does not exist. Run stage 2a-1 before resolving OSRM durations."
    )

clustered_df = spark.read.table(clustered_table)
display(clustered_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Resolve driving times with OSRM
def get_driving_times(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["package_id"], kind="mergesort").reset_index(drop=True)
    coords = [(depot_lon, depot_lat)] + list(zip(pdf["longitude"].tolist(), pdf["latitude"].tolist()))
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
                    "duration_seconds": float(duration) if np.isfinite(duration) else np.nan,
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

print(f"Wrote CPU OSRM duration matrix to {distances_table}")

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
