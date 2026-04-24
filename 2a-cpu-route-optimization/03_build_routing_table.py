# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2a-3: Finalize CPU routing table
# MAGIC
# MAGIC This serverless-friendly notebook joins the classic OSRM duration matrix back to the
# MAGIC clustered shipment metadata and writes the stable `routing_unified_by_cluster_cpu_*`
# MAGIC contract consumed by the Ray optimizer.
# MAGIC
# MAGIC Recommended compute: serverless CPU notebook. The bundle runs this on the
# MAGIC `serverless_spark` environment (serverless environment version 5).

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("num_shipments", "40000", "Shipment count suffix")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
num_shipments = int(dbutils.widgets.get("num_shipments"))

clustered_table = f"{catalog}.{schema}.shipments_by_route_cpu_{num_shipments}"
distances_table = f"{catalog}.{schema}.distances_by_route_cpu_{num_shipments}"
routing_table = f"{catalog}.{schema}.routing_unified_by_cluster_cpu_{num_shipments}"

# COMMAND ----------

# DBTITLE 1,Build routing table
from pyspark.sql import functions as F

missing_inputs = [
    table_name
    for table_name in [clustered_table, distances_table]
    if not spark.catalog.tableExists(table_name)
]
if missing_inputs:
    raise ValueError(
        "Missing prerequisite CPU handoff table(s): "
        f"{', '.join(missing_inputs)}. Run stages 2a-1 and 2a-2 first."
    )

cluster_metadata = spark.read.table(clustered_table).select(
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

print(f"Wrote CPU routing handoff table to {routing_table}")

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
