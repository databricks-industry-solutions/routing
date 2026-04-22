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
