# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Build routing-ready shipments
# MAGIC
# MAGIC This serverless-friendly notebook turns the Photon audit table into the stable
# MAGIC `raw_shipments` contract used by the CPU and GPU routing stages. If you already have a
# MAGIC coordinates-ready table, you can also point `coordinates_source_table` at it and use
# MAGIC this notebook as the final handoff step directly.
# MAGIC
# MAGIC Recommended compute: serverless CPU notebook. The bundle runs this on the
# MAGIC `serverless_spark` environment (serverless environment version 5).

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("audit_table", "geocoded_addresses", "Geocode audit table")
dbutils.widgets.text("output_table", "raw_shipments", "Routing shipment output table")
dbutils.widgets.text(
    "coordinates_source_table",
    "",
    "Optional coordinates-ready table with package_id/city/latitude/longitude/weight",
)
dbutils.widgets.text("sample_seed", "42", "Seed for deterministic weights")
dbutils.widgets.text("weight_min", "1", "Minimum synthetic weight")
dbutils.widgets.text("weight_mode", "5", "Mode synthetic weight")
dbutils.widgets.text("weight_max", "100", "Maximum synthetic weight")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
audit_table_name = dbutils.widgets.get("audit_table")
output_table_name = dbutils.widgets.get("output_table")
coordinates_source_table = dbutils.widgets.get("coordinates_source_table").strip()
sample_seed = int(dbutils.widgets.get("sample_seed"))
weight_min = float(dbutils.widgets.get("weight_min"))
weight_mode = float(dbutils.widgets.get("weight_mode"))
weight_max = float(dbutils.widgets.get("weight_max"))

if weight_min <= 0 or weight_max <= 0:
    raise ValueError("weight_min and weight_max must be positive")
if weight_min > weight_max:
    raise ValueError("weight_min must be <= weight_max")
if not (weight_min <= weight_mode <= weight_max):
    raise ValueError("weight_mode must fall between weight_min and weight_max")

audit_table = f"{catalog}.{schema}.{audit_table_name}"
output_table = f"{catalog}.{schema}.{output_table_name}"

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

# DBTITLE 1,Optional coordinates-only bypass
expected_coordinate_columns = {"package_id", "city", "latitude", "longitude", "weight"}

if coordinates_source_table:
    passthrough_df = spark.read.table(coordinates_source_table)
    missing = expected_coordinate_columns - set(passthrough_df.columns)
    if missing:
        raise ValueError(
            f"coordinates_source_table is missing required columns: {sorted(missing)}"
        )

    passthrough_df.select(
        "package_id", "city", "latitude", "longitude", "weight"
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

    print(
        f"Bypassed Photon and wrote {coordinates_source_table} to {output_table}."
    )
    display(spark.read.table(output_table).limit(10))
    dbutils.notebook.exit(output_table)

# COMMAND ----------

# DBTITLE 1,Build routing-ready shipments
if not spark.catalog.tableExists(audit_table):
    raise ValueError(
        f"{audit_table} does not exist. Run step 4 or provide coordinates_source_table."
    )

if weight_max == weight_min:
    triangular_c = 1.0
else:
    triangular_c = (weight_mode - weight_min) / (weight_max - weight_min)

seed_base = F.pmod(F.abs(F.xxhash64("address_id", F.lit(sample_seed))), F.lit(1_000_000))
u = (seed_base.cast("double") / F.lit(1_000_000.0)).alias("u")

geocoded_ok = spark.read.table(audit_table).where(
    F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
)
if geocoded_ok.limit(1).count() == 0:
    raise ValueError(
        f"{audit_table} did not contain any successful geocodes with coordinates."
    )

if weight_max == weight_min:
    weight_expr = F.lit(weight_min)
else:
    weight_expr = F.when(
        F.col("u") < triangular_c,
        weight_min
        + F.sqrt(F.col("u") * (weight_max - weight_min) * (weight_mode - weight_min)),
    ).otherwise(
        weight_max
        - F.sqrt((1 - F.col("u")) * (weight_max - weight_min) * (weight_max - weight_mode))
    )

shipments_df = (
    geocoded_ok.withColumn("u", u)
    .withColumn("weight", weight_expr)
    .withColumn(
        "package_id",
        F.concat(
            F.upper(
                F.lpad(
                    F.substring(F.regexp_replace(F.col("city"), "[^A-Za-z]", ""), 1, 3),
                    3,
                    "X",
                )
            ),
            F.lit("_"),
            F.lpad(
                F.row_number().over(Window.orderBy(F.col("address_id"))).cast("string"),
                5,
                "0",
            ),
        ),
    )
    .select(
        "package_id",
        F.col("city"),
        F.col("latitude"),
        F.col("longitude"),
        F.round("weight", 6).alias("weight"),
    )
)

shipments_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    output_table
)
display(spark.read.table(output_table).limit(10))

print(f"Wrote routing-ready shipments to {output_table}")

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
