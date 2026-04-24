# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Load and normalize addresses
# MAGIC
# MAGIC This serverless-friendly notebook reads the vendored address sample or a source table
# MAGIC override, writes the raw input to `source_addresses`, and materializes a deterministic
# MAGIC `normalized_addresses` table for the Photon task that follows.
# MAGIC
# MAGIC Recommended compute: serverless CPU notebook. The bundle runs this on the
# MAGIC `serverless_spark` environment (serverless environment version 5).

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("input_table", "source_addresses", "Input address table")
dbutils.widgets.text(
    "normalized_table",
    "normalized_addresses",
    "Normalized address table for Photon",
)
dbutils.widgets.text(
    "local_csv_relative_path",
    "../data/overture_indiana_addresses.csv",
    "Relative path to vendored address CSV",
)
dbutils.widgets.text(
    "source_table",
    "",
    "Optional Overture-shaped table override; leave blank for vendored CSV",
)
dbutils.widgets.text("row_limit", "50000", "Maximum address rows to process")
dbutils.widgets.text("fallback_city", "Indianapolis", "Fallback city name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
input_table_name = dbutils.widgets.get("input_table")
normalized_table_name = dbutils.widgets.get("normalized_table")
local_csv_relative_path = dbutils.widgets.get("local_csv_relative_path")
source_table = dbutils.widgets.get("source_table").strip()
row_limit = int(dbutils.widgets.get("row_limit"))
fallback_city = dbutils.widgets.get("fallback_city")

if row_limit < 1:
    raise ValueError("row_limit must be >= 1")

input_table = f"{catalog}.{schema}.{input_table_name}"
normalized_table = f"{catalog}.{schema}.{normalized_table_name}"

# COMMAND ----------

# DBTITLE 1,Repo paths and imports
from pathlib import Path
import sys

repo_root = Path.cwd().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

local_csv_path = (Path.cwd() / local_csv_relative_path).resolve()
print(f"Repo root: {repo_root}")
print(f"Vendored CSV path: {local_csv_path}")

# COMMAND ----------

# DBTITLE 1,Load addresses
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def load_addresses():
    if source_table:
        print(f"Loading source table override: {source_table}")
        return spark.table(source_table)

    if not local_csv_path.exists():
        raise FileNotFoundError(f"Vendored CSV not found at {local_csv_path}")

    print(f"Loading vendored CSV: {local_csv_path}")
    pdf = pd.read_csv(local_csv_path).head(row_limit)
    return spark.createDataFrame(pdf)


addresses_raw = load_addresses()
addresses_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    input_table
)
addresses_raw = spark.read.table(input_table).limit(row_limit)
display(addresses_raw.limit(10))

# COMMAND ----------

# DBTITLE 1,Normalize Overture-like address columns
address_levels_schema = T.ArrayType(T.StructType([T.StructField("value", T.StringType())]))
if "address_levels_json" in addresses_raw.columns:
    addresses_raw = addresses_raw.withColumn(
        "address_levels_struct",
        F.from_json(F.col("address_levels_json"), address_levels_schema),
    )
elif "address_levels" in addresses_raw.columns:
    addresses_raw = addresses_raw.withColumn("address_levels_struct", F.col("address_levels"))
else:
    addresses_raw = addresses_raw.withColumn("address_levels_struct", F.expr("array()"))

address_levels_field = addresses_raw.schema["address_levels_struct"]
if isinstance(address_levels_field.dataType, T.ArrayType) and isinstance(
    address_levels_field.dataType.elementType, T.StructType
):
    address_levels = F.transform(
        F.coalesce(
            F.col("address_levels_struct"),
            F.expr("array()").cast(address_levels_field.dataType.simpleString()),
        ),
        lambda level: level["value"],
    )
else:
    address_levels = F.coalesce(
        F.col("address_levels_struct"), F.expr("array()").cast("array<string>")
    )


def optional_col(name: str, data_type: str = "string"):
    if name in addresses_raw.columns:
        return F.col(name).cast(data_type)
    return F.lit(None).cast(data_type)


city_expr = F.coalesce(
    F.when(F.col("postal_city") != "", F.col("postal_city")),
    F.element_at(F.col("address_levels"), -1),
    F.lit(fallback_city),
)

normalized_df = (
    addresses_raw.where(optional_col("street").isNotNull() & optional_col("number").isNotNull())
    .select(
        F.coalesce(
            optional_col("address_id"),
            optional_col("id"),
            F.sha2(
                F.concat_ws(
                    "||",
                    optional_col("number"),
                    optional_col("street"),
                    optional_col("unit"),
                    optional_col("postal_city"),
                    optional_col("postcode"),
                ),
                256,
            ),
        ).alias("address_id"),
        F.coalesce(optional_col("country"), F.lit("US")).alias("country"),
        optional_col("street").alias("street"),
        optional_col("number").alias("number"),
        F.coalesce(optional_col("unit"), F.lit("")).alias("unit"),
        F.coalesce(optional_col("postcode"), F.lit("")).alias("postcode"),
        F.coalesce(optional_col("postal_city"), F.lit("")).alias("postal_city"),
        address_levels.alias("address_levels"),
    )
    .withColumn("city", city_expr)
    .withColumn(
        "formatted_address",
        F.concat_ws(
            ", ",
            F.concat_ws(" ", F.col("number"), F.col("street")),
            F.when(F.col("unit") != "", F.col("unit")),
            F.col("city"),
            F.when(F.col("postcode") != "", F.col("postcode")),
            F.col("country"),
        ),
    )
    .dropDuplicates(["formatted_address"])
    .orderBy("address_id")
    .limit(row_limit)
)

normalized_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    normalized_table
)
display(spark.read.table(normalized_table).limit(10))

print(f"Wrote normalized address requests to {normalized_table}")

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
