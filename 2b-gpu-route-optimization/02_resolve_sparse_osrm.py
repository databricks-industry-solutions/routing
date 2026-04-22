# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2b-2: Resolve sparse GPU OSRM durations
# MAGIC
# MAGIC This classic-compute notebook reads the sparse neighbor graph from stage 2b-1,
# MAGIC resolves origin-to-neighbor travel times through executor-local OSRM, and writes the
# MAGIC distance table consumed by the GPU cuOpt optimizer.
# MAGIC
# MAGIC Recommended compute: classic single-user cluster with the OSRM init script. The
# MAGIC bundle runs this on `gpu_distance_cluster`.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("num_shipments", "10000", "Shipment count suffix")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
num_shipments = int(dbutils.widgets.get("num_shipments"))

mapping_table = f"{catalog}.{schema}.shipment_ids_map_gpu_{num_shipments}"
clustered_table = f"{catalog}.{schema}.shipment_clusters_gpu_{num_shipments}"
distances_table = f"{catalog}.{schema}.distances_by_route_gpu_{num_shipments}"

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd
import requests
from pyspark.sql import functions as F
from pyspark.sql import types as T

missing_inputs = [
    table_name
    for table_name in [mapping_table, clustered_table]
    if not spark.catalog.tableExists(table_name)
]
if missing_inputs:
    raise ValueError(
        "Missing prerequisite GPU handoff table(s): "
        f"{', '.join(missing_inputs)}. Run stage 2b-1 first."
    )

# COMMAND ----------

# DBTITLE 1,Load mapping and sparse graph
knn_df = spark.read.table(clustered_table).select("origin_id", "global_idx_source", "neighbors")
mapping_df = spark.read.table(mapping_table).select(
    F.col("package_id").cast("string").alias("pid"),
    F.col("latitude").cast("double").alias("lat"),
    F.col("longitude").cast("double").alias("lon"),
    F.col("global_idx").cast("int").alias("global_idx"),
)

if knn_df.limit(1).count() == 0:
    raise ValueError(f"{clustered_table} is empty. Run stage 2b-1 successfully first.")

mapping_pdf = mapping_df.toPandas()
if mapping_pdf.empty:
    raise ValueError(f"{mapping_table} is empty. Run stage 2b-1 successfully first.")

if mapping_pdf["lat"].isnull().any() or mapping_pdf["lon"].isnull().any():
    raise ValueError(f"{mapping_table} contains null coordinates.")

id_to_lonlat = {
    row["pid"]: (float(row["lon"]), float(row["lat"])) for _, row in mapping_pdf.iterrows()
}
id_to_global_idx = {
    row["pid"]: int(row["global_idx"]) for _, row in mapping_pdf.iterrows()
}
depot_idx = int(id_to_global_idx.get("DEPOT", 0))
b_id_to_lonlat = sc.broadcast(id_to_lonlat)

display(knn_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Resolve sparse OSRM durations
OSRM_BASE = "http://127.0.0.1:5000"
MAX_TABLE = 500

result_schema = T.StructType(
    [
        T.StructField("origin_id", T.StringType()),
        T.StructField("destination_id", T.StringType()),
        T.StructField("origin_index", T.IntegerType()),
        T.StructField("destination_index", T.IntegerType()),
        T.StructField("duration_seconds", T.DoubleType()),
        T.StructField("global_idx_source", T.IntegerType()),
        T.StructField("global_idx_dest", T.IntegerType()),
    ]
)


def _osrm_one_origin(origin_id, dest_ids, coord_map, chunk_cap):
    if not dest_ids:
        return pd.DataFrame(columns=[field.name for field in result_schema])

    out = []
    for start in range(0, len(dest_ids), max(1, chunk_cap)):
        chunk = dest_ids[start : start + chunk_cap]
        coord_ids = [origin_id] + [dest_id for dest_id in chunk if dest_id != origin_id]
        id2idx = {pid: idx for idx, pid in enumerate(coord_ids)}
        coords = [coord_map[pid] for pid in coord_ids]
        src_idx = [id2idx[origin_id]]
        dst_idx = [id2idx[dest_id] for dest_id in chunk if dest_id in id2idx]

        coord_str = ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in coords)
        url = (
            f"{OSRM_BASE}/table/v1/driving/{coord_str}"
            f"?annotations=duration"
            f"&sources={';'.join(map(str, src_idx))}"
            f"&destinations={';'.join(map(str, dst_idx))}"
        )

        response = requests.get(url, timeout=60)
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "Ok" or "durations" not in payload:
            raise RuntimeError(f"OSRM error: {payload}")

        durations = payload["durations"]
        filtered_chunk = [dest_id for dest_id in chunk if dest_id in id2idx]
        for j_local, dest_id in enumerate(filtered_chunk):
            duration = durations[0][j_local]
            out.append(
                (
                    origin_id,
                    dest_id,
                    int(src_idx[0]),
                    int(dst_idx[j_local]),
                    float(duration) if duration is not None else np.nan,
                )
            )

    return pd.DataFrame(
        out,
        columns=[
            "origin_id",
            "destination_id",
            "origin_index",
            "destination_index",
            "duration_seconds",
        ],
    )


def osrm_group(pdf: pd.DataFrame) -> pd.DataFrame:
    if pdf.empty:
        return pd.DataFrame(columns=[field.name for field in result_schema])

    coord_map = b_id_to_lonlat.value
    row0 = pdf.iloc[0]
    origin_id = str(row0["origin_id"])
    global_idx_source = int(row0["global_idx_source"])

    dest_ids = []
    global_idx_dest_map = {}
    seen = set()
    for _, row in pdf.iterrows():
        for neighbor in row["neighbors"]:
            dest_id = (
                neighbor.get("dest_id")
                if isinstance(neighbor, dict)
                else getattr(neighbor, "dest_id")
            )
            global_idx_dest = (
                neighbor.get("global_idx_dest")
                if isinstance(neighbor, dict)
                else getattr(neighbor, "global_idx_dest")
            )
            if dest_id not in seen:
                seen.add(dest_id)
                dest_ids.append(dest_id)
            if global_idx_dest is not None:
                global_idx_dest_map[dest_id] = int(global_idx_dest)

    if "DEPOT" in seen and "DEPOT" not in global_idx_dest_map:
        global_idx_dest_map["DEPOT"] = depot_idx

    osrm_df = _osrm_one_origin(origin_id, dest_ids, coord_map, MAX_TABLE)
    osrm_df["global_idx_source"] = global_idx_source
    osrm_df["global_idx_dest"] = osrm_df["destination_id"].map(global_idx_dest_map)
    return osrm_df[
        [
            "origin_id",
            "destination_id",
            "origin_index",
            "destination_index",
            "duration_seconds",
            "global_idx_source",
            "global_idx_dest",
        ]
    ]


result_df = knn_df.groupBy("global_idx_source").applyInPandas(osrm_group, schema=result_schema)
result_df.write.mode("overwrite").option("overwriteSchema", "true").option(
    "mergeSchema", "true"
).saveAsTable(
    distances_table
)
display(spark.read.table(distances_table).limit(10))

print(f"Wrote GPU sparse OSRM distances to {distances_table}")
