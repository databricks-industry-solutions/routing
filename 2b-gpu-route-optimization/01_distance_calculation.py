# Databricks notebook source
# MAGIC %md
# MAGIC # GPU route preparation: sparse distance calculation
# MAGIC
# MAGIC This notebook builds a sparse neighbor graph with Databricks geospatial functions,
# MAGIC then resolves origin-to-neighbor travel times with executor-local OSRM. The resulting
# MAGIC sparse matrix feeds the GPU cuOpt notebook.

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
NUM_SHIPMENTS = int(dbutils.widgets.get("num_shipments"))
NUM_ROUTES = int(dbutils.widgets.get("num_routes"))
DEPOT_LAT = float(dbutils.widgets.get("depot_lat"))
DEPOT_LON = float(dbutils.widgets.get("depot_lon"))

shipments_table = f"{catalog}.{schema}.{shipments_table_name}"
mapping_table = f"{catalog}.{schema}.shipment_ids_map_gpu_{NUM_SHIPMENTS}"
clustered_table = f"{catalog}.{schema}.shipment_clusters_gpu_{NUM_SHIPMENTS}"
distances_table = f"{catalog}.{schema}.distances_by_route_gpu_{NUM_SHIPMENTS}"

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
from pyspark.databricks.sql import functions as DBF
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

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
display(shipments_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Add stable global indices
w = Window.orderBy("package_id")
shipments_df_with_idx = shipments_df.withColumn("global_idx", F.row_number().over(w))

depot_row = [("DEPOT", "Indianapolis", DEPOT_LAT, DEPOT_LON, 0.0, 0)]
depot_schema = ["package_id", "city", "latitude", "longitude", "weight", "global_idx"]
depot_df = spark.createDataFrame(depot_row, depot_schema)

mapped_df = depot_df.unionByName(shipments_df_with_idx)
mapped_df.write.mode("overwrite").saveAsTable(mapping_table)
display(spark.read.table(mapping_table).limit(10))

# COMMAND ----------

# DBTITLE 1,Geospatial nearest neighbors
H3_RES = 8
K_RING = 5
TOP_K = 20
MIN_CAND = 5
BOOST_RING_DELTA = 5
PARENT_RING = 2

global_index_df = (
    spark.table(mapping_table)
    .select(
        F.col("package_id").cast("string").alias("pid"),
        F.col("global_idx").cast("int").alias("global_idx"),
    )
)

depot_idx_row = (
    global_index_df.where(F.col("pid") == F.lit("DEPOT")).select("global_idx").limit(1).collect()
)
DEPOT_IDX = int(depot_idx_row[0][0]) if depot_idx_row else 0

pts = (
    shipments_df.select(
        F.col("package_id").cast("string").alias("pid"),
        F.col("latitude").cast("double").alias("lat"),
        F.col("longitude").cast("double").alias("lon"),
    ).withColumn("h3", DBF.h3_longlatash3(F.col("lon"), F.col("lat"), F.lit(H3_RES)))
)

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
            F.lit(DEPOT_LON),
            F.lit(DEPOT_LAT),
        ),
    )
    .select(
        "origin_id",
        F.lit("DEPOT").alias("dest_id"),
        "geo_dist",
        "global_idx_source",
        F.lit(DEPOT_IDX).cast("int").alias("global_idx_dest"),
    )
)

knn_lists = (
    topk.unionByName(depot_dist)
    .groupBy("origin_id", "global_idx_source")
    .agg(
        F.collect_list(
            F.struct(
                F.col("dest_id"),
                F.col("geo_dist"),
                F.col("global_idx_dest"),
            )
        ).alias("neighbors")
    )
)
knn_lists.write.mode("overwrite").option("overwriteSchema", "true").option(
    "mergeSchema", "true"
).saveAsTable(clustered_table)
display(spark.read.table(clustered_table).limit(10))

# COMMAND ----------

# DBTITLE 1,Resolve sparse OSRM durations
OSRM_BASE = "http://127.0.0.1:5000"
MAX_TABLE = 500

knn_df = spark.table(clustered_table).select("origin_id", "global_idx_source", "neighbors")
origin_ids_df = knn_df.select(F.col("origin_id").alias("pid"))
neighbor_ids_df = (
    knn_df.select(F.explode("neighbors").alias("n"))
    .select(F.col("n.dest_id").alias("pid"))
    .where(F.col("pid") != F.lit("DEPOT"))
)
needed_ids_df = origin_ids_df.unionByName(neighbor_ids_df).distinct()

pts_df = shipments_df.select(
    F.col("package_id").cast("string").alias("pid"),
    F.col("latitude").cast("double").alias("lat"),
    F.col("longitude").cast("double").alias("lon"),
)
needed_pts_df = needed_ids_df.join(pts_df, "pid", "left")
if needed_pts_df.where(F.col("lat").isNull() | F.col("lon").isNull()).limit(1).count():
    raise ValueError("Some IDs in kNN lists were not found in shipments_table.")

id_lonlat_pdf = needed_pts_df.toPandas()
id_to_lonlat = {
    row["pid"]: (float(row["lon"]), float(row["lat"]))
    for _, row in id_lonlat_pdf.iterrows()
}
id_to_lonlat["DEPOT"] = (float(DEPOT_LON), float(DEPOT_LAT))
b_id_to_lonlat = sc.broadcast(id_to_lonlat)

schema = T.StructType(
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
        return pd.DataFrame(columns=[field.name for field in schema])

    out = []
    for start in range(0, len(dest_ids), max(1, chunk_cap)):
        chunk = dest_ids[start : start + chunk_cap]
        coord_ids = [origin_id] + [d for d in chunk if d != origin_id]
        id2idx = {pid: i for i, pid in enumerate(coord_ids)}
        coords = [coord_map[pid] for pid in coord_ids]
        src_idx = [id2idx[origin_id]]
        dst_idx = [id2idx[d] for d in chunk if d in id2idx]

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
        filtered_chunk = [d for d in chunk if d in id2idx]
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
        return pd.DataFrame(columns=[field.name for field in schema])

    coord_map = b_id_to_lonlat.value
    row0 = pdf.iloc[0]
    origin_id = str(row0["origin_id"])
    gsrc = int(row0["global_idx_source"])

    dest_ids = []
    gid_dest_map = {}
    seen = set()
    for _, row in pdf.iterrows():
        for neighbor in row["neighbors"]:
            dest_id = (
                neighbor.get("dest_id")
                if isinstance(neighbor, dict)
                else getattr(neighbor, "dest_id")
            )
            gdst = (
                neighbor.get("global_idx_dest")
                if isinstance(neighbor, dict)
                else getattr(neighbor, "global_idx_dest")
            )
            if dest_id not in seen:
                seen.add(dest_id)
                dest_ids.append(dest_id)
            if gdst is not None:
                gid_dest_map[dest_id] = int(gdst)

    if "DEPOT" in seen and "DEPOT" not in gid_dest_map:
        gid_dest_map["DEPOT"] = DEPOT_IDX

    osrm_df = _osrm_one_origin(origin_id, dest_ids, coord_map, MAX_TABLE)
    osrm_df["global_idx_source"] = gsrc
    osrm_df["global_idx_dest"] = osrm_df["destination_id"].map(gid_dest_map)
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


result_df = (
    knn_df.groupBy("global_idx_source")
    .applyInPandas(osrm_group, schema=schema)
)
result_df.write.mode("overwrite").option("overwriteSchema", "true").option(
    "mergeSchema", "true"
).saveAsTable(distances_table)
display(spark.read.table(distances_table).limit(10))
