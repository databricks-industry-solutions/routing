# Databricks notebook source
# MAGIC %md
# MAGIC # Geocode local addresses into routing-ready shipments
# MAGIC
# MAGIC The default path reads the vendored Indianapolis-area Overture sample from
# MAGIC `../data/overture_indiana_addresses.csv`, geocodes it against executor-local Photon,
# MAGIC and writes routing-ready shipments to `demos.routing.raw_shipments`.
# MAGIC
# MAGIC If you already have trusted coordinates, set `coordinates_source_table` and this
# MAGIC notebook will pass that data through directly.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("input_table", "source_addresses", "Input address table")
dbutils.widgets.text("audit_table", "geocoded_addresses", "Geocode audit table")
dbutils.widgets.text("output_table", "raw_shipments", "Routing shipment output table")
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
dbutils.widgets.text(
    "coordinates_source_table",
    "",
    "Optional coordinates-ready table with package_id/city/latitude/longitude/weight",
)
dbutils.widgets.text("row_limit", "50000", "Maximum address rows to process")
dbutils.widgets.text("sample_seed", "42", "Seed for deterministic weights")
dbutils.widgets.text("batch_size", "500", "Rows per pandas batch")
dbutils.widgets.text("shuffle_partitions", "128", "Shuffle partitions")
dbutils.widgets.text("photon_timeout_s", "5", "Photon timeout seconds")
dbutils.widgets.text("photon_limit", "5", "Photon candidate limit")
dbutils.widgets.text("photon_countrycode", "US", "Photon country code filter")
dbutils.widgets.text("photon_state", "Indiana", "Photon state filter")
dbutils.widgets.text(
    "photon_bbox",
    "-88.1,37.7,-84.7,41.9",
    "Photon bbox minLon,minLat,maxLon,maxLat",
)
dbutils.widgets.text(
    "min_geocode_success_ratio",
    "0.70",
    "Minimum successful geocode ratio before failing",
)
dbutils.widgets.text("fallback_city", "Indianapolis", "Fallback city name")
dbutils.widgets.text("weight_min", "1", "Minimum synthetic weight")
dbutils.widgets.text("weight_mode", "5", "Mode synthetic weight")
dbutils.widgets.text("weight_max", "100", "Maximum synthetic weight")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
input_table = dbutils.widgets.get("input_table")
audit_table = dbutils.widgets.get("audit_table")
output_table = dbutils.widgets.get("output_table")
local_csv_relative_path = dbutils.widgets.get("local_csv_relative_path")
source_table = dbutils.widgets.get("source_table").strip()
coordinates_source_table = dbutils.widgets.get("coordinates_source_table").strip()
row_limit = int(dbutils.widgets.get("row_limit"))
sample_seed = int(dbutils.widgets.get("sample_seed"))
batch_size = int(dbutils.widgets.get("batch_size"))
shuffle_partitions = int(dbutils.widgets.get("shuffle_partitions"))
photon_timeout_s = int(dbutils.widgets.get("photon_timeout_s"))
photon_limit = int(dbutils.widgets.get("photon_limit"))
photon_countrycode = dbutils.widgets.get("photon_countrycode").strip().upper()
photon_state = dbutils.widgets.get("photon_state").strip()
photon_bbox = dbutils.widgets.get("photon_bbox").strip()
min_geocode_success_ratio = float(dbutils.widgets.get("min_geocode_success_ratio"))
fallback_city = dbutils.widgets.get("fallback_city")
weight_min = float(dbutils.widgets.get("weight_min"))
weight_mode = float(dbutils.widgets.get("weight_mode"))
weight_max = float(dbutils.widgets.get("weight_max"))

if photon_limit < 1:
    raise ValueError("photon_limit must be >= 1")

photon_bbox_bounds = None
photon_bias_lat = None
photon_bias_lon = None
if photon_bbox:
    bbox_parts = [part.strip() for part in photon_bbox.split(",")]
    if len(bbox_parts) != 4:
        raise ValueError(
            "photon_bbox must use minLon,minLat,maxLon,maxLat format when provided"
        )
    photon_bbox_bounds = tuple(float(part) for part in bbox_parts)
    min_lon, min_lat, max_lon, max_lat = photon_bbox_bounds
    photon_bias_lon = (min_lon + max_lon) / 2
    photon_bias_lat = (min_lat + max_lat) / 2

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", batch_size)

input_table_fqn = f"{catalog}.{schema}.{input_table}"
audit_table_fqn = f"{catalog}.{schema}.{audit_table}"
output_table_fqn = f"{catalog}.{schema}.{output_table}"

# COMMAND ----------

# DBTITLE 1,Repo paths
from pathlib import Path
import sys

repo_root = Path.cwd().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

local_csv_path = (Path.cwd() / local_csv_relative_path).resolve()
print(f"Repo root: {repo_root}")
print(f"Vendored CSV path: {local_csv_path}")

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
    ).write.mode("overwrite").saveAsTable(output_table_fqn)

    print(
        f"Bypassed Photon and wrote {coordinates_source_table} "
        f"to {output_table_fqn}."
    )
    dbutils.notebook.exit(output_table_fqn)

# COMMAND ----------

# DBTITLE 1,Load addresses
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


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
addresses_raw.write.mode("overwrite").saveAsTable(input_table_fqn)
addresses_raw = spark.read.table(input_table_fqn).limit(row_limit)
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
    .limit(row_limit)
)
display(normalized_df.limit(10))

target_geocode_partitions = max(
    1,
    min(
        shuffle_partitions,
        max(1, (row_limit + max(batch_size, 1) - 1) // max(batch_size, 1)),
    ),
)
geocode_input_df = normalized_df.repartition(target_geocode_partitions, "address_id")
print(
    "Geocoding "
    f"up to {row_limit:,} addresses across {target_geocode_partitions} partitions "
    f"with batch size {batch_size}."
)

# COMMAND ----------

# DBTITLE 1,Geocode via Photon
from typing import Iterator

geocode_schema = T.StructType(
    normalized_df.schema.fields
    + [
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("photon_label", T.StringType(), True),
        T.StructField("geocode_status", T.StringType(), True),
    ]
)


def geocode_partitions(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    import re
    import requests

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
    session.mount("http://", adapter)
    search_endpoint = "http://127.0.0.1:2322/api"

    expected_country_values = {photon_countrycode.lower()} if photon_countrycode else set()
    if photon_countrycode == "US":
        expected_country_values.update(
            {"usa", "united states", "united states of america"}
        )

    def normalize_text(value) -> str:
        return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()

    def tokenize(value) -> set[str]:
        normalized = normalize_text(value)
        return set(normalized.split()) if normalized else set()

    expected_state_tokens = tokenize(photon_state)
    if photon_state:
        expected_state_tokens.add(normalize_text(photon_state[:2]))

    def within_bbox(coordinates) -> bool:
        if not photon_bbox_bounds:
            return True
        lon, lat = coordinates
        min_lon, min_lat, max_lon, max_lat = photon_bbox_bounds
        return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat

    def score_feature(feature, row):
        coordinates = feature.get("geometry", {}).get("coordinates", [])
        if len(coordinates) < 2 or coordinates[0] is None or coordinates[1] is None:
            return None
        if not within_bbox(coordinates):
            return None

        props = feature.get("properties", {})
        country_value = normalize_text(props.get("countrycode") or props.get("country"))
        if expected_country_values and country_value and country_value not in expected_country_values:
            return None

        score = 0
        if photon_bbox_bounds:
            score += 20
        if country_value in expected_country_values:
            score += 40

        postcode = normalize_text(getattr(row, "postcode", ""))
        feature_postcode = normalize_text(props.get("postcode"))
        if postcode and feature_postcode:
            if postcode == feature_postcode:
                score += 25
            else:
                return None

        city_tokens = tokenize(getattr(row, "city", ""))
        feature_city_tokens = (
            tokenize(props.get("city"))
            or tokenize(props.get("district"))
            or tokenize(props.get("county"))
        )
        if city_tokens and feature_city_tokens:
            if city_tokens == feature_city_tokens:
                score += 20
            elif city_tokens & feature_city_tokens:
                score += 10

        street = normalize_text(getattr(row, "street", ""))
        feature_street = normalize_text(props.get("street") or props.get("name"))
        if street and feature_street:
            if street == feature_street:
                score += 20
            elif street in feature_street or feature_street in street:
                score += 8

        house_number = normalize_text(getattr(row, "number", ""))
        feature_house_number = normalize_text(props.get("housenumber"))
        if house_number and feature_house_number:
            if house_number == feature_house_number:
                score += 15
            else:
                return None

        state_tokens = tokenize(props.get("state"))
        if expected_state_tokens and state_tokens:
            if expected_state_tokens & state_tokens:
                score += 10

        label = (
            props.get("name")
            or " ".join(
                part
                for part in [props.get("housenumber"), props.get("street")]
                if part
            )
            or getattr(row, "formatted_address", "")
        )
        return score, coordinates[1], coordinates[0], label

    def first_valid_match(endpoint: str, params: dict, row):
        params = {key: value for key, value in params.items() if value not in ("", None)}
        response = session.get(endpoint, params=params, timeout=(2, photon_timeout_s))
        response.raise_for_status()

        best_match = None
        for feature in response.json().get("features", []):
            candidate = score_feature(feature, row)
            if candidate is None:
                continue
            if best_match is None or candidate[0] > best_match[0]:
                best_match = candidate
        return best_match

    def lookup(row):
        try:
            # Photon 0.7.x assets in this accelerator do not expose /structured,
            # so use /api with tighter filters and several shorter query forms.
            common_search_params = {
                "limit": photon_limit,
                "bbox": photon_bbox,
                "lat": photon_bias_lat,
                "lon": photon_bias_lon,
                "location_bias_scale": 0.05 if photon_bias_lat is not None else None,
            }
            street_and_number = " ".join(
                part
                for part in [getattr(row, "number", ""), getattr(row, "street", "")]
                if part
            ).strip()
            query_parts = [
                street_and_number,
                getattr(row, "city", ""),
                getattr(row, "postcode", ""),
            ]
            attempts = [
                {
                    **common_search_params,
                    "q": ", ".join(part for part in query_parts if part),
                },
                {
                    **common_search_params,
                    "q": ", ".join(
                        part
                        for part in [street_and_number, getattr(row, "city", "")]
                        if part
                    ),
                },
                {
                    **common_search_params,
                    "q": ", ".join(
                        part
                        for part in [getattr(row, "street", ""), getattr(row, "city", "")]
                        if part
                    ),
                },
            ]
            last_http_error = None

            for params in attempts:
                try:
                    match = first_valid_match(search_endpoint, params, row)
                except requests.exceptions.HTTPError as exc:
                    if exc.response is not None:
                        response_text = exc.response.text.replace("\n", " ").strip()
                        last_http_error = (
                            f"{exc.response.status_code}:{response_text[:120]}"
                        )
                    else:
                        last_http_error = "unknown"
                    continue
                if match is not None:
                    _, latitude, longitude, label = match
                    return latitude, longitude, label, "OK"

            if last_http_error is not None:
                return None, None, None, f"ERROR:HTTP:{last_http_error}"
            return None, None, None, "NO_MATCH"
        except Exception as exc:  # noqa: BLE001
            return None, None, None, f"ERROR:{type(exc).__name__}"

    for pdf in iterator:
        results = [lookup(row) for row in pdf.itertuples(index=False)]
        pdf = pdf.copy()
        pdf[["latitude", "longitude", "photon_label", "geocode_status"]] = pd.DataFrame(
            results, index=pdf.index
        )
        yield pdf


geocoded_df = geocode_input_df.mapInPandas(
    geocode_partitions, schema=geocode_schema
).cache()
geocoded_df.write.mode("overwrite").saveAsTable(audit_table_fqn)

geocode_counts = geocoded_df.agg(
    F.count("*").alias("input_rows"),
    F.sum(F.when(F.col("geocode_status") == "OK", 1).otherwise(0)).alias(
        "geocoded_rows"
    ),
).collect()[0]
input_rows = int(geocode_counts["input_rows"] or 0)
geocoded_rows = int(geocode_counts["geocoded_rows"] or 0)
success_ratio = geocoded_rows / input_rows if input_rows else 0.0

print(
    "Photon geocoded "
    f"{geocoded_rows:,}/{input_rows:,} rows ({success_ratio:.1%} success rate)"
)
if input_rows and success_ratio < min_geocode_success_ratio:
    raise ValueError(
        "Photon geocoding success ratio "
        f"({success_ratio:.1%}) fell below the configured minimum "
        f"({min_geocode_success_ratio:.1%}). Check Photon query parameters, "
        "region filters, or source address quality before continuing."
    )

display(spark.read.table(audit_table_fqn).limit(10))

# COMMAND ----------

# DBTITLE 1,Build routing-ready shipments
seed_base = F.pmod(F.abs(F.xxhash64("address_id", F.lit(sample_seed))), F.lit(1_000_000))
u = (seed_base.cast("double") / F.lit(1_000_000.0)).alias("u")
triangular_c = (weight_mode - weight_min) / (weight_max - weight_min)

geocoded_ok = spark.read.table(audit_table_fqn).where(
    F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
)

shipments_df = (
    geocoded_ok.withColumn("u", u)
    .withColumn(
        "weight",
        F.when(
            F.col("u") < triangular_c,
            weight_min
            + F.sqrt(F.col("u") * (weight_max - weight_min) * (weight_mode - weight_min)),
        ).otherwise(
            weight_max
            - F.sqrt(
                (1 - F.col("u")) * (weight_max - weight_min) * (weight_max - weight_mode)
            )
        ),
    )
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

shipments_df.write.mode("overwrite").saveAsTable(output_table_fqn)
display(spark.read.table(output_table_fqn).limit(10))

# COMMAND ----------

# DBTITLE 1,Status summary
summary = spark.sql(
    f"""
    SELECT
      COUNT(*) AS input_rows,
      SUM(CASE WHEN geocode_status = 'OK' THEN 1 ELSE 0 END) AS geocoded_rows,
      SUM(CASE WHEN geocode_status <> 'OK' THEN 1 ELSE 0 END) AS failed_rows
    FROM {audit_table_fqn}
    """
)
display(summary)

print(f"Wrote routing-ready shipments to {output_table_fqn}")
