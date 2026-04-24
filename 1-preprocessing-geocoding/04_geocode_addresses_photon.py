# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Geocode addresses with Photon
# MAGIC
# MAGIC This classic-compute notebook reads the normalized handoff table from step 3,
# MAGIC geocodes each address against executor-local Photon, and writes the audit table used
# MAGIC by step 5.
# MAGIC
# MAGIC Recommended compute: classic single-user cluster named `geocode_cluster` with
# MAGIC Photon and OSRM init scripts.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text(
    "normalized_table",
    "normalized_addresses",
    "Normalized address table from step 3",
)
dbutils.widgets.text("audit_table", "geocoded_addresses", "Geocode audit table")
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

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
normalized_table_name = dbutils.widgets.get("normalized_table")
audit_table_name = dbutils.widgets.get("audit_table")
batch_size = int(dbutils.widgets.get("batch_size"))
shuffle_partitions = int(dbutils.widgets.get("shuffle_partitions"))
photon_timeout_s = int(dbutils.widgets.get("photon_timeout_s"))
photon_limit = int(dbutils.widgets.get("photon_limit"))
photon_countrycode = dbutils.widgets.get("photon_countrycode").strip().upper()
photon_state = dbutils.widgets.get("photon_state").strip()
photon_bbox = dbutils.widgets.get("photon_bbox").strip()
min_geocode_success_ratio = float(dbutils.widgets.get("min_geocode_success_ratio"))

if photon_limit < 1:
    raise ValueError("photon_limit must be >= 1")

normalized_table = f"{catalog}.{schema}.{normalized_table_name}"
audit_table = f"{catalog}.{schema}.{audit_table_name}"

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

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
from typing import Iterator

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", batch_size)

normalized_df = spark.read.table(normalized_table)
if normalized_df.limit(1).count() == 0:
    raise ValueError(
        f"{normalized_table} is empty. Run step 3 successfully before calling Photon."
    )

normalized_count = normalized_df.count()
target_geocode_partitions = max(
    1,
    min(
        shuffle_partitions,
        max(
            1,
            (normalized_count + max(batch_size, 1) - 1) // max(batch_size, 1),
        ),
    ),
)
geocode_input_df = normalized_df.repartition(target_geocode_partitions, "address_id")
print(
    f"Geocoding {normalized_count:,} normalized addresses across "
    f"{target_geocode_partitions} partition(s) with batch size {batch_size}."
)

# COMMAND ----------

# DBTITLE 1,Geocode via Photon
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
        if expected_state_tokens and state_tokens and expected_state_tokens & state_tokens:
            score += 10

        label = (
            props.get("name")
            or " ".join(
                part for part in [props.get("housenumber"), props.get("street")] if part
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
geocoded_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    audit_table
)

geocode_counts = geocoded_df.agg(
    F.count("*").alias("input_rows"),
    F.sum(F.when(F.col("geocode_status") == "OK", 1).otherwise(0)).alias("geocoded_rows"),
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

display(spark.read.table(audit_table).limit(10))
print(f"Wrote Photon audit rows to {audit_table}")

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
