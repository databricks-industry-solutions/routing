# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Photon and OSRM worker services
# MAGIC
# MAGIC Recommended compute: classic single-user cluster named `geocode_cluster` with
# MAGIC Photon and OSRM init scripts.
# MAGIC
# MAGIC Run this notebook after:
# MAGIC
# MAGIC 1. Executing `01_prepare_assets.py`
# MAGIC 2. Attaching the rendered init scripts from the shared volume to the cluster
# MAGIC 3. Restarting that cluster

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("volume", "routing_assets", "Volume")
dbutils.widgets.text("region", "indiana", "Region")
dbutils.widgets.text("photon_query", "Indianapolis", "Photon health query")
dbutils.widgets.text(
    "osrm_health_route",
    "-86.1581,39.7684;-86.1180,39.9784",
    "OSRM health route coordinates",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
region = dbutils.widgets.get("region")
photon_query = dbutils.widgets.get("photon_query")
osrm_health_route = dbutils.widgets.get("osrm_health_route")
volume_base = f"/Volumes/{catalog}/{schema}/{volume}"

print(f"Environment file: {volume_base}/config/demo_env.sh")
print(f"Photon init script: {volume_base}/init/photon-worker.sh")
print(f"OSRM init script: {volume_base}/init/osrm-worker.sh")

# COMMAND ----------

# DBTITLE 1,Driver smoke tests
import requests

photon_response = requests.get(
    "http://127.0.0.1:2322/api",
    params={"q": photon_query, "limit": 1},
    timeout=(2, 10),
)
photon_response.raise_for_status()
photon_payload = photon_response.json()
if not photon_payload.get("features"):
    raise ValueError(
        f"Photon returned no features for health query '{photon_query}'. "
        "Check that the local Photon dump matches the intended region."
    )
display(photon_payload)

# COMMAND ----------

# DBTITLE 1,OSRM health
osrm_route_response = requests.get(
    f"http://127.0.0.1:5000/route/v1/driving/{osrm_health_route}",
    params={"overview": "false"},
    timeout=(2, 10),
)
if osrm_route_response.ok and osrm_route_response.json().get("code") == "Ok":
    display(osrm_route_response.json())
else:
    osrm_nearest_response = requests.get(
        f"http://127.0.0.1:5000/nearest/v1/driving/{osrm_health_route.split(';')[0]}",
        params={"number": 1},
        timeout=(2, 10),
    )
    osrm_nearest_response.raise_for_status()
    display(osrm_nearest_response.json())

# COMMAND ----------

# DBTITLE 1,Optional executor-local validation
from pyspark.sql import functions as F


def _executor_probe(_):
    import json
    import urllib.request

    results = []
    for service_name, url in [
        ("photon", f"http://127.0.0.1:2322/api?q={photon_query}&limit=1"),
        (
            "osrm",
            f"http://127.0.0.1:5000/route/v1/driving/{osrm_health_route}?overview=false",
        ),
    ]:
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
            ok = True
            if service_name == "photon":
                ok = bool(payload.get("features"))
            results.append((service_name, ok, str(payload)[:200]))
        except Exception as exc:  # noqa: BLE001
            results.append((service_name, False, str(exc)))
    return iter(results)


probe_df = spark.range(0, spark.sparkContext.defaultParallelism).repartition(
    spark.sparkContext.defaultParallelism
)
display(
    probe_df.rdd.mapPartitions(_executor_probe).toDF(
        ["service_name", "ok", "sample_payload"]
    )
)

# COMMAND ----------

# DBTITLE 1,Next step
print(
    "If both services are healthy on the driver and the executor probe succeeds, "
    "run 04_geocode_addresses_photon.py with the default local Indiana sample."
)

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
