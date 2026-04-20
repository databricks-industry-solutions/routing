# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Photon and OSRM worker services
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
    "run 03_geocode_addresses.py with the default local Indiana sample."
)
