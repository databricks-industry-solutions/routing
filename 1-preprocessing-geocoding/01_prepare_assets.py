# Databricks notebook source
# MAGIC %md
# MAGIC # Prepare Photon and OSRM assets
# MAGIC
# MAGIC This notebook creates the shared `demos.routing` volume layout, downloads Photon
# MAGIC assets, builds OSRM, preprocesses the Indiana extract, and renders the worker init
# MAGIC scripts consumed by the rest of the accelerator.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demos", "Catalog")
dbutils.widgets.text("schema", "routing", "Schema")
dbutils.widgets.text("volume", "routing_assets", "Volume")
dbutils.widgets.text("region", "indiana", "GeoFabrik region slug")
dbutils.widgets.text(
    "pbf_url",
    "https://download.geofabrik.de/north-america/us/indiana-latest.osm.pbf",
    "OSM extract URL",
)
dbutils.widgets.text("osrm_version", "v6.0.0", "OSRM version")
dbutils.widgets.text("photon_version", "0.7.2", "Photon version")
dbutils.widgets.text(
    "photon_jar_url",
    "https://github.com/komoot/photon/releases/download/0.7.2/photon-0.7.2.jar",
    "Photon JAR URL",
)
dbutils.widgets.text(
    "photon_db_url",
    "https://download1.graphhopper.com/public/north-america/usa/photon-db-usa-0.7ES-latest.tar.bz2",
    "Photon database dump URL",
)
dbutils.widgets.text("osrm_threads", "2", "OSRM worker threads")
dbutils.widgets.text("osrm_max_table_size", "500", "OSRM max table size")
dbutils.widgets.text("photon_heap", "4G", "Photon JVM heap")
dbutils.widgets.text("photon_health_query", "Indianapolis", "Photon health query")
dbutils.widgets.dropdown(
    "force_refresh_photon",
    "false",
    ["false", "true"],
    "Force redownload Photon assets",
)
dbutils.widgets.text(
    "osrm_health_route",
    "-86.1581,39.7684;-86.1180,39.9784",
    "OSRM health route coordinates",
)
dbutils.widgets.dropdown(
    "debug_start_osrm",
    "false",
    ["false", "true"],
    "Run rendered OSRM worker script for debugging",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
region = dbutils.widgets.get("region")
pbf_url = dbutils.widgets.get("pbf_url")
osrm_version = dbutils.widgets.get("osrm_version")
photon_version = dbutils.widgets.get("photon_version")
photon_jar_url = dbutils.widgets.get("photon_jar_url")
photon_db_url = dbutils.widgets.get("photon_db_url")
osrm_threads = dbutils.widgets.get("osrm_threads")
osrm_max_table_size = dbutils.widgets.get("osrm_max_table_size")
photon_heap = dbutils.widgets.get("photon_heap")
photon_health_query = dbutils.widgets.get("photon_health_query")
force_refresh_photon = dbutils.widgets.get("force_refresh_photon").lower() == "true"
osrm_health_route = dbutils.widgets.get("osrm_health_route")
debug_start_osrm = dbutils.widgets.get("debug_start_osrm").lower() == "true"
volume_base = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

# DBTITLE 1,Create UC assets
catalog_exists = (
    spark.sql("SHOW CATALOGS")
    .where(f"catalog = '{catalog}'")
    .limit(1)
    .count()
)
if not catalog_exists:
    raise ValueError(
        f"Catalog `{catalog}` must already exist before running this notebook."
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

for subdir in [
    "config",
    "downloads",
    f"maps/{region}",
    "photon",
    "build",
    "logs",
    "init",
    "debug",
]:
    dbutils.fs.mkdirs(f"{volume_base}/{subdir}")

display(spark.sql(f"SHOW VOLUMES IN `{catalog}`.`{schema}`"))

# COMMAND ----------

# DBTITLE 1,Write shared environment file
env_contents = f"""export CATALOG="{catalog}"
export SCHEMA="{schema}"
export VOLUME="{volume}"
export REGION="{region}"
export VOLUME_BASE="{volume_base}"
export PBF_URL="{pbf_url}"
export OSRM_VERSION="{osrm_version}"
export PHOTON_VERSION="{photon_version}"
export PHOTON_JAR_URL="{photon_jar_url}"
export PHOTON_DB_URL="{photon_db_url}"
export PHOTON_PORT="2322"
export OSRM_PORT="5000"
export OSRM_THREADS="{osrm_threads}"
export OSRM_MAX_TABLE_SIZE="{osrm_max_table_size}"
export PHOTON_HEAP="{photon_heap}"
export PHOTON_HEALTH_QUERY="{photon_health_query}"
export FORCE_REFRESH_PHOTON="{str(force_refresh_photon).lower()}"
export OSRM_HEALTH_ROUTE="{osrm_health_route}"
export LOCAL_ASSET_ROOT="/local_disk0/routing-geocoding"
export LOCAL_OSRM_SRC="/local_disk0/routing-geocoding/osrm-src"
export LOCAL_OSRM_BUILD="/local_disk0/routing-geocoding/osrm-build"
export LOCAL_OSRM_WORK="/local_disk0/routing-geocoding/osrm-work"
export LOCAL_PHOTON_ROOT="/local_disk0/routing-geocoding/photon"
export OSRM_TARGET_DIR="{volume_base}/maps/{region}"
export PHOTON_TARGET_DIR="{volume_base}/photon"
"""

from pathlib import Path

tmp_env_file = Path("/tmp/routing_geocoding_demo_env.sh")
tmp_env_file.write_text(env_contents)

dbutils.fs.put(f"{volume_base}/config/demo_env.sh", env_contents, overwrite=True)
print(f"Temporary shell env file: {tmp_env_file}")
print(env_contents)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Photon assets
# MAGIC
# MAGIC The default path keeps the heavyweight geocoder data in the shared volume so later
# MAGIC worker init scripts can copy it to local disk on each executor.

# COMMAND ----------

# DBTITLE 1,Download Photon JAR and dump
# MAGIC %sh -e
# MAGIC source /tmp/routing_geocoding_demo_env.sh
# MAGIC mkdir -p "$VOLUME_BASE/downloads" "$VOLUME_BASE/photon"
# MAGIC if [[ "$FORCE_REFRESH_PHOTON" != "true" && -f "$PHOTON_TARGET_DIR/photon-${PHOTON_VERSION}.jar" && -d "$PHOTON_TARGET_DIR/photon_data" ]]; then
# MAGIC   echo "Photon assets already present in $PHOTON_TARGET_DIR, skipping download."
# MAGIC   ls -lh "$PHOTON_TARGET_DIR"
# MAGIC   exit 0
# MAGIC fi
# MAGIC if [[ "$FORCE_REFRESH_PHOTON" == "true" ]]; then
# MAGIC   echo "Force refreshing Photon assets in $PHOTON_TARGET_DIR"
# MAGIC fi
# MAGIC cd "$VOLUME_BASE/downloads"
# MAGIC
# MAGIC PHOTON_DB_ARCHIVE="$(basename "$PHOTON_DB_URL")"
# MAGIC PHOTON_STAGE="$LOCAL_ASSET_ROOT/photon-stage"
# MAGIC wget -nv -O "photon-${PHOTON_VERSION}.jar" "$PHOTON_JAR_URL"
# MAGIC wget -nv -O "$PHOTON_DB_ARCHIVE" "$PHOTON_DB_URL"
# MAGIC test -s "$PHOTON_DB_ARCHIVE"
# MAGIC
# MAGIC rm -rf "$PHOTON_STAGE"
# MAGIC mkdir -p "$PHOTON_STAGE"
# MAGIC tar -xjf "$PHOTON_DB_ARCHIVE" -C "$PHOTON_STAGE"
# MAGIC test -d "$PHOTON_STAGE/photon_data"
# MAGIC
# MAGIC rm -rf "$VOLUME_BASE/photon/photon_data"
# MAGIC mkdir -p "$VOLUME_BASE/photon/photon_data"
# MAGIC cp -R "$PHOTON_STAGE/photon_data/." "$VOLUME_BASE/photon/photon_data/"
# MAGIC rm -rf "$PHOTON_STAGE"
# MAGIC cp "photon-${PHOTON_VERSION}.jar" "$VOLUME_BASE/photon/"
# MAGIC printf '%s\n' "$PHOTON_DB_URL" > "$VOLUME_BASE/photon/source_url.txt"
# MAGIC
# MAGIC ls -lh "$VOLUME_BASE/photon"

# COMMAND ----------

# DBTITLE 1,Validate Photon dump against health query
# MAGIC %sh -e
# MAGIC source /tmp/routing_geocoding_demo_env.sh
# MAGIC sudo apt-get update -qq
# MAGIC sudo apt-get install -y --no-install-recommends \
# MAGIC   openjdk-21-jre-headless \
# MAGIC   curl \
# MAGIC   procps
# MAGIC PHOTON_VALIDATE_ROOT="$LOCAL_ASSET_ROOT/photon-validate"
# MAGIC PHOTON_VALIDATE_LOG="$LOCAL_ASSET_ROOT/logs/photon-validate.log"
# MAGIC PHOTON_VALIDATE_PORT="$((PHOTON_PORT + 100))"
# MAGIC rm -rf "$PHOTON_VALIDATE_ROOT"
# MAGIC mkdir -p "$PHOTON_VALIDATE_ROOT/photon_data" "$LOCAL_ASSET_ROOT/logs"
# MAGIC cp "$PHOTON_TARGET_DIR/photon-${PHOTON_VERSION}.jar" "$PHOTON_VALIDATE_ROOT/"
# MAGIC cp -R "$PHOTON_TARGET_DIR/photon_data/." "$PHOTON_VALIDATE_ROOT/photon_data/"
# MAGIC cd "$PHOTON_VALIDATE_ROOT"
# MAGIC nohup java -Xmx"$PHOTON_HEAP" -jar "photon-${PHOTON_VERSION}.jar" \
# MAGIC   -data-dir "$PHOTON_VALIDATE_ROOT" \
# MAGIC   -listen-port "$PHOTON_VALIDATE_PORT" \
# MAGIC   > "$PHOTON_VALIDATE_LOG" 2>&1 &
# MAGIC PHOTON_VALIDATE_PID=$!
# MAGIC cleanup_validate_photon() {
# MAGIC   if [[ -n "${PHOTON_VALIDATE_PID:-}" ]] && kill -0 "$PHOTON_VALIDATE_PID" 2>/dev/null; then
# MAGIC     kill "$PHOTON_VALIDATE_PID" 2>/dev/null || true
# MAGIC     wait "$PHOTON_VALIDATE_PID" 2>/dev/null || true
# MAGIC   fi
# MAGIC }
# MAGIC trap cleanup_validate_photon EXIT
# MAGIC for attempt in $(seq 1 45); do
# MAGIC   payload="$(curl -sf "http://127.0.0.1:${PHOTON_VALIDATE_PORT}/api?q=${PHOTON_HEALTH_QUERY}&limit=1")" || {
# MAGIC     sleep 2
# MAGIC     continue
# MAGIC   }
# MAGIC   if printf '%s' "$payload" | python3 -c 'import json,sys; data=json.load(sys.stdin); raise SystemExit(0 if data.get("features") else 1)'; then
# MAGIC     echo "Photon validation query returned at least one feature for ${PHOTON_HEALTH_QUERY}."
# MAGIC     exit 0
# MAGIC   fi
# MAGIC   sleep 2
# MAGIC done
# MAGIC echo "Photon validation failed for query ${PHOTON_HEALTH_QUERY}" >&2
# MAGIC cat "$PHOTON_VALIDATE_LOG" || true
# MAGIC exit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build and preprocess OSRM

# COMMAND ----------

# DBTITLE 1,Install OSRM build dependencies
# MAGIC %sh -e
# MAGIC source /tmp/routing_geocoding_demo_env.sh
# MAGIC sudo apt-get update -qq
# MAGIC sudo apt-get install -y --no-install-recommends \
# MAGIC   wget \
# MAGIC   curl \
# MAGIC   jq \
# MAGIC   libboost-program-options1.83.0=1.83.0-2.1ubuntu3.2 \
# MAGIC   libboost-iostreams1.83.0=1.83.0-2.1ubuntu3.2 \
# MAGIC   libboost-thread1.83.0=1.83.0-2.1ubuntu3.2 \
# MAGIC   build-essential \
# MAGIC   git \
# MAGIC   cmake \
# MAGIC   pkg-config \
# MAGIC   libboost-all-dev \
# MAGIC   libtbb-dev \
# MAGIC   lua5.2 \
# MAGIC   liblua5.2-dev \
# MAGIC   libbz2-dev \
# MAGIC   libzip-dev \
# MAGIC   libxml2-dev \
# MAGIC   zlib1g-dev

# COMMAND ----------

# DBTITLE 1,Clone and build OSRM
# MAGIC %sh -e
# MAGIC source /tmp/routing_geocoding_demo_env.sh
# MAGIC if [[ -x "$VOLUME_BASE/build/osrm/osrm-routed" && -f "$VOLUME_BASE/build/osrm/profiles/car.lua" ]]; then
# MAGIC   echo "OSRM build artifacts already present in $VOLUME_BASE/build/osrm, skipping build."
# MAGIC   ls -lh "$VOLUME_BASE/build/osrm"
# MAGIC   exit 0
# MAGIC fi
# MAGIC
# MAGIC sudo rm -rf "$LOCAL_OSRM_SRC" "$LOCAL_OSRM_BUILD"
# MAGIC git clone --depth 1 -b "$OSRM_VERSION" \
# MAGIC   https://github.com/Project-OSRM/osrm-backend.git "$LOCAL_OSRM_SRC"
# MAGIC sed -i 's/-Werror[ =a-zA-Z0-9_-]*//g' "$LOCAL_OSRM_SRC/CMakeLists.txt"
# MAGIC
# MAGIC mkdir -p "$LOCAL_OSRM_BUILD"
# MAGIC cd "$LOCAL_OSRM_BUILD"
# MAGIC export CXXFLAGS="-Wno-error=array-bounds -Wno-array-bounds"
# MAGIC cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_LTO=OFF "$LOCAL_OSRM_SRC"
# MAGIC cmake --build . -j"$(nproc)"
# MAGIC
# MAGIC rm -rf "$VOLUME_BASE/build/osrm"
# MAGIC mkdir -p "$VOLUME_BASE/build/osrm"
# MAGIC cp -RL "$LOCAL_OSRM_BUILD"/* "$VOLUME_BASE/build/osrm/"
# MAGIC cp -RL "$LOCAL_OSRM_SRC/profiles" "$VOLUME_BASE/build/osrm/"

# COMMAND ----------

# DBTITLE 1,Download and preprocess state extract
# MAGIC %sh -e
# MAGIC source /tmp/routing_geocoding_demo_env.sh
# MAGIC if [[ -f "$OSRM_TARGET_DIR/map.osrm.partition" ]]; then
# MAGIC   echo "Preprocessed map already present in $OSRM_TARGET_DIR, skipping extract/customize."
# MAGIC   ls -lh "$OSRM_TARGET_DIR"
# MAGIC   exit 0
# MAGIC fi
# MAGIC
# MAGIC sudo rm -rf "$LOCAL_OSRM_WORK" "$LOCAL_OSRM_BUILD"
# MAGIC mkdir -p "$LOCAL_OSRM_WORK" "$LOCAL_OSRM_BUILD"
# MAGIC cp -R "$VOLUME_BASE/build/osrm/." "$LOCAL_OSRM_BUILD/"
# MAGIC test -x "$LOCAL_OSRM_BUILD/osrm-extract"
# MAGIC test -x "$LOCAL_OSRM_BUILD/osrm-partition"
# MAGIC test -x "$LOCAL_OSRM_BUILD/osrm-customize"
# MAGIC cd "$LOCAL_OSRM_WORK"
# MAGIC
# MAGIC wget -nv -O "map.osm.pbf" "$PBF_URL"
# MAGIC "$LOCAL_OSRM_BUILD/osrm-extract" "map.osm.pbf" -p "$LOCAL_OSRM_BUILD/profiles/car.lua"
# MAGIC "$LOCAL_OSRM_BUILD/osrm-partition" "map.osrm"
# MAGIC "$LOCAL_OSRM_BUILD/osrm-customize" "map.osrm"
# MAGIC test -f "map.osrm.partition"
# MAGIC ls -lh map.osrm*
# MAGIC
# MAGIC rm -rf "$OSRM_TARGET_DIR"
# MAGIC mkdir -p "$OSRM_TARGET_DIR"
# MAGIC cp -RL map.osrm* "$OSRM_TARGET_DIR/"
# MAGIC test -f "$OSRM_TARGET_DIR/map.osrm.partition"
# MAGIC ls -lh "$OSRM_TARGET_DIR"

# COMMAND ----------

# DBTITLE 1,Render volume-backed init scripts
template_dir = Path.cwd() / "scripts" / "init"
env_file = f"{volume_base}/config/demo_env.sh"

for template_name in ["photon-worker.sh", "osrm-worker.sh"]:
    template = (template_dir / template_name).read_text()
    rendered = template.replace("__ROUTING_ENV_FILE__", env_file)
    dbutils.fs.put(f"{volume_base}/init/{template_name}", rendered, overwrite=True)

print(
    "Rendered init scripts:\n"
    f"- {volume_base}/init/photon-worker.sh\n"
    f"- {volume_base}/init/osrm-worker.sh"
)

# COMMAND ----------

# DBTITLE 1,Optional debug: run OSRM worker script on this cluster
if debug_start_osrm:
    import subprocess

    debug_script = Path("/tmp/routing_debug_osrm.sh")
    debug_script.write_text(
        f"""#!/bin/bash
set +e
set -x
source /tmp/routing_geocoding_demo_env.sh
echo "==== local OSRM workdir ===="
if [[ -d "${{LOCAL_OSRM_WORK}}" ]]; then
  ls -lah "${{LOCAL_OSRM_WORK}}"
else
  echo "missing: ${{LOCAL_OSRM_WORK}}"
fi
echo
echo "==== volume OSRM target ===="
if [[ -d "${{OSRM_TARGET_DIR}}" ]]; then
  ls -lah "${{OSRM_TARGET_DIR}}"
else
  echo "missing: ${{OSRM_TARGET_DIR}}"
fi
echo
if [[ -f "${{LOCAL_OSRM_WORK}}/map.osm.pbf" && ! -f "${{LOCAL_OSRM_WORK}}/map.osrm.partition" ]]; then
  echo "==== OSRM preprocess probe ===="
  rm -rf "${{LOCAL_OSRM_BUILD}}"
  mkdir -p "${{LOCAL_OSRM_BUILD}}"
  cp -R "${{VOLUME_BASE}}/build/osrm/." "${{LOCAL_OSRM_BUILD}}/"
  cd "${{LOCAL_OSRM_WORK}}"
  "${{LOCAL_OSRM_BUILD}}/osrm-extract" "map.osm.pbf" -p "${{LOCAL_OSRM_BUILD}}/profiles/car.lua"
  extract_rc=$?
  echo "osrm-extract exit code: $extract_rc"
  ls -lah
  if [[ "$extract_rc" -eq 0 ]]; then
    "${{LOCAL_OSRM_BUILD}}/osrm-partition" "map.osrm"
    partition_rc=$?
    echo "osrm-partition exit code: $partition_rc"
    if [[ "$partition_rc" -eq 0 ]]; then
      "${{LOCAL_OSRM_BUILD}}/osrm-customize" "map.osrm"
      customize_rc=$?
      echo "osrm-customize exit code: $customize_rc"
      ls -lah map.osrm*
    fi
  fi
  echo
fi
bash "{volume_base}/init/osrm-worker.sh"
rc=$?
echo
echo "osrm-worker.sh exit code: $rc"
if [[ -f "${{LOCAL_ASSET_ROOT}}/logs/osrm.log" ]]; then
  echo "==== tail ${{LOCAL_ASSET_ROOT}}/logs/osrm.log ===="
  tail -200 "${{LOCAL_ASSET_ROOT}}/logs/osrm.log"
fi
exit 0
"""
    )
    debug_script.chmod(0o755)

    result = subprocess.run(
        ["bash", str(debug_script)],
        check=False,
        capture_output=True,
        text=True,
    )
    debug_output = (
        "STDOUT\n"
        f"{result.stdout}\n"
        "STDERR\n"
        f"{result.stderr}\n"
        f"RETURN_CODE={result.returncode}\n"
    )
    dbutils.fs.put(
        f"{volume_base}/debug/osrm_worker_debug.txt",
        debug_output,
        overwrite=True,
    )
    print(debug_output)
    print(f"Wrote debug log to {volume_base}/debug/osrm_worker_debug.txt")
else:
    print("Skipping optional OSRM worker debug run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC
# MAGIC Use the rendered init scripts above on the cluster that will run stage 1 validation and
# MAGIC the downstream geocoding / distance notebooks, then run `02_validate_services.py`.
