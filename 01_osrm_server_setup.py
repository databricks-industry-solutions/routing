# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/routing.git. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/scalable-route-generation.

# COMMAND ----------

# MAGIC %md The purpose of this notebook is to compile the OSRM software and pre-process the OpenStreetMap files required to generate map routes from within a Databricks cluster.

# COMMAND ----------

# MAGIC %md ## Introduction
# MAGIC
# MAGIC The steps performed in this notebook illustrate how you may prepare the assets required to run the OSRM Backend Server within a Databricks cluster.  These steps are performed infrequently, possibly even once, ahead of the launching of an OSRM-equipped Databricks cluster.  
# MAGIC
# MAGIC To perform this work, **it is recommended you run this notebook as a Dedicated, single-node cluster**, *i.e.* a cluster with only a driver and no worker nodes. (To deploy a single node cluster, select *single node* from the *Create Cluster* page.) The driver server needs to have a very large volume of RAM assigned to it relative to the size of the map files to be processed.  To give a general sense of the sizing of the driver node, we found a 128 GB RAM driver node was required to process the 11.5 GB North American *.osm.pbf* map file downloaded from the [GeoFabrik website](https://download.geofabrik.de/). For the Indiana example in this demo less memory is required.
# MAGIC </p>
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/osrm_preprocessing_deployment2.png' width=500>

# COMMAND ----------

# DBTITLE 1,Config
catalog = "default"
schema = "routing"
volume = "osrm_backend"

# COMMAND ----------

# DBTITLE 1,Create Volume
spark.sql(f'create catalog if not exists {catalog}')
spark.sql(f'use catalog {catalog}')
spark.sql(f'create schema if not exists {schema}')
spark.sql(f'use schema {schema}')
spark.sql(f'create volume if not exists {volume}')

# COMMAND ----------

# DBTITLE 1,Set Environment Variables
import os
os.environ['CATALOG'] = catalog
os.environ['SCHEMA'] = schema
os.environ['VOLUME'] = volume

# COMMAND ----------

# DBTITLE 1,Configure Scripting Variables
# MAGIC %sh -e
# MAGIC # Write variables to a script
# MAGIC cat << 'EOF' > /tmp/osrm_env.sh
# MAGIC export VOLUME_BASE=/Volumes/$CATALOG/$SCHEMA/$VOLUME
# MAGIC export PROFILE=$VOLUME_BASE/osrm-src/profiles/car.lua
# MAGIC export LOCAL_OSRM=/tmp/osrm_test
# MAGIC export OSRM_VERSION=v6.0.0
# MAGIC export SRC_ROOT=/local_disk0/osrm-src          # local SSD, POSIX OK
# MAGIC export BUILD_ROOT=/local_disk0/osrm-build
# MAGIC export BIN_DIR=$VOLUME_BASE/build                  # osrm-extract, etc.
# MAGIC export PROFILE=$VOLUME_BASE/osrm-src/profiles/car.lua
# MAGIC export LOCAL_WORK=/local_disk0/osrm-work           # fast NVMe
# MAGIC export REGION=indiana
# MAGIC export PBF_URL=https://download.geofabrik.de/north-america/us/${REGION}-latest.osm.pbf
# MAGIC export TARGET_DIR=$VOLUME_BASE/maps/${REGION}
# MAGIC export PORT=5000
# MAGIC EOF
# MAGIC
# MAGIC
# MAGIC # Copy script to Volume so it persists beyond driver restarts
# MAGIC mkdir -p /Volumes/$CATALOG/$SCHEMA/$VOLUME
# MAGIC cp /tmp/osrm_env.sh /Volumes/$CATALOG/$SCHEMA/$VOLUME/osrm_env.sh

# COMMAND ----------

# DBTITLE 1,Source Scripting Variables
# MAGIC %sh -e
# MAGIC # 1. source the shared environment:
# MAGIC source /Volumes/$CATALOG/$SCHEMA/$VOLUME/osrm_env.sh
# MAGIC echo "VOLUME_BASE is $VOLUME_BASE"
# MAGIC echo "PROFILE is $PROFILE"
# MAGIC echo "REGION is $REGION"
# MAGIC echo "PORT is $PORT"
# MAGIC echo "LOCAL_OSRM is $LOCAL_OSRM"
# MAGIC
# MAGIC # 2. everything else can use those vars directly:
# MAGIC mkdir -p "$LOCAL_OSRM"
# MAGIC cp -R "$VOLUME_BASE" "$LOCAL_OSRM"

# COMMAND ----------

# DBTITLE 1,Install OSRM Build Dependencies
# MAGIC %sh -e
# MAGIC sudo apt-get update -qq
# MAGIC # core build chain + headers
# MAGIC sudo apt-get install -y --no-install-recommends \
# MAGIC         build-essential git cmake pkg-config \
# MAGIC         libboost-all-dev libtbb-dev \
# MAGIC         lua5.2 liblua5.2-dev \
# MAGIC         libbz2-dev libzip-dev \
# MAGIC         libxml2-dev

# COMMAND ----------

# DBTITLE 1,Build OSRM
# MAGIC %sh -e
# MAGIC source /Volumes/$CATALOG/$SCHEMA/$VOLUME/osrm_env.sh
# MAGIC # 1. clean up any previous attempt
# MAGIC sudo rm -rf "$SRC_ROOT" "$BUILD_ROOT"
# MAGIC
# MAGIC # 2. clone the source (no symlink problems on local SSD)
# MAGIC git clone --depth 1 -b "$OSRM_VERSION" \
# MAGIC         https://github.com/Project-OSRM/osrm-backend.git "$SRC_ROOT"
# MAGIC
# MAGIC # 3. remove all -Werror flags *once* (prevents any sub-targets from re-adding them)
# MAGIC sed -i 's/-Werror[ =a-zA-Z0-9_-]*//g' "$SRC_ROOT/CMakeLists.txt"
# MAGIC
# MAGIC # 4. configure & build in an out-of-tree directory
# MAGIC mkdir -p "$BUILD_ROOT" && cd "$BUILD_ROOT"
# MAGIC export CXXFLAGS="-Wno-error=array-bounds -Wno-array-bounds"
# MAGIC cmake -DCMAKE_BUILD_TYPE=Release \
# MAGIC       -DENABLE_LTO=OFF \
# MAGIC       "$SRC_ROOT"
# MAGIC cmake --build . -j"$(nproc)"

# COMMAND ----------

# DBTITLE 1,Copy and Source OSRM
# MAGIC %sh 
# MAGIC source /Volumes/$CATALOG/$SCHEMA/$VOLUME/osrm_env.sh
# MAGIC cp -RL "$SRC_ROOT"        "$VOLUME_BASE/osrm-src"
# MAGIC cp -RL "$BUILD_ROOT"      "$VOLUME_BASE/build"

# COMMAND ----------

# MAGIC %md
# MAGIC If a `no peak bytes used message` like below was logged, you might have OOMed. Try a cluster with more memory or using a smaller dataset/region </br>
# MAGIC `[2025-06-01T18:47:12.600322491] [info] RAM: peak bytes used: 1848184832`

# COMMAND ----------

# DBTITLE 1,Extract Map
# MAGIC %sh -e
# MAGIC source /Volumes/$CATALOG/$SCHEMA/$VOLUME/osrm_env.sh
# MAGIC # 0 ▸ fresh local workspace
# MAGIC sudo rm -rf "$LOCAL_WORK"
# MAGIC mkdir -p  "$LOCAL_WORK"
# MAGIC cd        "$LOCAL_WORK"
# MAGIC
# MAGIC # 1 ▸ download PBF onto NVMe
# MAGIC wget -q "$PBF_URL" -O map.osm.pbf
# MAGIC echo "Downloaded $(du -h map.osm.pbf | cut -f1) → $LOCAL_WORK/map.osm.pbf"
# MAGIC
# MAGIC # 2 ▸ build the graph (all local, mmap-friendly)
# MAGIC $BIN_DIR/osrm-extract   map.osm.pbf -p "$PROFILE"
# MAGIC $BIN_DIR/osrm-partition map.osrm
# MAGIC $BIN_DIR/osrm-customize map.osrm
# MAGIC
# MAGIC # 3 ▸ copy only the necessary .osrm* outputs to the volume
# MAGIC rm -rf "$TARGET_DIR" && mkdir -p "$TARGET_DIR"
# MAGIC cp -RL map.osrm* "$TARGET_DIR"
# MAGIC
# MAGIC echo -e "\n✅  $REGION graph ready at $TARGET_DIR"
# MAGIC ls -lh "$TARGET_DIR"

# COMMAND ----------

# DBTITLE 1,Launch OSRM Router
# MAGIC %sh -e
# MAGIC source /Volumes/$CATALOG/$SCHEMA/$VOLUME/osrm_env.sh
# MAGIC rm -rf "$LOCAL_OSRM"
# MAGIC time cp -R "$VOLUME_BASE" "$LOCAL_OSRM"          # measure copy time
# MAGIC
# MAGIC echo "Launching router ..."
# MAGIC cd "$LOCAL_OSRM/maps/$REGION"
# MAGIC "$LOCAL_OSRM/build/osrm-routed" map.osrm \
# MAGIC         --algorithm=MLD --port=$PORT --threads=2 >/tmp/osrm_test.log 2>&1 &

# COMMAND ----------

# DBTITLE 1,Test
# MAGIC %sh
# MAGIC curl -v "http://localhost:5000/route/v1/driving/39.204907,-86.520124;39.78208,-86.077596?overview=false"

# COMMAND ----------

# DBTITLE 1,Update Init Script
import re
from pathlib import Path

script_path   = Path.cwd() / "osrm-backend.sh"
content       = script_path.read_text()

# 1️⃣  Build the new export block
export_block  = (
    f'export CATALOG="{catalog}"\n'
    f'export SCHEMA="{schema}"\n'
    f'export VOLUME="{volume}"'
)

# Replace an existing block *or* insert right after the shebang
if re.search(r"^export\s+CATALOG=", content, flags=re.MULTILINE):
    # Overwrite all three lines as a single block (handles prior ordering)
    content = re.sub(
        r"^export\s+CATALOG=.*\n^export\s+SCHEMA=.*\n^export\s+VOLUME=.*",
        export_block,
        content,
        flags=re.MULTILINE,
    )
else:
    # Insert after #!/bin/bash (or any shebang)
    content = re.sub(
        r"^(#![^\n]*\n)",
        r"\1" + export_block + "\n",
        content,
        count=1,
        flags=re.MULTILINE,
    )

# 2️⃣  Update the dynamic source line
volume_base   = f"/Volumes/{catalog}/{schema}/{volume}"
new_source    = f"source {volume_base}/osrm_env.sh"
content       = re.sub(
    r"^source\s+.*osrm_env\.sh.*$",
    new_source,
    content,
    flags=re.MULTILINE,
)

# 3️⃣  Write back if anything changed
script_path.write_text(content)
print(f"✔ Patched {script_path.name}\n"
      f"  • CATALOG={catalog}, SCHEMA={schema}, VOLUME={volume}\n"
      f"  • source line → {new_source}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Troubleshooting:

# COMMAND ----------

# DBTITLE 1,Process Running
# %sh
# ps -ef | grep osrm-routed | grep -v grep || echo "No osrm-routed process found"

# COMMAND ----------

# DBTITLE 1,Listening on Port
# %sh
# NETSTAT=$(netstat -nlp 2>/dev/null | grep osrm-routed || true)
# if [ -z "$NETSTAT" ]; then
#   echo "osrm-routed is not listening on any TCP port"
# else
#   echo "osrm-routed listening socket:"
#   echo "$NETSTAT"
# fi

# COMMAND ----------

# DBTITLE 1,Files Downloaded
# %sh
# ls -lh /tmp/osrm_test/maps/us-northeast/*.osrm*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC | library                | description                                                                                      | license      | source                                                    |
# MAGIC |------------------------|--------------------------------------------------------------------------------------------------|--------------|-----------------------------------------------------------|
# MAGIC | OSRM Backend Server    | High performance routing engine written in C++14 designed to run on OpenStreetMap data           | BSD 2-Clause "Simplified" License | https://github.com/Project-OSRM/osrm-backend              |
# MAGIC | osmnx                  | Download, model, analyze, and visualize street networks and other geospatial features from OpenStreetMap in Python | MIT License  | https://github.com/gboeing/osmnx                          |
# MAGIC | ortools                | Operations research tools developed at Google for combinatorial optimization                     | Apache License 2.0 | https://github.com/google/or-tools                        |
# MAGIC | folium                 | Visualize data in Python on interactive Leaflet.js maps                                          | MIT License  | https://github.com/python-visualization/folium            |
# MAGIC | dash                   | Python framework for building analytical web applications and dashboards; built on Flask, React, and Plotly.js | MIT License  | https://github.com/plotly/dash                            |
# MAGIC | branca                 | Library for generating complex HTML+JS pages in Python; provides non-map-specific features for folium | MIT License  | https://github.com/python-visualization/branca            |
# MAGIC | plotly                 | Open-source Python library for creating interactive, publication-quality charts and graphs        | MIT License  | https://github.com/plotly/plotly.py                       |
# MAGIC ray |	Flexible, high-performance distributed execution framework for scaling Python workflows |	Apache2.0 |	https://github.com/ray-project/ray