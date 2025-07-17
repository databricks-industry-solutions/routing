# Databricks notebook source
# MAGIC %pip install databricks-sdk==0.58.0
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Cluster Setup
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.compute import InitScriptInfo, WorkspaceStorageInfo, DataSecurityMode


cloud_map = {
    "aws": "m5d.4xlarge",
    "azure": "Standard_D16ds_v5",
    "gcp": "n2-standard-16"      
}

def detect_cloud(host: str) -> str:
    if ".azuredatabricks." in host:
        return "azure"
    if ".gcp.databricks." in host or ".cloud.databricks." in host and "gcp" in host:
        return "gcp"
    return "aws"

w = WorkspaceClient()
cloud = detect_cloud(w.config.host.lower())
node_type = cloud_map[cloud]


init_script = InitScriptInfo(
    workspace=WorkspaceStorageInfo(destination=f"{os.getcwd()}/osrm-backend.sh")
)

cluster_config = {
    "cluster_name": "Routing Optimization Cluster",
    "num_workers": 4,
    "node_type_id": node_type,
    "spark_version": "16.4.x-cpu-ml-scala2.12",
    "autoscale": None,
    "init_scripts": [init_script],
    "autotermination_minutes": 65,
    "data_security_mode": DataSecurityMode.SINGLE_USER,
    "single_user_name": w.current_user.me().user_name,
    "spark_conf": {
        # enable Ray-friendly chunking of PySpark partitions
        "spark.databricks.pyspark.dataFrameChunk.enabled": "true",
        # make every Spark task ask for zero GPUs (so you can
        # run on CPU instances without triggering GPU scheduling logic)
        "spark.task.resource.gpu.amount": "0"
    }
}

# Create or update cluster
try:
    new_cluster = w.clusters.create(**cluster_config)
    print(f"Created cluster → {w.config.host}#setting/clusters/{new_cluster.cluster_id}/configuration")
except ResourceAlreadyExists:
    print("Cluster exists. Use the existing cluster, rename the existing cluster, or delete the cluster and try again")

# COMMAND ----------

# DBTITLE 1,App Deployment
# from databricks.sdk.service.apps import AppDeploymentMode, AppDeployment
# from databricks.sdk.errors import NotFound
# app_name = "routing-optimization"

# try:
#     w.apps.create_and_wait(
#         name=app_name,
#         description="Routing optimization demo",
#         user_api_scopes=["sql"],       # on behalf of user -> run SQL as the user
#     )
# except ResourceAlreadyExists:
#     w.apps.deploy(
#         app_name=app_name,
#         app_deployment=AppDeployment( 
#             source_code_path=f"{os.getcwd()}/routing_app",
#             mode=AppDeploymentMode.SNAPSHOT
#         ),
#     )

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
