#!/usr/bin/env python3
"""Create or update the recommended classic clusters for manual notebook runs.

Run this helper from the repo root after authenticating with the Databricks CLI.
Use `--asset-prep-only` for the first pass if the shared init scripts have not been
rendered yet, then rerun without that flag after `01_prepare_assets.py` completes.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from typing import Any

try:
    from databricks.sdk import WorkspaceClient
except ImportError as exc:  # pragma: no cover - import guard for user guidance
    raise SystemExit(
        "Install databricks-sdk first: python3 -m pip install databricks-sdk"
    ) from exc


AWS_DEFAULTS = {
    "cpu_node_type_id": "m5d.4xlarge",
    "geocode_node_type_id": "m5d.4xlarge",
}

AZURE_DEFAULTS = {
    "cpu_node_type_id": "Standard_D16ds_v5",
    "geocode_node_type_id": "Standard_D32ds_v5",
}

DEFAULT_SPARK_CONF = {
    "spark.databricks.pyspark.dataFrameChunk.enabled": "true",
    "spark.task.resource.gpu.amount": "0",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create or update the named classic clusters recommended by this accelerator "
            "for manual notebook runs."
        )
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile name. Defaults to the active profile.",
    )
    parser.add_argument(
        "--cloud",
        choices=["auto", "aws", "azure"],
        default="auto",
        help="Cloud to target. Defaults to auto-detect from the workspace host.",
    )
    parser.add_argument("--catalog", default="demos", help="Unity Catalog catalog name.")
    parser.add_argument("--schema", default="routing", help="Unity Catalog schema name.")
    parser.add_argument(
        "--volume",
        default="routing_assets",
        help="Managed volume that stores rendered init scripts.",
    )
    parser.add_argument(
        "--spark-version",
        default="17.3.x-cpu-ml-scala2.13",
        help="Classic cluster runtime for asset prep, geocoding, and CPU routing.",
    )
    parser.add_argument(
        "--cpu-node-type-id",
        default=None,
        help="Override the default CPU node type for asset prep and CPU clusters.",
    )
    parser.add_argument(
        "--geocode-node-type-id",
        default=None,
        help="Override the default node type for the Photon + OSRM geocode cluster.",
    )
    parser.add_argument(
        "--autotermination-minutes",
        type=int,
        default=30,
        help="Autotermination window for the created clusters.",
    )
    parser.add_argument(
        "--asset-prep-only",
        action="store_true",
        help="Create or update only `asset_prep_cluster` for the first setup pass.",
    )
    parser.add_argument(
        "--start",
        action="store_true",
        help="Start clusters after creating or updating them.",
    )
    return parser.parse_args()


def detect_cloud(host: str, requested_cloud: str) -> str:
    if requested_cloud != "auto":
        return requested_cloud
    return "azure" if "azuredatabricks.net" in host else "aws"


def availability_attributes(cloud: str) -> dict[str, Any]:
    if cloud == "azure":
        return {
            "azure_attributes": {
                "availability": "SPOT_WITH_FALLBACK_AZURE",
                "first_on_demand": 1,
            }
        }
    return {
        "aws_attributes": {
            "availability": "SPOT_WITH_FALLBACK",
            "first_on_demand": 1,
        }
    }


def volume_init_script(path: str) -> dict[str, Any]:
    return {"volumes": {"destination": path}}


def build_cluster_payload(
    *,
    name: str,
    cloud: str,
    single_user_name: str,
    spark_version: str,
    node_type_id: str,
    num_workers: int,
    autotermination_minutes: int,
    spark_conf: dict[str, str] | None = None,
    init_scripts: list[dict[str, Any]] | None = None,
    single_node: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "cluster_name": name,
        "spark_version": spark_version,
        "node_type_id": node_type_id,
        "num_workers": num_workers,
        "autotermination_minutes": autotermination_minutes,
        "data_security_mode": "SINGLE_USER",
        "single_user_name": single_user_name,
        **availability_attributes(cloud),
    }

    merged_spark_conf = deepcopy(spark_conf or {})
    if single_node:
        merged_spark_conf.update(
            {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            }
        )
        payload["custom_tags"] = {"ResourceClass": "SingleNode"}

    if merged_spark_conf:
        payload["spark_conf"] = merged_spark_conf
    if init_scripts:
        payload["init_scripts"] = deepcopy(init_scripts)

    return payload


def build_cluster_specs(
    *,
    cloud: str,
    single_user_name: str,
    spark_version: str,
    cpu_node_type_id: str,
    geocode_node_type_id: str,
    autotermination_minutes: int,
    init_root: str,
    asset_prep_only: bool,
) -> list[dict[str, Any]]:
    osrm_init = [volume_init_script(f"{init_root}/osrm-worker.sh")]
    photon_and_osrm_init = [
        volume_init_script(f"{init_root}/photon-worker.sh"),
        volume_init_script(f"{init_root}/osrm-worker.sh"),
    ]

    specs = [
        build_cluster_payload(
            name="asset_prep_cluster",
            cloud=cloud,
            single_user_name=single_user_name,
            spark_version=spark_version,
            node_type_id=cpu_node_type_id,
            num_workers=0,
            autotermination_minutes=autotermination_minutes,
            single_node=True,
        )
    ]
    if asset_prep_only:
        return specs

    specs.append(
        build_cluster_payload(
            name="geocode_cluster",
            cloud=cloud,
            single_user_name=single_user_name,
            spark_version=spark_version,
            node_type_id=geocode_node_type_id,
            num_workers=0 if cloud == "azure" else 2,
            autotermination_minutes=autotermination_minutes,
            spark_conf=None if cloud == "azure" else DEFAULT_SPARK_CONF,
            init_scripts=photon_and_osrm_init,
            single_node=(cloud == "azure"),
        )
    )
    specs.append(
        build_cluster_payload(
            name="cpu_distance_cluster",
            cloud=cloud,
            single_user_name=single_user_name,
            spark_version=spark_version,
            node_type_id=cpu_node_type_id,
            num_workers=4,
            autotermination_minutes=autotermination_minutes,
            spark_conf=DEFAULT_SPARK_CONF,
            init_scripts=osrm_init,
        )
    )
    specs.append(
        build_cluster_payload(
            name="cpu_optimize_cluster",
            cloud=cloud,
            single_user_name=single_user_name,
            spark_version=spark_version,
            node_type_id=cpu_node_type_id,
            num_workers=4,
            autotermination_minutes=autotermination_minutes,
            spark_conf=DEFAULT_SPARK_CONF,
        )
    )
    specs.append(
        build_cluster_payload(
            name="gpu_distance_cluster",
            cloud=cloud,
            single_user_name=single_user_name,
            spark_version=spark_version,
            node_type_id=cpu_node_type_id,
            num_workers=2,
            autotermination_minutes=autotermination_minutes,
            spark_conf=DEFAULT_SPARK_CONF,
            init_scripts=osrm_init,
        )
    )
    return specs


def get_cluster_state_name(state: Any) -> str:
    if state is None:
        return "UNKNOWN"
    return getattr(state, "value", str(state))


def create_or_update_clusters(
    w: WorkspaceClient, cluster_specs: list[dict[str, Any]], start_clusters: bool
) -> None:
    existing = {
        cluster.cluster_name: cluster
        for cluster in w.clusters.list()
        if cluster.cluster_name and cluster.cluster_id
    }

    for spec in cluster_specs:
        name = spec["cluster_name"]
        cluster = existing.get(name)
        if cluster:
            edit_payload = deepcopy(spec)
            edit_payload["cluster_id"] = cluster.cluster_id
            w.api_client.do("POST", "/api/2.0/clusters/edit", body=edit_payload)
            cluster_id = cluster.cluster_id
            action = "updated"
        else:
            response = w.api_client.do("POST", "/api/2.0/clusters/create", body=spec)
            cluster_id = response["cluster_id"]
            action = "created"

        print(f"{action}: {name} ({cluster_id})")

        if not start_clusters:
            continue

        cluster_details = w.clusters.get(cluster_id=cluster_id)
        state_name = get_cluster_state_name(cluster_details.state).upper()
        if state_name != "RUNNING":
            w.clusters.start(cluster_id=cluster_id).result()
            print(f"started: {name}")
        else:
            print(f"already running: {name}")


def main() -> None:
    args = parse_args()
    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    host = w.config.host or ""
    cloud = detect_cloud(host, args.cloud)
    defaults = AZURE_DEFAULTS if cloud == "azure" else AWS_DEFAULTS
    current_user = w.current_user.me()
    single_user_name = current_user.user_name
    init_root = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/init"

    print(f"workspace: {host}")
    print(f"cloud: {cloud}")
    print(f"single-user owner: {single_user_name}")
    print(f"init script root: {init_root}")
    if args.asset_prep_only:
        print("mode: asset prep only")
    else:
        print(
            "note: run `1-preprocessing-geocoding/01_prepare_assets.py` first if the "
            "volume-backed init scripts are not rendered yet."
        )

    cluster_specs = build_cluster_specs(
        cloud=cloud,
        single_user_name=single_user_name,
        spark_version=args.spark_version,
        cpu_node_type_id=args.cpu_node_type_id or defaults["cpu_node_type_id"],
        geocode_node_type_id=args.geocode_node_type_id
        or defaults["geocode_node_type_id"],
        autotermination_minutes=args.autotermination_minutes,
        init_root=init_root,
        asset_prep_only=args.asset_prep_only,
    )
    create_or_update_clusters(w, cluster_specs, start_clusters=args.start)


if __name__ == "__main__":
    main()
