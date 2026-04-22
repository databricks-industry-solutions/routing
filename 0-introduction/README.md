# Stage 0: Introduction

This is the fast path for learning the accelerator and validating serverless compute.
It uses the checked-in `../utils/shipments.csv` sample, builds synthetic haversine
travel times, and avoids Photon, OSRM, init scripts, and Ray.

## Notebooks

- `01_cpu_intro.py`
  Runs a small capacity-constrained OR-Tools solve on serverless CPU
  (environment version 5).
- `02_gpu_intro.py`
  Runs a small cuOpt solve on serverless GPU (environment version 4 on A10) with the
  same repo-local sample.

## Default outputs

- `demos.routing.intro_shipments`
- `demos.routing.intro_routes_cpu`
- `demos.routing.intro_routes_gpu`

Both route outputs keep the same core schema shape used elsewhere in the repo:
`cluster_id`, `truck_type`, `route_index`, `package_id`, `latitude`, `longitude`.

Both intro notebooks default to the same 50-row subset so `intro_shipments` stays
consistent across the CPU and GPU examples.

## When to use it

- First validation in a blank workspace
- Quick regression check for serverless support
- Learning the routing table contract before the Photon + OSRM workflow

## Manual run notes

- Run `01_cpu_intro.py` on serverless CPU (environment version 5).
- Run `02_gpu_intro.py` on serverless GPU, preferably `GPU_1xA10` with environment
  version 4.
- The bundle config wires the needed serverless environments for job runs.
- The GPU notebook installs `cuopt-cu12==25.8.0` itself because scheduled
  serverless GPU jobs do not support bundle-managed Python dependencies.
- For an interactive run, add dependencies in the notebook Environment panel first:
  - CPU intro: `ortools==9.14.6206`
  - GPU intro: none required beyond the notebook's built-in
    `%pip install --extra-index-url=https://pypi.nvidia.com cuopt-cu12==25.8.0`
