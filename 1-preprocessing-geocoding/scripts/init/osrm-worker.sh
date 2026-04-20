#!/bin/bash
set -euo pipefail

BOOTSTRAP_ASSET_ROOT="${LOCAL_ASSET_ROOT:-/local_disk0/routing-geocoding}"
mkdir -p "${BOOTSTRAP_ASSET_ROOT}/logs"

ENV_FILE="${ROUTING_GEOCODING_ENV_FILE:-__ROUTING_ENV_FILE__}"
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "OSRM init: missing env file at ${ENV_FILE}" >&2
  exit 1
fi

source "${ENV_FILE}"

DEBUG_LOG="${BOOTSTRAP_ASSET_ROOT}/logs/osrm-init-$(hostname).log"
exec > >(tee -a "${DEBUG_LOG}") 2>&1
set -x

trap 'rc=$?; line=${BASH_LINENO[0]}; echo "OSRM init: failed at line ${line} with exit code ${rc}"; if [[ -n "${LOCAL_ASSET_ROOT:-}" && -f "${LOCAL_ASSET_ROOT}/logs/osrm.log" ]]; then tail -200 "${LOCAL_ASSET_ROOT}/logs/osrm.log"; fi; exit "${rc}"' ERR

sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
  libboost-program-options1.83.0=1.83.0-2.1ubuntu3.2 \
  libboost-iostreams1.83.0=1.83.0-2.1ubuntu3.2 \
  libboost-thread1.83.0=1.83.0-2.1ubuntu3.2 \
  curl

mkdir -p "${LOCAL_ASSET_ROOT}" "${LOCAL_ASSET_ROOT}/logs"
rm -rf "${LOCAL_ASSET_ROOT}/osrm"
mkdir -p "${LOCAL_ASSET_ROOT}/osrm/build" "${LOCAL_ASSET_ROOT}/osrm/maps/${REGION}"
ls -lh "${VOLUME_BASE}/build/osrm"
ls -lh "${OSRM_TARGET_DIR}"
if [[ ! -f "${OSRM_TARGET_DIR}/map.osrm.partition" ]]; then
  echo "OSRM init: missing preprocessed OSRM artifacts in ${OSRM_TARGET_DIR}" >&2
  exit 1
fi

cp -R "${VOLUME_BASE}/build/osrm/." "${LOCAL_ASSET_ROOT}/osrm/build/"
cp -R "${OSRM_TARGET_DIR}/." "${LOCAL_ASSET_ROOT}/osrm/maps/${REGION}/"
test -f "${LOCAL_ASSET_ROOT}/osrm/maps/${REGION}/map.osrm.partition"

pkill -f "osrm-routed" || true

cd "${LOCAL_ASSET_ROOT}/osrm/maps/${REGION}"
ldd "${LOCAL_ASSET_ROOT}/osrm/build/osrm-routed" || true
nohup "${LOCAL_ASSET_ROOT}/osrm/build/osrm-routed" \
  --algorithm=MLD \
  --port="${OSRM_PORT}" \
  --threads="${OSRM_THREADS}" \
  --max-table-size="${OSRM_MAX_TABLE_SIZE}" \
  "map.osrm" \
  > "${LOCAL_ASSET_ROOT}/logs/osrm.log" 2>&1 &

PRIMARY_HEALTH_URL="http://127.0.0.1:${OSRM_PORT}/route/v1/driving/${OSRM_HEALTH_ROUTE}?overview=false"
FALLBACK_HEALTH_URL="http://127.0.0.1:${OSRM_PORT}/nearest/v1/driving/${OSRM_HEALTH_ROUTE%%;*}?number=1"

for attempt in $(seq 1 60); do
  if curl -sf "${PRIMARY_HEALTH_URL}" >/dev/null || curl -sf "${FALLBACK_HEALTH_URL}" >/dev/null; then
    echo "OSRM init: service is healthy on port ${OSRM_PORT}"
    exit 0
  fi
  if [[ -f "${LOCAL_ASSET_ROOT}/logs/osrm.log" ]]; then
    tail -20 "${LOCAL_ASSET_ROOT}/logs/osrm.log" || true
  fi
  sleep 2
done

echo "OSRM init: service failed to become healthy" >&2
if [[ -f "${LOCAL_ASSET_ROOT}/logs/osrm.log" ]]; then
  tail -200 "${LOCAL_ASSET_ROOT}/logs/osrm.log" || true
fi
exit 1
