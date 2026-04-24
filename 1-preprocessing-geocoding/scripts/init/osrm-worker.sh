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
DEBUG_TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
VOLUME_DEBUG_DIR="${VOLUME_BASE}/debug"
exec > >(tee -a "${DEBUG_LOG}") 2>&1
set -x

persist_debug_artifacts() {
  mkdir -p "${VOLUME_DEBUG_DIR}" || true
  cp "${DEBUG_LOG}" "${VOLUME_DEBUG_DIR}/osrm-init-${DEBUG_TIMESTAMP}-$(hostname).log" || true
  if [[ -n "${LOCAL_ASSET_ROOT:-}" && -f "${LOCAL_ASSET_ROOT}/logs/osrm.log" ]]; then
    cp "${LOCAL_ASSET_ROOT}/logs/osrm.log" "${VOLUME_DEBUG_DIR}/osrm-runtime-${DEBUG_TIMESTAMP}-$(hostname).log" || true
  fi
}

trap 'rc=$?; line=${BASH_LINENO[0]}; echo "OSRM init: failed at line ${line} with exit code ${rc}"; if [[ -n "${LOCAL_ASSET_ROOT:-}" && -f "${LOCAL_ASSET_ROOT}/logs/osrm.log" ]]; then tail -200 "${LOCAL_ASSET_ROOT}/logs/osrm.log"; fi; persist_debug_artifacts; exit "${rc}"' ERR

apt_get_retry() {
  local attempt
  for attempt in $(seq 1 6); do
    if sudo DEBIAN_FRONTEND=noninteractive apt-get "$@"; then
      return 0
    fi
    if [[ "${attempt}" -lt 6 ]]; then
      echo "OSRM init: apt-get $* failed on attempt ${attempt}, retrying in 10s" >&2
      sleep 10
    fi
  done
  echo "OSRM init: apt-get $* failed after retries" >&2
  return 1
}

mkdir -p "${LOCAL_ASSET_ROOT}" "${LOCAL_ASSET_ROOT}/logs"
rm -rf "${LOCAL_ASSET_ROOT}/osrm"
mkdir -p "${LOCAL_ASSET_ROOT}/osrm/build" "${LOCAL_ASSET_ROOT}/osrm/maps/${REGION}"
ls -lh "${VOLUME_BASE}/build/osrm"
ls -lh "${OSRM_TARGET_DIR}"
ls -lh "${OSRM_BUILD_ARCHIVE_PATH}" || true
ls -lh "${OSRM_MAP_ARCHIVE_PATH}" || true
if [[ ! -f "${OSRM_TARGET_DIR}/map.osrm.partition" && ! -f "${OSRM_MAP_ARCHIVE_PATH}" ]]; then
  echo "OSRM init: missing preprocessed OSRM artifacts in ${OSRM_TARGET_DIR}" >&2
  exit 1
fi

if [[ -f "${OSRM_BUILD_ARCHIVE_PATH}" && -f "${OSRM_MAP_ARCHIVE_PATH}" ]]; then
  tar -xf "${OSRM_BUILD_ARCHIVE_PATH}" -C "${LOCAL_ASSET_ROOT}/osrm/build"
  tar -xf "${OSRM_MAP_ARCHIVE_PATH}" -C "${LOCAL_ASSET_ROOT}/osrm/maps/${REGION}"
else
  cp -R "${VOLUME_BASE}/build/osrm/." "${LOCAL_ASSET_ROOT}/osrm/build/"
  cp -R "${OSRM_TARGET_DIR}/." "${LOCAL_ASSET_ROOT}/osrm/maps/${REGION}/"
fi
test -x "${LOCAL_ASSET_ROOT}/osrm/build/osrm-routed"
test -f "${LOCAL_ASSET_ROOT}/osrm/build/profiles/car.lua"
test -f "${LOCAL_ASSET_ROOT}/osrm/maps/${REGION}/map.osrm.partition"

OSRM_LDD_OUTPUT="$(ldd "${LOCAL_ASSET_ROOT}/osrm/build/osrm-routed" 2>&1 || true)"
printf '%s\n' "${OSRM_LDD_OUTPUT}"
if [[ "${OSRM_LDD_OUTPUT}" == *"not found"* ]] || ! command -v curl >/dev/null || ! command -v pkill >/dev/null; then
  apt_get_retry update -qq
  apt_get_retry install -y --no-install-recommends \
    libboost-program-options1.83.0=1.83.0-2.1ubuntu3.2 \
    libboost-iostreams1.83.0=1.83.0-2.1ubuntu3.2 \
    libboost-thread1.83.0=1.83.0-2.1ubuntu3.2 \
    curl \
    procps
fi

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

for attempt in $(seq 1 180); do
  if curl -sf "${PRIMARY_HEALTH_URL}" >/dev/null || curl -sf "${FALLBACK_HEALTH_URL}" >/dev/null; then
    echo "OSRM init: service is healthy on port ${OSRM_PORT}"
    persist_debug_artifacts
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
