#!/bin/bash
set -euo pipefail

BOOTSTRAP_ASSET_ROOT="${LOCAL_ASSET_ROOT:-/local_disk0/routing-geocoding}"
mkdir -p "${BOOTSTRAP_ASSET_ROOT}/logs"
DEBUG_LOG="${BOOTSTRAP_ASSET_ROOT}/logs/photon-init-$(hostname).log"
exec > >(tee -a "${DEBUG_LOG}") 2>&1
set -x

persist_debug_artifacts() {
  mkdir -p "${VOLUME_DEBUG_DIR}" || true
  cp "${DEBUG_LOG}" "${VOLUME_DEBUG_DIR}/photon-init-${DEBUG_TIMESTAMP}-$(hostname).log" || true
  if [[ -n "${LOCAL_ASSET_ROOT:-}" && -f "${LOCAL_ASSET_ROOT}/logs/photon.log" ]]; then
    cp "${LOCAL_ASSET_ROOT}/logs/photon.log" "${VOLUME_DEBUG_DIR}/photon-runtime-${DEBUG_TIMESTAMP}-$(hostname).log" || true
  fi
}

trap 'rc=$?; line=${BASH_LINENO[0]}; echo "Photon init: failed at line ${line} with exit code ${rc}"; if [[ -n "${LOCAL_ASSET_ROOT:-}" && -f "${LOCAL_ASSET_ROOT}/logs/photon.log" ]]; then tail -200 "${LOCAL_ASSET_ROOT}/logs/photon.log"; fi; persist_debug_artifacts; exit "${rc}"' ERR

ENV_FILE="${ROUTING_GEOCODING_ENV_FILE:-__ROUTING_ENV_FILE__}"
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Photon init: missing env file at ${ENV_FILE}" >&2
  exit 1
fi

source "${ENV_FILE}"

DEBUG_TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
VOLUME_DEBUG_DIR="${VOLUME_BASE}/debug"

apt_get_retry() {
  local attempt
  for attempt in $(seq 1 6); do
    if sudo DEBIAN_FRONTEND=noninteractive apt-get "$@"; then
      return 0
    fi
    if [[ "${attempt}" -lt 6 ]]; then
      echo "Photon init: apt-get $* failed on attempt ${attempt}, retrying in 10s" >&2
      sleep 10
    fi
  done
  echo "Photon init: apt-get $* failed after retries" >&2
  return 1
}

if ! command -v java >/dev/null || ! command -v bzip2 >/dev/null || ! command -v curl >/dev/null || ! command -v pkill >/dev/null; then
  apt_get_retry update -qq
  apt_get_retry install -y --no-install-recommends \
    openjdk-21-jre-headless \
    bzip2 \
    curl \
    procps
fi

mkdir -p "${LOCAL_PHOTON_ROOT}" "${LOCAL_ASSET_ROOT}/logs"
ls -lh "${PHOTON_TARGET_DIR}"
ls -lh "${PHOTON_DB_ARCHIVE_PATH}" || true
ls -lh "${PHOTON_DB_FAST_ARCHIVE_PATH}" || true

cp "${PHOTON_TARGET_DIR}/photon-${PHOTON_VERSION}.jar" "${LOCAL_PHOTON_ROOT}/"
rm -rf "${LOCAL_PHOTON_ROOT}/photon_data"
if [[ -f "${PHOTON_DB_FAST_ARCHIVE_PATH}" ]]; then
  test -s "${PHOTON_DB_FAST_ARCHIVE_PATH}"
  tar -xf "${PHOTON_DB_FAST_ARCHIVE_PATH}" -C "${LOCAL_PHOTON_ROOT}"
else
  test -s "${PHOTON_DB_ARCHIVE_PATH}"
  tar -xjf "${PHOTON_DB_ARCHIVE_PATH}" -C "${LOCAL_PHOTON_ROOT}"
fi
test -d "${LOCAL_PHOTON_ROOT}/photon_data"

pkill -f "photon-${PHOTON_VERSION}.jar" || true

cd "${LOCAL_PHOTON_ROOT}"
java -version
nohup java -Xmx"${PHOTON_HEAP}" -jar "photon-${PHOTON_VERSION}.jar" \
  -data-dir "${LOCAL_PHOTON_ROOT}" \
  -listen-port "${PHOTON_PORT}" \
  > "${LOCAL_ASSET_ROOT}/logs/photon.log" 2>&1 &

for attempt in $(seq 1 180); do
  payload="$(curl -sf "http://127.0.0.1:${PHOTON_PORT}/api?q=${PHOTON_HEALTH_QUERY}&limit=1")" || {
    if [[ -f "${LOCAL_ASSET_ROOT}/logs/photon.log" ]]; then
      tail -20 "${LOCAL_ASSET_ROOT}/logs/photon.log" || true
    fi
    sleep 2
    continue
  }
  if printf '%s' "${payload}" | python3 -c 'import json,sys; data=json.load(sys.stdin); raise SystemExit(0 if data.get("features") else 1)'; then
    echo "Photon init: service is healthy on port ${PHOTON_PORT}"
    persist_debug_artifacts
    exit 0
  fi
  if [[ -f "${LOCAL_ASSET_ROOT}/logs/photon.log" ]]; then
    tail -20 "${LOCAL_ASSET_ROOT}/logs/photon.log" || true
  fi
  sleep 2
done

echo "Photon init: service failed to become healthy" >&2
if [[ -f "${LOCAL_ASSET_ROOT}/logs/photon.log" ]]; then
  tail -200 "${LOCAL_ASSET_ROOT}/logs/photon.log" || true
fi
exit 1
