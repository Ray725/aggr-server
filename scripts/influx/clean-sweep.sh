#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
readonly COMPOSE_FILE="${AGGR_INFLUX_COMPOSE_FILE:-${REPO_ROOT}/docker/docker-compose.yml}"
readonly COMPOSE_ENV_FILE="${AGGR_INFLUX_ENV_FILE:-${REPO_ROOT}/docker/.env}"
readonly INFLUX_CONTAINER="${AGGR_INFLUX_CONTAINER:-aggr-influx}"
readonly INFLUX_DATABASE="${AGGR_INFLUX_DATABASE:-significant_trades}"
readonly INFLUX_STARTUP_TIMEOUT="${AGGR_INFLUX_STARTUP_TIMEOUT:-300}"

readonly -a RETENTION_PROFILE=(
  'autogen|30d|1d|default'
  'aggr_10s|13h53m20s|1h|'
  'aggr_30s|1d17h40m|1h|'
  'aggr_1m|3d11h20m|1d|'
  'aggr_3m|10d10h|1d|'
  'aggr_5m|17d8h40m|1d|'
  'aggr_15m|30d|1d|'
  'aggr_30m|30d|1d|'
  'aggr_1h|30d|1d|'
  'aggr_2h|30d|1d|'
  'aggr_4h|30d|1d|'
  'aggr_6h|30d|1d|'
  'aggr_1d|30d|1d|'
)

readonly -a EXPECTED_INFLUX_ENV=(
  'INFLUXDB_DATA_INDEX_VERSION=tsi1'
  'INFLUXDB_DATA_CACHE_MAX_MEMORY_SIZE=256m'
  'INFLUXDB_DATA_CACHE_SNAPSHOT_MEMORY_SIZE=32m'
  'INFLUXDB_DATA_CACHE_SNAPSHOT_WRITE_COLD_DURATION=5m'
  'INFLUXDB_DATA_MAX_CONCURRENT_COMPACTIONS=1'
  'INFLUXDB_RETENTION_CHECK_INTERVAL=10m'
)

ASSUME_YES=false

log() {
  printf '[influx-clean-sweep] %s\n' "$*"
}

die() {
  printf '[influx-clean-sweep] ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: scripts/influx/clean-sweep.sh [--yes]

Converges the production InfluxDB container and data to the repository's
bounded-memory profile, with retention-policy durations capped at 30 days.
The command preserves the database volume and only drops wholly expired shards.

Options:
  --yes   Skip the destructive-action confirmation (for automation).
  --help  Show this help.
EOF
}

while (($#)); do
  case "$1" in
    --yes)
      ASSUME_YES=true
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
  shift
done

[[ "${INFLUX_DATABASE}" =~ ^[A-Za-z0-9_]+$ ]] ||
  die "database name may contain only letters, numbers, and underscores"
[[ "${INFLUX_STARTUP_TIMEOUT}" =~ ^[1-9][0-9]*$ ]] ||
  die "AGGR_INFLUX_STARTUP_TIMEOUT must be a positive number of seconds"
[[ -f "${COMPOSE_FILE}" ]] || die "compose file not found: ${COMPOSE_FILE}"
[[ -f "${COMPOSE_ENV_FILE}" ]] || die "compose env file not found: ${COMPOSE_ENV_FILE}"
command -v docker >/dev/null 2>&1 || die "docker is not installed"
command -v node >/dev/null 2>&1 || die "node is required to validate the Compose data mount"

DOCKER=(docker)
if ! "${DOCKER[@]}" info >/dev/null 2>&1; then
  command -v sudo >/dev/null 2>&1 || die "cannot connect to the Docker daemon and sudo is unavailable"
  log "Direct Docker access is unavailable; requesting sudo for Docker commands"
  DOCKER=(sudo docker)
  "${DOCKER[@]}" info >/dev/null || die "cannot connect to the Docker daemon, including through sudo"
fi
readonly -a DOCKER
"${DOCKER[@]}" compose version >/dev/null 2>&1 || die "the Docker Compose plugin is required"

readonly -a COMPOSE=(
  "${DOCKER[@]}" compose
  --project-directory "${REPO_ROOT}/docker"
  --env-file "${COMPOSE_ENV_FILE}"
  -f "${COMPOSE_FILE}"
)

"${COMPOSE[@]}" config --quiet

COMPOSE_PROFILE="$(
  "${COMPOSE[@]}" config --format json | node -e '
    const fs = require("fs")
    const config = JSON.parse(fs.readFileSync(0, "utf8"))
    const influx = config.services?.influx
    if (!influx?.mem_limit || !influx?.memswap_limit) {
      process.exit(1)
    }
    const volumes = config.services?.influx?.volumes || []
    const influxVolume = volumes.find(volume => volume.target === "/var/lib/influxdb")
    if (!influxVolume || influxVolume.type !== "bind" || !influxVolume.source) {
      process.exit(1)
    }
    process.stdout.write([
      influxVolume.source,
      influx.mem_limit,
      influx.memswap_limit
    ].join("\n"))
  '
)" || die "could not resolve the production InfluxDB runtime profile"
mapfile -t COMPOSE_PROFILE_LINES <<<"${COMPOSE_PROFILE}"
[[ "${#COMPOSE_PROFILE_LINES[@]}" == '3' ]] || die "the production InfluxDB runtime profile is incomplete"
EXPECTED_VOLUME_SOURCE="${COMPOSE_PROFILE_LINES[0]}"
EXPECTED_MEMORY_BYTES="${COMPOSE_PROFILE_LINES[1]}"
EXPECTED_MEMORY_SWAP_BYTES="${COMPOSE_PROFILE_LINES[2]}"
[[ -n "${EXPECTED_VOLUME_SOURCE}" ]] || die "the production InfluxDB bind mount resolved to an empty path"
[[ "${EXPECTED_MEMORY_BYTES}" =~ ^[1-9][0-9]*$ ]] || die "the production memory limit is invalid"
[[ "${EXPECTED_MEMORY_SWAP_BYTES}" =~ ^[1-9][0-9]*$ ]] || die "the production memory+swap limit is invalid"
EXPECTED_MEMORY_GIB="$((EXPECTED_MEMORY_BYTES / 1024 / 1024 / 1024))"
readonly EXPECTED_VOLUME_SOURCE EXPECTED_MEMORY_BYTES EXPECTED_MEMORY_SWAP_BYTES EXPECTED_MEMORY_GIB

if command -v flock >/dev/null 2>&1; then
  readonly LOCK_FILE="${TMPDIR:-/tmp}/aggr-influx-clean-sweep.lock"
  exec 9>>"${LOCK_FILE}"
  flock -n 9 || die "another clean sweep is already running"
fi

container_exists() {
  "${DOCKER[@]}" inspect "${INFLUX_CONTAINER}" >/dev/null 2>&1
}

container_running() {
  [[ "$("${DOCKER[@]}" inspect --format '{{.State.Running}}' "${INFLUX_CONTAINER}" 2>/dev/null)" == 'true' ]]
}

influx_ready() {
  "${DOCKER[@]}" exec "${INFLUX_CONTAINER}" influx -execute 'SHOW DATABASES' >/dev/null 2>&1
}

wait_for_influx() {
  local attempt

  for ((attempt = 1; attempt <= INFLUX_STARTUP_TIMEOUT; attempt++)); do
    if influx_ready; then
      return 0
    fi
    sleep 1
  done

  "${COMPOSE[@]}" logs --no-color --tail=100 influx >&2 || true
  die "InfluxDB did not become queryable within ${INFLUX_STARTUP_TIMEOUT} seconds"
}

influx_query() {
  "${DOCKER[@]}" exec "${INFLUX_CONTAINER}" influx -format csv -execute "$1"
}

policy_exists() {
  local policies_csv="$1"
  local policy_name="$2"

  awk -F, -v policy="${policy_name}" \
    'NR > 1 && $1 == policy { found = 1 } END { exit(found ? 0 : 1) }' \
    <<<"${policies_csv}"
}

ensure_policy() {
  local policy_name="$1"
  local duration="$2"
  local shard_duration="$3"
  local default_flag="$4"
  local default_clause=''
  local policies_csv
  local create_output

  if [[ "${default_flag}" == 'default' ]]; then
    default_clause=' DEFAULT'
  fi

  policies_csv="$(influx_query "SHOW RETENTION POLICIES ON \"${INFLUX_DATABASE}\"")"

  if ! policy_exists "${policies_csv}" "${policy_name}"; then
    log "Creating retention policy ${policy_name}"
    if ! create_output="$(influx_query \
      "CREATE RETENTION POLICY \"${policy_name}\" ON \"${INFLUX_DATABASE}\" DURATION ${duration} REPLICATION 1 SHARD DURATION ${shard_duration}${default_clause}" 2>&1)"; then
      policies_csv="$(influx_query "SHOW RETENTION POLICIES ON \"${INFLUX_DATABASE}\"")"
      if ! policy_exists "${policies_csv}" "${policy_name}"; then
        printf '%s\n' "${create_output}" >&2
        return 1
      fi
    fi
  fi

  influx_query \
    "ALTER RETENTION POLICY \"${policy_name}\" ON \"${INFLUX_DATABASE}\" DURATION ${duration} REPLICATION 1 SHARD DURATION ${shard_duration}${default_clause}" \
    >/dev/null
}

reconcile_retention() {
  local entry
  local policy_name
  local duration
  local shard_duration
  local default_flag

  log "Reconciling retention policies in ${INFLUX_DATABASE}"
  influx_query "CREATE DATABASE \"${INFLUX_DATABASE}\"" >/dev/null

  for entry in "${RETENTION_PROFILE[@]}"; do
    IFS='|' read -r policy_name duration shard_duration default_flag <<<"${entry}"
    ensure_policy "${policy_name}" "${duration}" "${shard_duration}" "${default_flag}"
  done
}

duration_to_seconds() {
  local remaining="$1"
  local total=0
  local value
  local unit

  while [[ -n "${remaining}" ]]; do
    if [[ ! "${remaining}" =~ ^([0-9]+)([smhdw])(.*)$ ]]; then
      return 1
    fi

    value="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    remaining="${BASH_REMATCH[3]}"

    case "${unit}" in
      s) ((total += value)) ;;
      m) ((total += value * 60)) ;;
      h) ((total += value * 60 * 60)) ;;
      d) ((total += value * 60 * 60 * 24)) ;;
      w) ((total += value * 60 * 60 * 24 * 7)) ;;
    esac
  done

  printf '%s\n' "${total}"
}

verify_retention() {
  local policies_csv
  local entry
  local policy_name
  local expected_duration
  local expected_shard_duration
  local default_flag
  local policy_line
  local actual_name
  local actual_duration
  local actual_shard_duration
  local actual_replication
  local actual_default

  policies_csv="$(influx_query "SHOW RETENTION POLICIES ON \"${INFLUX_DATABASE}\"")"

  for entry in "${RETENTION_PROFILE[@]}"; do
    IFS='|' read -r policy_name expected_duration expected_shard_duration default_flag <<<"${entry}"
    policy_line="$(awk -F, -v policy="${policy_name}" '$1 == policy { print; exit }' <<<"${policies_csv}")"
    [[ -n "${policy_line}" ]] || die "retention policy is missing after reconciliation: ${policy_name}"

    IFS=, read -r actual_name actual_duration actual_shard_duration actual_replication actual_default <<<"${policy_line}"
    [[ "$(duration_to_seconds "${actual_duration}")" == "$(duration_to_seconds "${expected_duration}")" ]] ||
      die "unexpected duration for ${policy_name}: ${actual_duration}"
    [[ "$(duration_to_seconds "${actual_shard_duration}")" == "$(duration_to_seconds "${expected_shard_duration}")" ]] ||
      die "unexpected shard duration for ${policy_name}: ${actual_shard_duration}"
    [[ "${actual_replication}" == '1' ]] || die "unexpected replication for ${policy_name}: ${actual_replication}"

    if [[ "${default_flag}" == 'default' ]]; then
      [[ "${actual_default}" == 'true' ]] || die "${policy_name} is not the default retention policy"
    else
      [[ "${actual_default}" == 'false' ]] || die "${policy_name} unexpectedly became the default retention policy"
    fi
  done
}

managed_policy() {
  case "$1" in
    autogen|aggr_10s|aggr_30s|aggr_1m|aggr_3m|aggr_5m|aggr_15m|aggr_30m|aggr_1h|aggr_2h|aggr_4h|aggr_6h|aggr_1d)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

purge_expired_shards() {
  local shards_csv
  local now
  local row_name
  local shard_id
  local database
  local retention_policy
  local shard_group
  local start_time
  local end_time
  local expiry_time
  local owners
  local purged=0

  shards_csv="$(influx_query 'SHOW SHARDS')"
  now="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  while IFS=, read -r row_name shard_id database retention_policy shard_group start_time end_time expiry_time owners; do
    [[ "${shard_id}" == 'id' ]] && continue
    [[ "${database}" == "${INFLUX_DATABASE}" ]] || continue
    managed_policy "${retention_policy}" || continue
    [[ -n "${expiry_time}" ]] || continue

    if [[ "${expiry_time}" < "${now}" || "${expiry_time}" == "${now}" ]]; then
      [[ "${shard_id}" =~ ^[0-9]+$ ]] || die "invalid shard id returned by InfluxDB: ${shard_id}"
      log "Dropping expired shard ${shard_id} (${retention_policy}, ended ${end_time})"
      influx_query "DROP SHARD ${shard_id}" >/dev/null
      ((purged += 1))
    fi
  done <<<"${shards_csv}"

  log "Expired shards dropped: ${purged}"
}

verify_no_expired_shards() {
  local shards_csv
  local now
  local row_name
  local shard_id
  local database
  local retention_policy
  local shard_group
  local start_time
  local end_time
  local expiry_time
  local owners

  shards_csv="$(influx_query 'SHOW SHARDS')"
  now="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  while IFS=, read -r row_name shard_id database retention_policy shard_group start_time end_time expiry_time owners; do
    [[ "${shard_id}" == 'id' ]] && continue
    [[ "${database}" == "${INFLUX_DATABASE}" ]] || continue
    managed_policy "${retention_policy}" || continue
    [[ -n "${expiry_time}" ]] || continue

    if [[ "${expiry_time}" < "${now}" || "${expiry_time}" == "${now}" ]]; then
      die "expired managed shard remains after cleanup: ${shard_id} (${retention_policy})"
    fi
  done <<<"${shards_csv}"
}

container_volume_source() {
  "${DOCKER[@]}" inspect --format \
    '{{range .Mounts}}{{if eq .Destination "/var/lib/influxdb"}}{{.Source}}{{end}}{{end}}' \
    "${INFLUX_CONTAINER}"
}

print_volume_source() {
  local volume_source

  volume_source="$(container_volume_source)"

  [[ -n "${volume_source}" ]] || die "the container has no /var/lib/influxdb mount; refusing to continue"
  [[ "${volume_source}" == "${EXPECTED_VOLUME_SOURCE}" ]] ||
    die "InfluxDB volume mismatch: container uses ${volume_source}, production Compose uses ${EXPECTED_VOLUME_SOURCE}"
  log "Preserving InfluxDB volume at ${volume_source}"
}

runtime_matches_profile() {
  local runtime_limits
  local container_env
  local expected_env

  runtime_limits="$("${DOCKER[@]}" inspect --format \
    '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' \
    "${INFLUX_CONTAINER}" 2>/dev/null)" || return 1
  [[ "${runtime_limits}" == "${EXPECTED_MEMORY_BYTES} ${EXPECTED_MEMORY_SWAP_BYTES}" ]] || return 1

  container_env="$("${DOCKER[@]}" inspect --format '{{range .Config.Env}}{{println .}}{{end}}' \
    "${INFLUX_CONTAINER}")"
  for expected_env in "${EXPECTED_INFLUX_ENV[@]}"; do
    grep -Fqx "${expected_env}" <<<"${container_env}" || return 1
  done
}

needs_recreate=true
if container_exists; then
  print_volume_source
  if runtime_matches_profile; then
    needs_recreate=false
  fi
fi

if ! ${ASSUME_YES}; then
  if [[ ! -t 0 ]]; then
    die "confirmation requires a terminal; rerun with --yes for automation"
  fi

  printf '%s\n' \
    'This permanently drops InfluxDB shards outside the configured windows.' \
    "Container: ${INFLUX_CONTAINER}" \
    "Database:  ${INFLUX_DATABASE}" \
    "Data:      ${EXPECTED_VOLUME_SOURCE}" \
    "RAM limit: ${EXPECTED_MEMORY_GIB} GiB (swap disabled)" \
    'The database volume is preserved, and no retained shard is deleted.' \
    'No backup is created automatically.'
  read -r -p 'Continue? [y/N] ' answer
  [[ "${answer}" == 'y' || "${answer}" == 'Y' ]] || die "cancelled"
fi

if container_running && influx_ready; then
  log "Applying retention to the existing InfluxDB before container reconciliation"
  print_volume_source
  reconcile_retention
  purge_expired_shards
else
  log "No queryable existing InfluxDB container; retention will be applied after startup"
fi

log "Reconciling the production InfluxDB container"
if ${needs_recreate}; then
  "${COMPOSE[@]}" up -d --no-deps --force-recreate influx
else
  "${COMPOSE[@]}" up -d --no-deps influx
fi

wait_for_influx
runtime_matches_profile || die "the running container does not have the rendered RAM/no-swap profile"
print_volume_source

reconcile_retention
purge_expired_shards
verify_retention
verify_no_expired_shards

log "Final retention policies:"
influx_query "SHOW RETENTION POLICIES ON \"${INFLUX_DATABASE}\""
log "Complete: InfluxDB is running with a ${EXPECTED_MEMORY_GIB} GiB hard limit, no swap, and trade retention policies capped at 30 days"
