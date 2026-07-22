const assert = require('node:assert/strict')
const {
  chmodSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync
} = require('node:fs')
const { tmpdir } = require('node:os')
const { join, resolve } = require('node:path')
const { spawnSync } = require('node:child_process')
const test = require('node:test')

const repoRoot = resolve(__dirname, '..')
const sweepScript = join(repoRoot, 'scripts/influx/clean-sweep.sh')

test('production Compose renders the bounded InfluxDB profile', t => {
  const composeVersion = spawnSync('docker', ['compose', 'version'], {
    encoding: 'utf8'
  })
  if (composeVersion.status !== 0) {
    t.skip('Docker Compose plugin is not installed')
    return
  }

  const dockerDir = join(repoRoot, 'docker')
  const rendered = spawnSync(
    'docker',
    [
      'compose',
      '--project-directory',
      dockerDir,
      '--env-file',
      join(dockerDir, '.env'),
      '-f',
      join(dockerDir, 'docker-compose.yml'),
      'config',
      '--format',
      'json'
    ],
    { encoding: 'utf8' }
  )
  assert.equal(rendered.status, 0, rendered.stderr)

  const influx = JSON.parse(rendered.stdout).services.influx
  assert.equal(influx.mem_limit, '8589934592')
  assert.equal(influx.mem_reservation, '4294967296')
  assert.equal(influx.memswap_limit, '8589934592')
  assert.equal(influx.deploy.resources.limits.memory, '8589934592')
  assert.equal(influx.deploy.resources.reservations.memory, '4294967296')
  assert.deepEqual(influx.environment, {
    INFLUXDB_DATA_CACHE_MAX_MEMORY_SIZE: '256m',
    INFLUXDB_DATA_CACHE_SNAPSHOT_MEMORY_SIZE: '32m',
    INFLUXDB_DATA_CACHE_SNAPSHOT_WRITE_COLD_DURATION: '5m',
    INFLUXDB_DATA_INDEX_VERSION: 'tsi1',
    INFLUXDB_DATA_MAX_CONCURRENT_COMPACTIONS: '1',
    INFLUXDB_RETENTION_CHECK_INTERVAL: '10m'
  })
})

test('clean sweep converges once and is safe to repeat', t => {
  const fixtureDir = mkdtempSync(join(tmpdir(), 'aggr-influx-sweep-'))
  t.after(() => rmSync(fixtureDir, { recursive: true, force: true }))
  const fakeDocker = join(fixtureDir, 'docker')
  const commandLog = join(fixtureDir, 'docker.log')
  const containerState = join(fixtureDir, 'container-running')
  const runtimeProfileState = join(fixtureDir, 'runtime-profile-applied')
  const droppedState = join(fixtureDir, 'expired-shard-dropped')
  const policyState = join(fixtureDir, 'daily-policy-created')

  writeFileSync(
    fakeDocker,
    String.raw`#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"__DOLLAR__{FAKE_DOCKER_LOG}"

if [[ "$1" == 'compose' && "$2" == 'version' ]]; then
  exit 0
fi

if [[ "$1" == 'info' ]]; then
  exit 0
fi

if [[ "$1" == 'compose' ]]; then
  case " $* " in
    *' config --format json '*)
      printf '%s\n' '{"services":{"influx":{"mem_limit":"8589934592","memswap_limit":"8589934592","volumes":[{"type":"bind","source":"/srv/aggr/influxdb","target":"/var/lib/influxdb"}]}}}'
      exit 0
      ;;
    *' config --quiet '*)
      exit 0
      ;;
    *' up -d '*)
      : >"__DOLLAR__{FAKE_CONTAINER_STATE}"
      if [[ " $* " == *' --force-recreate '* ]]; then
        : >"__DOLLAR__{FAKE_RUNTIME_PROFILE_STATE}"
      fi
      exit 0
      ;;
    *' logs '*)
      exit 0
      ;;
  esac
fi

if [[ "$1" == 'inspect' ]]; then
  [[ -f "__DOLLAR__{FAKE_CONTAINER_STATE}" ]] || exit 1

  if [[ "$2" != '--format' ]]; then
    exit 0
  fi

  case "$3" in
    *'.State.Running'*)
      printf 'true\n'
      ;;
    *'.HostConfig.Memory'*)
      if [[ -f "__DOLLAR__{FAKE_RUNTIME_PROFILE_STATE}" ]]; then
        printf '8589934592 8589934592\n'
      else
        printf '8589934592 17179869184\n'
      fi
      ;;
    *'.Config.Env'*)
      if [[ ! -f "__DOLLAR__{FAKE_RUNTIME_PROFILE_STATE}" ]]; then
        printf 'INFLUXDB_DATA_INDEX_VERSION=inmem\n'
        exit 0
      fi
      cat <<'EOF'
INFLUXDB_DATA_INDEX_VERSION=tsi1
INFLUXDB_DATA_CACHE_MAX_MEMORY_SIZE=256m
INFLUXDB_DATA_CACHE_SNAPSHOT_MEMORY_SIZE=32m
INFLUXDB_DATA_CACHE_SNAPSHOT_WRITE_COLD_DURATION=5m
INFLUXDB_DATA_MAX_CONCURRENT_COMPACTIONS=1
INFLUXDB_RETENTION_CHECK_INTERVAL=10m
EOF
      ;;
    *'.Mounts'*)
      printf '/srv/aggr/influxdb\n'
      ;;
  esac
  exit 0
fi

if [[ "$1" == 'exec' ]]; then
  query="__DOLLAR__{!#}"
  case "__DOLLAR__{query}" in
    'SHOW DATABASES')
      printf 'name\n_internal\nsignificant_trades\n'
      ;;
    'SHOW RETENTION POLICIES ON "significant_trades"')
      cat <<'EOF'
name,duration,shardGroupDuration,replicaN,default
autogen,720h0m0s,24h0m0s,1,true
aggr_10s,13h53m20s,1h0m0s,1,false
aggr_30s,41h40m0s,1h0m0s,1,false
aggr_1m,83h20m0s,24h0m0s,1,false
aggr_3m,250h0m0s,24h0m0s,1,false
aggr_5m,416h40m0s,24h0m0s,1,false
aggr_15m,720h0m0s,24h0m0s,1,false
aggr_30m,720h0m0s,24h0m0s,1,false
aggr_1h,720h0m0s,24h0m0s,1,false
aggr_2h,720h0m0s,24h0m0s,1,false
aggr_4h,720h0m0s,24h0m0s,1,false
aggr_6h,720h0m0s,24h0m0s,1,false
EOF
      if [[ -f "__DOLLAR__{FAKE_POLICY_STATE}" ]]; then
        printf '%s\n' 'aggr_1d,720h0m0s,24h0m0s,1,false'
      fi
      ;;
    'CREATE DATABASE "significant_trades"')
      ;;
    'CREATE RETENTION POLICY "aggr_1d" ON "significant_trades" DURATION 30d REPLICATION 1 SHARD DURATION 1d')
      : >"__DOLLAR__{FAKE_POLICY_STATE}"
      ;;
    'ALTER RETENTION POLICY '*)
      ;;
    'SHOW SHARDS')
      printf '%s\n' 'name,id,database,retention_policy,shard_group,start_time,end_time,expiry_time,owners'
      if [[ ! -f "__DOLLAR__{FAKE_DROPPED_STATE}" ]]; then
        printf '%s\n' 'significant_trades,41,significant_trades,aggr_1d,41,2019-01-01T00:00:00Z,2019-01-02T00:00:00Z,2020-01-01T00:00:00Z,'
      fi
      printf '%s\n' 'significant_trades,42,significant_trades,aggr_1d,42,2998-01-01T00:00:00Z,2998-01-02T00:00:00Z,2999-01-01T00:00:00Z,'
      ;;
    'DROP SHARD 41')
      : >"__DOLLAR__{FAKE_DROPPED_STATE}"
      ;;
    *)
      printf 'unexpected influx query: %s\n' "__DOLLAR__{query}" >&2
      exit 1
      ;;
  esac
  exit 0
fi

exit 1
`.replaceAll('__DOLLAR__', '$'),
  )
  chmodSync(fakeDocker, 0o755)
  writeFileSync(containerState, '')

  const environment = {
    ...process.env,
    PATH: `${fixtureDir}:${process.env.PATH}`,
    TMPDIR: fixtureDir,
    FAKE_DOCKER_LOG: commandLog,
    FAKE_CONTAINER_STATE: containerState,
    FAKE_RUNTIME_PROFILE_STATE: runtimeProfileState,
    FAKE_DROPPED_STATE: droppedState,
    FAKE_POLICY_STATE: policyState,
  }

  const firstRun = spawnSync(sweepScript, ['--yes'], {
    cwd: repoRoot,
    encoding: 'utf8',
    env: environment,
  })
  assert.equal(firstRun.status, 0, firstRun.stderr || firstRun.stdout)

  const firstLog = readFileSync(commandLog, 'utf8')
  assert.match(firstLog, /up -d --no-deps --force-recreate influx/)
  assert.equal((firstLog.match(/ALTER RETENTION POLICY/g) || []).length, 26)
  assert.equal(
    (firstLog.match(/CREATE RETENTION POLICY "aggr_1d"/g) || []).length,
    1
  )
  assert.match(
    firstLog,
    /ALTER RETENTION POLICY "autogen" .* DURATION 30d .* SHARD DURATION 1d DEFAULT/
  )
  assert.match(
    firstLog,
    /ALTER RETENTION POLICY "aggr_10s" .* DURATION 13h53m20s .* SHARD DURATION 1h/
  )
  assert.match(
    firstLog,
    /ALTER RETENTION POLICY "aggr_1d" .* DURATION 30d .* SHARD DURATION 1d/
  )
  assert.match(firstLog, /DROP SHARD 41/)
  assert.doesNotMatch(firstLog, /DROP SHARD 42/)
  assert.ok(
    firstLog.indexOf('DROP SHARD 41') <
      firstLog.indexOf('up -d --no-deps --force-recreate influx'),
    'expired data should be purged before the stale container is recreated'
  )

  writeFileSync(commandLog, '')
  const secondRun = spawnSync(sweepScript, ['--yes'], {
    cwd: repoRoot,
    encoding: 'utf8',
    env: environment,
  })
  assert.equal(secondRun.status, 0, secondRun.stderr || secondRun.stdout)

  const secondLog = readFileSync(commandLog, 'utf8')
  assert.match(secondLog, /up -d --no-deps influx/)
  assert.doesNotMatch(secondLog, /--force-recreate/)
  assert.doesNotMatch(secondLog, /DROP SHARD 41/)
  assert.equal((secondLog.match(/ALTER RETENTION POLICY/g) || []).length, 26)
})
