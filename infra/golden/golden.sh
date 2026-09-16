#!/usr/bin/env bash
# Golden snapshot and restore (SPEC.md §9.3).
#
# The docker half of the operation; the decisions (what must be captured, how stale a snapshot
# may be, what order a restore runs in) live in platform/opsctl/golden.py, where they are
# tested. This script must not make those decisions independently.
#
#   ./infra/golden/golden.sh snapshot     take a cold snapshot of every volume
#   ./infra/golden/golden.sh restore      restore, then catch up the gap
#
set -euo pipefail

COMPOSE="docker compose -f infra/compose/docker-compose.yml"
GOLDEN_DIR="${GOLDEN_DIR:-./.golden}"
VOLUMES=(warehouse-data app-data redpanda-data airflow-data)
PROJECT="analytics-infra"

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

# Volumes are archived through a throwaway container rather than from the host, so this works
# the same on a Mac (where docker volumes live inside a VM) as on the Linux VM.
archive_volume() {
  local volume="$1" out="$2"
  docker run --rm \
    -v "${PROJECT}_${volume}:/data:ro" \
    -v "$(cd "$(dirname "$out")" && pwd):/backup" \
    alpine:3.20 sh -c "tar -C /data -cf - . | zstd -3 -T0 -o /backup/$(basename "$out")"
}

restore_volume() {
  local volume="$1" archive="$2"
  docker run --rm \
    -v "${PROJECT}_${volume}:/data" \
    -v "$(cd "$(dirname "$archive")" && pwd):/backup:ro" \
    alpine:3.20 sh -c "rm -rf /data/* /data/..?* .[!.]* 2>/dev/null; \
                       zstd -dc /backup/$(basename "$archive") | tar -C /data -xf -"
}

cmd_snapshot() {
  mkdir -p "$GOLDEN_DIR"
  # COLD, and all together. A snapshot taken while Postgres is running would restore into
  # recovery; one taken without Redpanda would restore the databases to a position the
  # connector's stored offsets no longer match.
  log "stopping the stack"
  $COMPOSE stop

  local golden_at
  golden_at="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"

  for volume in "${VOLUMES[@]}"; do
    log "archiving $volume"
    archive_volume "$volume" "$GOLDEN_DIR/$volume.tar.zst"
  done

  uv run python -m opsctl.cli write-manifest \
    --golden-at "$golden_at" \
    --dir "$GOLDEN_DIR" \
    --stack-version "$(git rev-parse --short HEAD)"

  log "starting the stack"
  $COMPOSE start
  log "golden snapshot written to $GOLDEN_DIR (golden_at=$golden_at)"
}

cmd_restore() {
  local manifest="$GOLDEN_DIR/manifest.json"
  [[ -f "$manifest" ]] || { log "no manifest at $manifest"; exit 1; }

  # Fails loudly if the snapshot is too old to catch up from, or missing a volume.
  uv run python -m opsctl.cli plan-restore --dir "$GOLDEN_DIR"

  log "stopping the stack"
  $COMPOSE down

  for volume in "${VOLUMES[@]}"; do
    log "restoring $volume"
    restore_volume "$volume" "$GOLDEN_DIR/$volume.tar.zst"
  done

  log "starting databases and broker"
  $COMPOSE up -d postgres-app postgres-warehouse redpanda
  $COMPOSE up -d connect

  # snapshot.mode=never: the restored volumes already contain the snapshot, and re-running it
  # would duplicate the entire source database on top of itself.
  log "registering connector at the restored slot position"
  # The file is the bare config object, so the override is a merge and the result can go
  # straight to the config endpoint. --fail is not optional: curl exits 0 on an HTTP 500, and a
  # restore that silently failed to register the connector looks identical to one that worked.
  jq '. + {"snapshot.mode": "never"}' infra/connect/subscriptions-source.json \
    | curl --fail -sS -X PUT -H 'Content-Type: application/json' --data @- \
        http://localhost:8083/connectors/app-cdc/config > /dev/null

  log "starting the rest of the stack"
  $COMPOSE up -d

  local golden_at
  golden_at="$(uv run python -m opsctl.cli golden-at --dir "$GOLDEN_DIR")"
  log "catching up from $golden_at"
  uv run simulator-history --catch-up-from "$golden_at"

  log "restore complete; trigger load_backfill and a full dbt build to finish"
}

case "${1:-}" in
  snapshot) cmd_snapshot ;;
  restore)  cmd_restore ;;
  *) echo "usage: $0 {snapshot|restore}" >&2; exit 2 ;;
esac
