#!/usr/bin/env bash
# Bring the stack up on the VM. Idempotent: safe to re-run after a git pull or reboot.
# cloud-init runs it once at first boot; afterwards: sudo /opt/deploy/bootstrap.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/log-pipeline}"
DEPLOY_DIR="${DEPLOY_DIR:-/opt/deploy}"
PRODUCER_RATE="${PRODUCER_RATE:-20}"
INDEXER_WORKERS="${INDEXER_WORKERS:-1}"
REDPANDA_RETENTION_HOURS="${REDPANDA_RETENTION_HOURS:-24}"
DISK_GUARD_PCT="${DISK_GUARD_PCT:-85}"

cd "$APP_DIR"
compose=(docker compose --project-directory "$APP_DIR"
         -f "$APP_DIR/docker-compose.yml" -f "$DEPLOY_DIR/docker-compose.oci.yml")

# 1. Secrets: generated on the VM, never in git or Terraform state.
if [[ ! -f .env ]]; then
  pg_pw="$(openssl rand -hex 16)"
  gf_pw="$(openssl rand -hex 12)"
  sed -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${pg_pw}|" \
      -e "s|^PG_DSN=.*|PG_DSN=postgres://logs:${pg_pw}@postgres:5432/logs|" \
      -e "s|^PRODUCER_RATE=.*|PRODUCER_RATE=${PRODUCER_RATE}|" \
      -e "s|^WORKERS=.*|WORKERS=${INDEXER_WORKERS}|" \
      .env.example > .env
  echo "GRAFANA_ADMIN_PASSWORD=${gf_pw}" >> .env
  chmod 600 .env
  echo "generated .env (Grafana admin password: sudo grep GRAFANA $APP_DIR/.env)"
fi

# 2. Build the app image natively on arm64 and start everything.
"${compose[@]}" up -d --build

# 3. Redpanda keeps 7 days by default; Postgres is the system of record here, so 1 day of
#    topic history is enough for replay and keeps the disk budget predictable.
for _ in $(seq 1 60); do
  docker exec redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true' && break
  sleep 5
done
docker exec redpanda rpk cluster config set log_retention_ms "$((REDPANDA_RETENTION_HOURS * 3600 * 1000))"

# 4. Disk guard: Postgres keeps 7 daily partitions (drop_old_log_partitions), but if the
#    boot volume still fills up, stop the producer before Postgres or Redpanda run out.
cat > /etc/cron.d/log-pipeline-disk-guard <<EOF
*/10 * * * * root [ "\$(df --output=pcent / | tail -1 | tr -dc 0-9)" -ge ${DISK_GUARD_PCT} ] && cd ${APP_DIR} && ${compose[*]} stop producer && logger -t log-pipeline "disk >= ${DISK_GUARD_PCT}%: producer stopped"
EOF
chmod 644 /etc/cron.d/log-pipeline-disk-guard

"${compose[@]}" ps
