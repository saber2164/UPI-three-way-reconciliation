#!/usr/bin/env bash
# Submit the rendered reconciliation SQL through the Flink SQL Gateway.
#
# The gateway is required: sql-client.sh in embedded mode would start its own
# MiniCluster inside this container instead of talking to the JobManager.
set -euo pipefail

# Never let a single probe block the one-shot submit container forever.
curl() { command curl --connect-timeout 5 --max-time 30 "$@"; }

SQL_DIR="${SQL_DIR:-/opt/flink/sql}"
GATEWAY="${SQL_GATEWAY_ENDPOINT:-flink-sql-gateway:8083}"
JM="${FLINK_REST_URL:-http://flink-jobmanager:8081}"
ICEBERG="${ICEBERG_REST_URI:-http://iceberg-rest:8181}"
OUT="${OUT:-/tmp/upi-recon-apply.sql}"

echo "waiting for Iceberg REST at ${ICEBERG} ..."
for _ in $(seq 1 120); do
  if curl -sf "${ICEBERG}/v1/config" >/dev/null; then
    break
  fi
  sleep 2
done
curl -sf "${ICEBERG}/v1/config" >/dev/null || {
  echo "Iceberg REST did not become ready at ${ICEBERG}" >&2
  exit 1
}

echo "waiting for TaskManager registration ..."
for _ in $(seq 1 120); do
  overview="$(curl -s "${JM}/overview" || true)"
  taskmanagers="$(printf '%s' "${overview}" | { grep -o '"taskmanagers":[0-9]*' || true; } | cut -d: -f2)"
  if [ "${taskmanagers:-0}" -ge 1 ]; then
    break
  fi
  sleep 2
done
overview="$(curl -s "${JM}/overview" || true)"
taskmanagers="$(printf '%s' "${overview}" | { grep -o '"taskmanagers":[0-9]*' || true; } | cut -d: -f2)"
if [ "${taskmanagers:-0}" -lt 1 ]; then
  echo "no TaskManager registered with ${JM}" >&2
  exit 1
fi

echo "waiting for SQL Gateway at ${GATEWAY} ..."
for _ in $(seq 1 120); do
  if curl -sf "http://${GATEWAY}/v1/info" >/dev/null; then
    break
  fi
  sleep 2
done
curl -sf "http://${GATEWAY}/v1/info" >/dev/null || {
  echo "SQL Gateway did not become ready" >&2
  exit 1
}

jobs_json="$(curl -s "${JM}/jobs/overview" || true)"
running="$(printf '%s' "${jobs_json}" | { grep -o '"state":"RUNNING"' || true; } | wc -l | tr -d ' ')"

if [ "${running}" -ge 2 ] && [ "${FORCE_RESUBMIT:-0}" != "1" ]; then
  echo "two reconciliation jobs are already RUNNING; nothing to do"
  echo "set FORCE_RESUBMIT=1 to cancel and submit again"
  exit 0
fi

if [ "${running}" -gt 0 ]; then
  echo "found ${running} running job(s); cancelling before a clean resubmit"
  for jid in $(printf '%s' "${jobs_json}" | { grep -o '"jid":"[^"]*"' || true; } | cut -d'"' -f4); do
    curl -s -X PATCH "${JM}/jobs/${jid}?mode=cancel" >/dev/null || true
  done
  sleep 3
fi

: > "${OUT}"
# The rendered file carries the JDBC credentials; remove it when we are done.
trap 'rm -f "${OUT}"' EXIT
for f in 00_catalogs 10_sources 20_recon 30_late_correction; do
  echo "-- ===== ${f}.sql =====" >> "${OUT}"
  bash "${SQL_DIR}/render_sql.sh" "${SQL_DIR}/${f}.sql" >> "${OUT}"
  echo >> "${OUT}"
done

echo "submitting ${OUT} (TIME_SCALE=${TIME_SCALE:-60}) ..."
# A failed statement can leave the client waiting on the gateway forever.
timeout --signal=TERM 900 /opt/flink/bin/sql-client.sh gateway --endpoint "${GATEWAY}" -f "${OUT}"

echo "waiting for both jobs to reach RUNNING ..."
for _ in $(seq 1 90); do
  running="$(curl -s "${JM}/jobs/overview" | { grep -o '"state":"RUNNING"' || true; } | wc -l | tr -d ' ')"
  if [ "${running}" -ge 2 ]; then
    echo "submitted: ${running} running jobs"
    exit 0
  fi
  sleep 2
done

echo "jobs did not reach RUNNING; current overview:" >&2
curl -s "${JM}/jobs/overview" >&2 || true
exit 1
