#!/usr/bin/env bash
# Render demo-time tokens in the Flink SQL.  The SQL files stay readable in
# production units (4 HOUR, 2 MINUTE); this script compresses them for the demo.
#
#   TIME_SCALE=60  -> 4 h settlement bound becomes 240 s, 24 h late bound 1,440 s
#   TIME_SCALE=1   -> production timing (4 h / 24 h)
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <sql-file>" >&2
  exit 2
fi

TS="${TIME_SCALE:-60}"
# awk handles non-integer scales without bash arithmetic errors.
read -r SETTLE_SEC LATE_SEC CB_SEC WM_SWITCH WM_SETTLE WM_CB IDLE KPI_MIN ALERT_MIN <<EOF
$(awk -v ts="$TS" 'BEGIN {
  if (ts < 1) ts = 1;
  settle = int(4*3600/ts); if (settle < 30) settle = 30;
  late = int(24*3600/ts); if (late < settle*2) late = settle*2;
  cb = int(120/ts); if (cb < 2) cb = 2;
  wm_switch = int(30/ts); if (wm_switch < 1) wm_switch = 1;
  wm_settle = int(300/ts); if (wm_settle < 2) wm_settle = 2;
  wm_cb = int(30/ts); if (wm_cb < 1) wm_cb = 1;
  idle = int(30/ts); if (idle < 1) idle = 1;
  printf "%d %d %d %d %d %d %d %d %d\n", settle, late, cb, wm_switch, wm_settle, wm_cb, idle, 1, 2;
}')
EOF

# Flink's DAY_INTERVAL_TYPES only accept a precision of 0..3, so a plain
# `INTERVAL '240' SECOND` is rejected ("exceeds precision of SECOND(2) field").
# Render each duration as the largest exact unit that fits the accepted range.
fmt_interval() {
  local s="$1"
  if [ "${s}" -ge 86400 ] && [ $((s % 86400)) -eq 0 ] && [ $((s / 86400)) -le 999 ]; then
    printf "INTERVAL '%d' DAY" $((s / 86400))
  elif [ "${s}" -ge 3600 ] && [ $((s % 3600)) -eq 0 ] && [ $((s / 3600)) -le 23 ]; then
    printf "INTERVAL '%d' HOUR" $((s / 3600))
  elif [ "${s}" -ge 60 ] && [ $((s % 60)) -eq 0 ] && [ $((s / 60)) -le 59 ]; then
    printf "INTERVAL '%d' MINUTE" $((s / 60))
  elif [ "${s}" -le 99 ]; then
    printf "INTERVAL '%d' SECOND" "${s}"
  elif [ "${s}" -le 999 ]; then
    printf "INTERVAL '%d' SECOND(3)" "${s}"
  else
    printf "INTERVAL '%d' HOUR + INTERVAL '%d' MINUTE + INTERVAL '%d' SECOND" \
      $((s / 3600)) $(((s % 3600) / 60)) $((s % 60))
  fi
}

# Service endpoints come from the environment so the same SQL runs in the
# standalone stack and in the shared platform compose.
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
SCHEMA_REGISTRY="${SCHEMA_REGISTRY:-http://schema-registry:8081}"
ICEBERG_REST="${ICEBERG_REST_URI:-http://iceberg-rest:8181}"
POSTGRES_DSN="${POSTGRES_DSN:-postgresql://app:app@postgres:5432/fintech}"
_dsn_rest="${POSTGRES_DSN#*://}"
_dsn_userinfo="${_dsn_rest%%@*}"
_dsn_host="${_dsn_rest#*@}"
POSTGRES_USER="${POSTGRES_USER:-${_dsn_userinfo%%:*}}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-${_dsn_userinfo#*:}}"
POSTGRES_JDBC="jdbc:postgresql://${_dsn_host}"

sed \
  -e "s|__KAFKA_BOOTSTRAP__|${KAFKA_BOOTSTRAP}|g" \
  -e "s|__SCHEMA_REGISTRY__|${SCHEMA_REGISTRY}|g" \
  -e "s|__ICEBERG_REST__|${ICEBERG_REST}|g" \
  -e "s|__POSTGRES_JDBC__|${POSTGRES_JDBC}|g" \
  -e "s|__POSTGRES_USER__|${POSTGRES_USER}|g" \
  -e "s|__POSTGRES_PASSWORD__|${POSTGRES_PASSWORD}|g" \
  -e "s|__SETTLE_WINDOW__|$(fmt_interval "${SETTLE_SEC}")|g" \
  -e "s|__LATE_WINDOW__|$(fmt_interval "${LATE_SEC}")|g" \
  -e "s|__CB_WINDOW__|$(fmt_interval "${CB_SEC}")|g" \
  -e "s|__WM_SWITCH__|$(fmt_interval "${WM_SWITCH}")|g" \
  -e "s|__WM_SETTLE__|$(fmt_interval "${WM_SETTLE}")|g" \
  -e "s|__WM_CALLBACK__|$(fmt_interval "${WM_CB}")|g" \
  -e "s|__IDLE_TIMEOUT__|${IDLE}s|g" \
  -e "s|__KPI_WINDOW__|INTERVAL '${KPI_MIN}' MINUTE|g" \
  -e "s|__ALERT_WINDOW__|INTERVAL '${ALERT_MIN}' MINUTE|g" \
  "$1"
