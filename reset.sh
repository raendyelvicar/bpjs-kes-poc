#!/usr/bin/env bash
# Removes everything setup.sh created for this NAME_SUFFIX: its ksqlDB queries, streams and their topics, the sample topic,
# the connector, the DLQ topic, the index template and the Elasticsearch index. Copies with another suffix are not touched.
# The OTel source topic (OTEL_TOPIC) is NOT deleted. Add --yes to skip the question.   ./reset.sh [--env local|dev|staging] [--yes]
set -euo pipefail
cd "$(dirname "$0")"
. ./env.sh
. ./names.sh
if [ "${1:-}" != --yes ]; then
  read -r -p "This deletes the OTel error pipeline in environment '${ENV_NAME:-default}' with suffix '$SFX' (index $INDEX) on $KSQL_URL / $ES_URL. Type yes to continue: " ans
  [ "$ans" = yes ] || { echo "cancelled"; exit 1; }
fi
req() { local auth="$1"; shift; if [ -n "$auth" ]; then curl -sS -u "$auth" "$@"; else curl -sS "$@"; fi; }
ksql() { jq -n --arg s "$1" '{ksql: $s, streamsProperties: {}}' | req "${KSQL_AUTH:-}" -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data @- "$KSQL_URL/ksql"; }

if [ -n "$SHARED_CONNECTOR" ]; then
  # Not ours: only take OUT_TOPIC out of its topics list (a topics.regex is left alone).
  cfg=$(req "${CONNECT_AUTH:-}" "$CONNECT_URL/connectors/$CONNECTOR/config" || true)
  if echo "$cfg" | jq -e --arg t "$OUT_TOPIC" 'has("topics") and ((.topics | split(",") | map(gsub("^\\s+|\\s+$"; "")) | index($t)) != null)' >/dev/null 2>&1; then
    echo "$cfg" | jq --arg t "$OUT_TOPIC" '.topics = ([.topics | split(",")[] | gsub("^\\s+|\\s+$"; "") | select(. != "" and . != $t)] | join(","))' \
      | req "${CONNECT_AUTH:-}" -X PUT -H 'Content-Type: application/json' --data @- "$CONNECT_URL/connectors/$CONNECTOR/config" >/dev/null \
      && echo "  removed $OUT_TOPIC from the topics of $CONNECTOR (connector kept)"
  else
    echo "  $CONNECTOR does not list $OUT_TOPIC: connector left as it is"
  fi
else
  req "${CONNECT_AUTH:-}" -X DELETE "$CONNECT_URL/connectors/$CONNECTOR" >/dev/null || true
fi
for id in $(ksql 'SHOW QUERIES;' | jq -r --argjson mine "$SINKS_JSON" '.[0].queries[]? | select(any(.sinks[]?; . as $k | $mine | index($k))) | .id'); do
  echo "  terminating $id"; ksql "TERMINATE $id;" >/dev/null
done
sleep 3
for s in $STREAMS $SAMPLE_STREAM; do
  ksql "DROP STREAM IF EXISTS $s DELETE TOPIC;" >/dev/null || true
done
ksql "DROP STREAM IF EXISTS $RAW_STREAM;" >/dev/null || true   # keeps the OTel source topic
# Topics left without a stream (the DLQ, made by Connect, or a pipeline topic the broker auto-created when the sink
# subscribed first): give each a throw-away stream so ksqlDB can delete it.
LEFTOVER="otel-logs-resource$SFX otel-logs-scope$SFX otel-logs-error-records$SFX $OUT_TOPIC"
[ -z "$SHARED_CONNECTOR" ] && LEFTOVER="$LEFTOVER $DLQ_TOPIC"
topics=$(ksql 'SHOW TOPICS;')
for t in $LEFTOVER; do
  echo "$topics" | jq -e --arg t "$t" '.[0].topics[]? | select(.name == $t)' >/dev/null || continue
  tmp="otel_logs_leftover_drop$SFX"
  ksql "CREATE STREAM IF NOT EXISTS $tmp (v VARCHAR) WITH (KAFKA_TOPIC = '$t', VALUE_FORMAT = 'KAFKA');" >/dev/null || true
  ksql "DROP STREAM IF EXISTS $tmp DELETE TOPIC;" >/dev/null || true
  echo "  deleted topic $t"
done
req "${ES_AUTH:-}" -X DELETE "$ES_URL/$INDEX" >/dev/null || true
req "${ES_AUTH:-}" -X DELETE "$ES_URL/_index_template/$INDEX" >/dev/null || true
echo "Reset done for suffix '$SFX'. Run ./setup.sh to start again."
