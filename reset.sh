#!/usr/bin/env bash
# Removes everything setup.sh created for this NAME_SUFFIX: its ksqlDB queries, streams and their topics, the sample topic,
# the connector, the DLQ topic, the index template and the Elasticsearch index. Copies with another suffix are not touched.
# The OTel source topic (OTEL_TOPIC) is NOT deleted. Add --yes to skip the question.
set -euo pipefail
cd "$(dirname "$0")"
set -a; . ./.env; set +a
. ./names.sh
if [ "${1:-}" != --yes ]; then
  read -r -p "This deletes the OTel error pipeline with suffix '$SFX' (index $INDEX) on $KSQL_URL / $ES_URL. Type yes to continue: " ans
  [ "$ans" = yes ] || { echo "cancelled"; exit 1; }
fi
req() { local auth="$1"; shift; if [ -n "$auth" ]; then curl -sS -u "$auth" "$@"; else curl -sS "$@"; fi; }
ksql() { jq -n --arg s "$1" '{ksql: $s, streamsProperties: {}}' | req "${KSQL_AUTH:-}" -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data @- "$KSQL_URL/ksql"; }

req "${CONNECT_AUTH:-}" -X DELETE "$CONNECT_URL/connectors/$CONNECTOR" >/dev/null || true
for id in $(ksql 'SHOW QUERIES;' | jq -r --argjson mine "$SINKS_JSON" '.[0].queries[]? | select(any(.sinks[]?; . as $k | $mine | index($k))) | .id'); do
  echo "  terminating $id"; ksql "TERMINATE $id;" >/dev/null
done
sleep 3
for s in $STREAMS $SAMPLE_STREAM; do
  ksql "DROP STREAM IF EXISTS $s DELETE TOPIC;" >/dev/null || true
done
ksql "DROP STREAM IF EXISTS $RAW_STREAM;" >/dev/null || true   # keeps the OTel source topic
# The DLQ topic was made by Connect, not ksqlDB: give it a throw-away stream so ksqlDB can delete it.
if ksql 'SHOW TOPICS;' | jq -e --arg t "$DLQ_TOPIC" '.[0].topics[]? | select(.name == $t)' >/dev/null; then
  tmp="otel_logs_dlq_drop$SFX"
  ksql "CREATE STREAM IF NOT EXISTS $tmp (v VARCHAR) WITH (KAFKA_TOPIC = '$DLQ_TOPIC', VALUE_FORMAT = 'KAFKA');" >/dev/null || true
  ksql "DROP STREAM IF EXISTS $tmp DELETE TOPIC;" >/dev/null || true
  echo "  deleted topic $DLQ_TOPIC"
fi
req "${ES_AUTH:-}" -X DELETE "$ES_URL/$INDEX" >/dev/null || true
req "${ES_AUTH:-}" -X DELETE "$ES_URL/_index_template/$INDEX" >/dev/null || true
echo "Reset done for suffix '$SFX'. Run ./setup.sh to start again."
