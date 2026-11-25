#!/usr/bin/env bash
# Shows the state of the OTel error pipeline (only the names with NAME_SUFFIX): streams, queries, connector, index.
# Read-only. Needs: bash, curl, jq.
set -uo pipefail
cd "$(dirname "$0")"
[ -f .env ] || { echo "Copy .env.example to .env and fill it in first."; exit 1; }
set -a; . ./.env; set +a
. ./names.sh
req() { local auth="$1"; shift; if [ -n "$auth" ]; then curl -sS -m 20 -u "$auth" "$@"; else curl -sS -m 20 "$@"; fi; }
ksql() { jq -n --arg s "$1" '{ksql: $s, streamsProperties: {}}' \
  | req "${KSQL_AUTH:-}" -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data @- "$KSQL_URL/ksql"; }
ok=0

echo "-- ksqlDB streams (suffix '$SFX')"
ksql 'SHOW STREAMS;' | jq -r --argjson mine "$SINKS_JSON" --arg raw "$RAW_STREAM_UP" --arg smp "$SAMPLE_STREAM_UP" \
  '.[0].streams[]? | select(.name as $n | ($mine + [$raw, $smp]) | index($n)) | "  \(.name) <- topic \(.topic)"'
echo "-- ksqlDB queries"
q=$(ksql 'SHOW QUERIES;')
MINE='[.[0].queries[]? | select(any(.sinks[]?; . as $k | $mine | index($k)))]'
echo "$q" | jq -r --argjson mine "$SINKS_JSON" "$MINE"' | .[] | "  \(.id) -> \(.sinks | join(",")) [\(.state // (.statusCount | tostring))]"'
n=$(echo "$q" | jq --argjson mine "$SINKS_JSON" "$MINE | length")
bad=$(echo "$q" | jq --argjson mine "$SINKS_JSON" "$MINE"' | map(select((.state // "RUNNING") != "RUNNING")) | length')
[ "$n" -ge 4 ] && [ "$bad" = 0 ] || { echo "  WARNING: expected at least 4 RUNNING queries, found $n ($bad not running)"; ok=1; }

echo "-- Connector $CONNECTOR"
s=$(req "${CONNECT_AUTH:-}" "$CONNECT_URL/connectors/$CONNECTOR/status")
echo "$s" | jq -r '"  connector: \(.connector.state)", (.tasks[] | "  task \(.id): \(.state)\(if .trace then " - " + (.trace | split("\n")[0]) else "" end)")' 2>/dev/null \
  || { echo "  not found: $(echo "$s" | head -c 200)"; ok=1; }
echo "$s" | jq -e '.connector.state == "RUNNING" and all(.tasks[]; .state == "RUNNING")' >/dev/null 2>&1 || ok=1

echo "-- Elasticsearch index $INDEX"
c=$(req "${ES_AUTH:-}" -H 'Content-Type: application/json' "$ES_URL/$INDEX/_search" \
     --data '{"size":0,"track_total_hits":true,"aggs":{"sev":{"terms":{"field":"severity"}},"svc":{"terms":{"field":"service_name","size":20}},"last":{"max":{"field":"@timestamp"}}}}')
if echo "$c" | jq -e '.hits' >/dev/null 2>&1; then
  echo "$c" | jq -r '"  documents: \(.hits.total.value)",
    "  by severity: \([.aggregations.sev.buckets[] | "\(.key)=\(.doc_count)"] | join(", "))",
    "  by service:  \([.aggregations.svc.buckets[] | "\(.key)=\(.doc_count)"] | join(", "))",
    "  newest log:  \(if .aggregations.last.value then (.aggregations.last.value / 1000 | floor | todate) else "-" end)"'
elif echo "$c" | grep -q index_not_found; then
  echo "  no index yet: it is created with the first ERROR/FATAL document (INFO/WARN never arrive)"
else
  echo "  could not query: $(echo "$c" | head -c 200)"; ok=1
fi

echo "-- Rejected documents"
# Connect creates the DLQ topic when the connector starts, so look for a message in it, not just the topic.
# PRINT returns a "rowtime:" line for the first message; an empty topic returns nothing until the timeout.
if ! ksql 'SHOW TOPICS;' | jq -e --arg t "$DLQ_TOPIC" '.[0].topics[]? | select(.name == $t)' >/dev/null; then
  echo "  none (no $DLQ_TOPIC topic yet)"
elif jq -n --arg t "$DLQ_TOPIC" '{ksql: ("PRINT '"'"'" + $t + "'"'"' FROM BEGINNING LIMIT 1;"), streamsProperties: {}}' \
     | req "${KSQL_AUTH:-}" -m 8 -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data @- "$KSQL_URL/query" 2>/dev/null \
     | grep -q 'rowtime:'; then
  echo "  WARNING: $DLQ_TOPIC has messages: Elasticsearch rejected at least one document."
  echo "  See why: the Connect worker log, or the 'connect.errors.*' headers of the messages in that topic."; ok=1
else
  echo "  none ($DLQ_TOPIC is empty)"
fi
[ "${1:-}" = --quiet ] || { [ $ok = 0 ] && echo "All OK." || echo "Something needs attention (see above)."; }
exit $ok
