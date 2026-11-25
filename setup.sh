#!/usr/bin/env bash
# Sets up the OpenTelemetry ERROR/FATAL log pipeline in one go:
#   checks the services, then the Elasticsearch index template, the ksqlDB streams, the optional sample feed and the sink connector.
# Safe to rerun. Needs: bash, curl, jq, sed, awk.
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] || { echo "Copy .env.example to .env and fill it in first."; exit 1; }
set -a; . ./.env; set +a
: "${KSQL_URL:?set KSQL_URL in .env}" "${CONNECT_URL:?set CONNECT_URL in .env}" "${ES_URL:?set ES_URL in .env}"
: "${ES_SINK_USER:?set ES_SINK_USER in .env}" "${ES_SINK_PASSWORD:?set ES_SINK_PASSWORD in .env}"
: "${OTEL_TOPIC:?set OTEL_TOPIC in .env (the Kafka topic OTel writes its logs to)}"
PARTITIONS="${PARTITIONS:-3}"; REPLICAS="${REPLICAS:-1}"; ES_SINK_URL="${ES_SINK_URL:-$ES_URL}"
. ./names.sh
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

# req AUTH curl-args...   (adds -u only when AUTH is not empty)
req() { local auth="$1"; shift; if [ -n "$auth" ]; then curl -sS -u "$auth" "$@"; else curl -sS "$@"; fi; }
# call AUTH METHOD URL [data-file]  -> body in $TMP/out, exits on non-2xx
call() {
  local auth="$1" method="$2" url="$3" data="${4:-}" code
  if [ -n "$data" ]; then
    code=$(req "$auth" -o "$TMP/out" -w '%{http_code}' -X "$method" -H 'Content-Type: application/json' --data @"$data" "$url") || code=000
  else
    code=$(req "$auth" -o "$TMP/out" -w '%{http_code}' -X "$method" "$url") || code=000
  fi
  case "$code" in 2*) return 0 ;; esac
  echo "  FAILED: $method $url -> HTTP $code" >&2; head -c 800 "$TMP/out" >&2 2>/dev/null; echo >&2
  [ "$code" = 401 ] && echo "  HTTP 401 = login refused: check the *_AUTH value for this service in .env." >&2
  return 1
}
# ksql STATEMENT-FILE [offset-reset]  -> runs ksqlDB statements, body in $TMP/out
ksql() {
  jq -Rs --arg r "${2:-latest}" '{ksql: ., streamsProperties: {"ksql.streams.auto.offset.reset": $r}}' "$1" > "$TMP/ksql.json"
  local code
  code=$(req "${KSQL_AUTH:-}" -o "$TMP/out" -w '%{http_code}' -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data @"$TMP/ksql.json" "$KSQL_URL/ksql") || code=000
  [ "$code" = 200 ] && return 0
  echo "  FAILED: HTTP $code" >&2; jq -r '.message // .' "$TMP/out" >&2 2>/dev/null || head -c 800 "$TMP/out" >&2; return 1
}

echo "== 1/6 Checking the services"
bad() { echo "  $1 ($2) did not answer like $1. $3" >&2; exit 1; }
call "${KSQL_AUTH:-}" GET "$KSQL_URL/info"
v=$(jq -er '.KsqlServerInfo.version' "$TMP/out" 2>/dev/null) || bad ksqlDB "$KSQL_URL" "Check KSQL_URL (usually port 8088) and KSQL_AUTH."
echo "  ksqlDB ok: $v"
call "${CONNECT_AUTH:-}" GET "$CONNECT_URL/"
v=$(jq -er '.version' "$TMP/out" 2>/dev/null) || bad "Kafka Connect" "$CONNECT_URL" "Check CONNECT_URL (usually port 8083) and CONNECT_AUTH."
echo "  Connect ok: $v"
call "${CONNECT_AUTH:-}" GET "$CONNECT_URL/connector-plugins"
jq -e '.[] | select(.class | test("ElasticsearchSinkConnector"))' "$TMP/out" >/dev/null \
  || { echo "  The Confluent Elasticsearch sink plugin is not installed on this Connect cluster." >&2; exit 1; }
echo "  Elasticsearch sink plugin found"
call "${ES_AUTH:-}" GET "$ES_URL/"
v=$(jq -er '.version.number' "$TMP/out" 2>/dev/null) || bad Elasticsearch "$ES_URL" "ES_URL must be Elasticsearch (usually port 9200), not Kibana (5601). Check ES_AUTH too."
major=${v%%.*}; minor=${v#*.}; minor=${minor%%.*}
if [ "$major" -lt 8 ] || { [ "$major" = 8 ] && [ "$minor" -lt 3 ]; }; then
  echo "  Elasticsearch $v is too old: the index template needs 8.3 or newer (\"subobjects\": false)." >&2; exit 1
fi
echo "  Elasticsearch ok: $v"

echo "== 2/6 Elasticsearch index template $INDEX"
# resource and attributes use "subobjects": false so keys like "deployment.environment" and
# "deployment.environment.name" can live side by side (normally they clash and the document is rejected).
# The pattern is the exact index name, so copies with other suffixes never match each other's template.
cat > "$TMP/tpl.json" <<JSON
{
  "index_patterns": ["$INDEX"],
  "priority": 200,
  "template": { "mappings": {
    "dynamic_templates": [
      { "resource_strings":   { "path_match": "resource.*",   "mapping": { "type": "keyword", "ignore_above": 1024 } } },
      { "attributes_strings": { "path_match": "attributes.*", "mapping": { "type": "keyword", "ignore_above": 1024 } } }
    ],
    "properties": {
      "@timestamp":          { "type": "date", "format": "epoch_millis" },
      "severity":            { "type": "keyword" },
      "severity_number":     { "type": "integer" },
      "severity_text":       { "type": "keyword" },
      "message":             { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 1024 } } },
      "service_name":        { "type": "keyword" },
      "service_version":     { "type": "keyword" },
      "service_instance_id": { "type": "keyword" },
      "environment":         { "type": "keyword" },
      "host_name":           { "type": "keyword" },
      "container_id":        { "type": "keyword" },
      "sdk_language":        { "type": "keyword" },
      "scope_name":          { "type": "keyword" },
      "trace_id":            { "type": "keyword" },
      "span_id":             { "type": "keyword" },
      "resource":            { "type": "object", "subobjects": false },
      "attributes":          { "type": "object", "subobjects": false, "properties": {
        "exception.message":    { "type": "text" },
        "exception.stacktrace": { "type": "text" },
        "exception.type":       { "type": "keyword" } } }
    } } }
}
JSON
call "${ES_AUTH:-}" PUT "$ES_URL/_index_template/$INDEX" "$TMP/tpl.json"; echo "  template applied"

echo "== 3/6 ksqlDB streams (suffix='$SFX', source topic=$OTEL_TOPIC, partitions=$PARTITIONS, replicas=$REPLICAS)"
echo 'SHOW TOPICS;' > "$TMP/st.sql"; ksql "$TMP/st.sql"; cp "$TMP/out" "$TMP/topics.json"
has_topic() { jq -e --arg t "$1" '.[0].topics[]? | select(.name == $t)' "$TMP/topics.json" >/dev/null; }
# An existing topic keeps its partitions; a missing one is created by ksqlDB (otherwise CREATE STREAM fails).
RAW_OPTS=""
if has_topic "$OTEL_TOPIC"; then
  echo "  source topic $OTEL_TOPIC found"
else
  RAW_OPTS=", PARTITIONS = $PARTITIONS, REPLICAS = $REPLICAS"
  echo "  source topic $OTEL_TOPIC does not exist yet: creating it ($PARTITIONS partitions)."
  others=$(jq -r '[.[0].topics[]?.name | select(test("otlp|otel|log"; "i"))] | join(", ")' "$TMP/topics.json")
  [ -n "$others" ] && echo "  If OTel writes to a different topic, fix OTEL_TOPIC in .env. Topics that look like logs: $others"
fi
# An existing raw stream on another topic would be kept silently by IF NOT EXISTS, so check first.
echo "DESCRIBE $RAW_STREAM EXTENDED;" > "$TMP/d.sql"
if ksql "$TMP/d.sql" 2>/dev/null; then
  cur=$(jq -r '.[0].sourceDescription.topic // empty' "$TMP/out")
  if [ -n "$cur" ] && [ "$cur" != "$OTEL_TOPIC" ]; then
    echo "  $RAW_STREAM already exists but reads '$cur', not '$OTEL_TOPIC'. Run ./reset.sh first, then ./setup.sh again." >&2; exit 1
  fi
fi
sed -e "s/__OTEL_TOPIC__/$OTEL_TOPIC/g" -e "s/__PARTITIONS__/$PARTITIONS/g" -e "s/__REPLICAS__/$REPLICAS/g" \
    -e "s/__RAW_TOPIC_OPTS__/$RAW_OPTS/g" -e "s/__SFX__/$SFX/g" ksql/otel-logs.sql > "$TMP/pipeline.sql"
ksql "$TMP/pipeline.sql" earliest
echo "  statements applied"

echo "== 4/6 Sample feed"
# Optional second source with the same schema, fed into the same chain at the resource stream.
# Everything after that (filter, flattening, sink, index) is shared with the real logs.
if [ -z "$SAMPLE_TOPIC" ]; then
  echo "  OTEL_SAMPLE_TOPIC is empty: skipped"
else
  SAMPLE_OPTS=", PARTITIONS = $PARTITIONS, REPLICAS = $REPLICAS"
  if has_topic "$SAMPLE_TOPIC"; then SAMPLE_OPTS=""; echo "  sample topic $SAMPLE_TOPIC found"
  else echo "  sample topic $SAMPLE_TOPIC does not exist yet: creating it ($PARTITIONS partitions)."; fi
  # Reuse the raw stream's schema from ksql/otel-logs.sql so it is written down only once.
  sed -e "s/otel_logs_raw__SFX__/$SAMPLE_STREAM/" -e "s/__OTEL_TOPIC__/$SAMPLE_TOPIC/" -e "s/__RAW_TOPIC_OPTS__/$SAMPLE_OPTS/" ksql/otel-logs.sql \
    | awk -v s="$SAMPLE_STREAM" '$0 ~ "^CREATE STREAM IF NOT EXISTS " s " \\(" {on=1} on{print} on&&/\);$/{exit}' > "$TMP/sample.sql"
  [ -s "$TMP/sample.sql" ] || { echo "  could not build the sample stream statement" >&2; exit 1; }
  ksql "$TMP/sample.sql"
  # INSERT INTO has no IF NOT EXISTS: start it only if no query reads the sample stream yet, or every row would arrive twice.
  echo 'SHOW QUERIES;' > "$TMP/q.sql"; ksql "$TMP/q.sql"
  if jq -e --arg s "$SAMPLE_STREAM" '.[0].queries[]? | select(.queryString | test("FROM\\s+`?" + $s + "`?(\\s|;|$)"; "i"))' "$TMP/out" >/dev/null; then
    echo "  sample feed already running"
  else
    echo "INSERT INTO $RESOURCE_STREAM SELECT EXPLODE(\`resourceLogs\`) AS rl FROM $SAMPLE_STREAM WHERE \`resourceLogs\` IS NOT NULL EMIT CHANGES;" > "$TMP/ins.sql"
    ksql "$TMP/ins.sql" earliest
    echo "  sample feed started: $SAMPLE_TOPIC -> $RESOURCE_STREAM"
  fi
fi

echo "== 5/6 Elasticsearch sink connector $CONNECTOR"
# key.ignore=true: document id = topic+partition+offset, so a replay of the same topic overwrites instead of duplicating.
jq -n --arg topic "$OUT_TOPIC" --arg dlq "$DLQ_TOPIC" --arg url "$ES_SINK_URL" --arg user "$ES_SINK_USER" --arg pass "$ES_SINK_PASSWORD" --arg rf "$REPLICAS" --arg tasks "$PARTITIONS" '{
  "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
  "tasks.max": $tasks,
  "topics": $topic,
  "connection.url": $url,
  "connection.username": $user,
  "connection.password": $pass,
  "key.ignore": "true",
  "schema.ignore": "true",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.deadletterqueue.topic.name": $dlq,
  "errors.deadletterqueue.topic.replication.factor": $rf,
  "errors.deadletterqueue.context.headers.enable": "true"
}' > "$TMP/sink.json"
call "${CONNECT_AUTH:-}" PUT "$CONNECT_URL/connectors/$CONNECTOR/config" "$TMP/sink.json"
echo "  connector $CONNECTOR created or updated"

echo "== 6/6 Status"
sleep 5
./check.sh --quiet || true
echo
echo "Done."
[ -n "$SAMPLE_TOPIC" ] && echo "Test it: ./load-data.sh test   (4 documents)   or   ./load-data.sh week"
echo "Kibana: create a data view '$INDEX' with time field '@timestamp'."
