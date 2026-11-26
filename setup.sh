#!/usr/bin/env bash
# Sets up the OpenTelemetry ERROR/FATAL log pipeline in one go:
#   checks the services, then the Elasticsearch index template, the ksqlDB streams, the optional sample feed and the sink connector.
# Safe to rerun. Needs: bash, curl, jq, sed, awk.
#
#   ./setup.sh [--env NAME]                    do it, with .env.NAME (local, dev, staging; none = .env)
#   ./setup.sh [--env NAME] --dry-run [DIR]    change nothing: read-only requests (checks, SHOW TOPICS, DESCRIBE, reading the connector
#                                 config) still run so the plan is accurate, but every request that would change something is
#                                 written to DIR (default ./dry-run, or ./dry-run-NAME with --env) as commands.sh + request bodies, with passwords masked.
set -euo pipefail
cd "$(dirname "$0")"
. ./env.sh
DRY=0; PLAN=""
case "${1:-}" in
  "") ;;
  -n|--dry-run) DRY=1; PLAN="${2:-dry-run${ENV_NAME:+-$ENV_NAME}}" ;;
  *) echo "usage: $0 [--env local|dev|staging] [--dry-run [DIR]]" >&2; exit 2 ;;
esac
: "${KSQL_URL:?set KSQL_URL in $ENV_FILE}" "${CONNECT_URL:?set CONNECT_URL in $ENV_FILE}" "${ES_URL:?set ES_URL in $ENV_FILE}"
: "${ES_SINK_USER:?set ES_SINK_USER in $ENV_FILE}" "${ES_SINK_PASSWORD:?set ES_SINK_PASSWORD in $ENV_FILE}"
: "${OTEL_TOPIC:?set OTEL_TOPIC in $ENV_FILE (the Kafka topic OTel writes its logs to)}"
PARTITIONS="${PARTITIONS:-3}"; REPLICAS="${REPLICAS:-1}"; ES_SINK_URL="${ES_SINK_URL:-$ES_URL}"
. ./names.sh
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

# --- dry run: record instead of send -------------------------------------------------------------------------------
if [ "$DRY" = 1 ]; then
  case "$PLAN" in ""|/|.|..) echo "refusing to use '$PLAN' as the dry-run folder" >&2; exit 2 ;; esac
  rm -rf "$PLAN"; mkdir -p "$PLAN"; chmod 700 "$PLAN"; STEP=0
  cat > "$PLAN/commands.sh" <<'HDR'
#!/usr/bin/env bash
# Written by ./setup.sh --dry-run. These are the requests that change something, in the order setup.sh would send them.
# REVIEW ONLY: passwords in the request bodies are masked as "***", so do not run this file as it is.
set -euo pipefail
cd "$(dirname "$0")"
HDR
  # The *_AUTH values come from the same env file setup.sh used.
  printf '# Environment: %s. The *_AUTH values are read from %s.\nset -a; . ../%s; set +a\n' "${ENV_NAME:-default}" "$ENV_FILE" "$ENV_FILE" >> "$PLAN/commands.sh"
  echo "DRY RUN: nothing will be changed. Plan folder: $PLAN/"
fi
auth_var() {   # which .env login belongs to a URL (for the commands in the plan)
  case "$1" in "$KSQL_URL"*) echo KSQL_AUTH ;; "$CONNECT_URL"*) echo CONNECT_AUTH ;; "$ES_URL"*) echo ES_AUTH ;; *) echo "" ;; esac
}
# plan METHOD URL BODY-FILE LABEL [content-type] [sql-file]  -> adds one request to the plan
plan() {
  local method="$1" url="$2" body="$3" label="$4" ctype="${5:-application/json}" sql="${6:-}" f av
  STEP=$((STEP + 1)); f="$(printf '%02d' "$STEP")-$label"
  jq 'walk(if type == "object" then with_entries(if (.key | test("password|secret"; "i")) then .value = "***" else . end) else . end)' \
    "$body" > "$PLAN/$f.json"
  [ -n "$sql" ] && cp "$sql" "$PLAN/$f.sql"
  av=$(auth_var "$url")
  { echo; echo "# $STEP. $label${sql:+  (statements: $f.sql)}"
    echo "curl -sS \${$av:+-u \"\$$av\"} -X $method -H 'Content-Type: $ctype' --data @$f.json '$url'"; } >> "$PLAN/commands.sh"
  echo "  [dry run] would $method $url  ->  $PLAN/$f.json${sql:+ (+ $f.sql)}"
}
# did MESSAGE  -> what was done, or that it was only planned
did() { if [ "$DRY" = 1 ]; then echo "  [dry run] not done: $1"; else echo "  $1"; fi; }

# req AUTH curl-args...   (adds -u only when AUTH is not empty)
req() { local auth="$1"; shift; if [ -n "$auth" ]; then curl -sS -u "$auth" "$@"; else curl -sS "$@"; fi; }
# call AUTH METHOD URL [data-file]  -> body in $TMP/out, exits on non-2xx
call() {
  local auth="$1" method="$2" url="$3" data="${4:-}" code
  if [ "$DRY" = 1 ] && [ "$method" != GET ]; then
    plan "$method" "$url" "${data:-/dev/null}" "${CALL_LABEL:-request}"; echo '{}' > "$TMP/out"; return 0
  fi
  if [ -n "$data" ]; then
    code=$(req "$auth" -o "$TMP/out" -w '%{http_code}' -X "$method" -H 'Content-Type: application/json' --data @"$data" "$url") || code=000
  else
    code=$(req "$auth" -o "$TMP/out" -w '%{http_code}' -X "$method" "$url") || code=000
  fi
  case "$code" in 2*) return 0 ;; esac
  echo "  FAILED: $method $url -> HTTP $code" >&2; head -c 800 "$TMP/out" >&2 2>/dev/null; echo >&2
  [ "$code" = 401 ] && echo "  HTTP 401 = login refused: check the *_AUTH value for this service in $ENV_FILE." >&2
  return 1
}
# ksql STATEMENT-FILE [offset-reset]  -> runs read-only ksqlDB statements (SHOW, DESCRIBE), body in $TMP/out
# ksql_change STATEMENT-FILE LABEL [offset-reset]  -> runs statements that change something (recorded in a dry run)
ksql_change() {
  if [ "$DRY" = 1 ]; then
    jq -Rs --arg r "${3:-latest}" '{ksql: ., streamsProperties: {"ksql.streams.auto.offset.reset": $r}}' "$1" > "$TMP/ksql.json"
    plan POST "$KSQL_URL/ksql" "$TMP/ksql.json" "$2" application/vnd.ksql.v1+json "$1"; echo '[]' > "$TMP/out"; return 0
  fi
  ksql "$1" "${3:-latest}"
}
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
CALL_LABEL=es-index-template call "${ES_AUTH:-}" PUT "$ES_URL/_index_template/$INDEX" "$TMP/tpl.json"; did "template applied"

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
  [ -n "$others" ] && echo "  If OTel writes to a different topic, fix OTEL_TOPIC in $ENV_FILE. Topics that look like logs: $others"
fi
# A pipeline topic that exists without its stream (e.g. auto-created by the broker when the sink subscribed first)
# makes CREATE STREAM fail with HTTP 500 if its partition count differs. Catch that here with a clear message.
echo 'SHOW STREAMS;' > "$TMP/ss.sql"; ksql "$TMP/ss.sql"; cp "$TMP/out" "$TMP/streams.json"
for t in "otel-logs-resource$SFX" "otel-logs-scope$SFX" "otel-logs-error-records$SFX" "$OUT_TOPIC"; do
  has_topic "$t" || continue
  jq -e --arg t "$t" '.[0].streams[]? | select(.topic == $t)' "$TMP/streams.json" >/dev/null && continue
  p=$(jq -r --arg t "$t" '.[0].topics[] | select(.name == $t) | .replicaInfo | length' "$TMP/topics.json")
  if [ "$p" != "$PARTITIONS" ]; then
    echo "  topic $t already exists with $p partition(s) but no stream (PARTITIONS=$PARTITIONS). Run ./reset.sh to delete it, or set PARTITIONS=$p in $ENV_FILE." >&2; exit 1
  fi
done
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
ksql_change "$TMP/pipeline.sql" ksql-pipeline-streams earliest
did "statements applied"

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
  ksql_change "$TMP/sample.sql" ksql-sample-stream
  # INSERT INTO has no IF NOT EXISTS: start it only if no query reads the sample stream yet, or every row would arrive twice.
  echo 'SHOW QUERIES;' > "$TMP/q.sql"; ksql "$TMP/q.sql"
  if jq -e --arg s "$SAMPLE_STREAM" '.[0].queries[]? | select(.queryString | test("FROM\\s+`?" + $s + "`?(\\s|;|$)"; "i"))' "$TMP/out" >/dev/null; then
    echo "  sample feed already running"
  else
    echo "INSERT INTO $RESOURCE_STREAM SELECT EXPLODE(\`resourceLogs\`) AS rl FROM $SAMPLE_STREAM WHERE \`resourceLogs\` IS NOT NULL EMIT CHANGES;" > "$TMP/ins.sql"
    ksql_change "$TMP/ins.sql" ksql-sample-insert earliest
    did "sample feed started: $SAMPLE_TOPIC -> $RESOURCE_STREAM"
  fi
fi

echo "== 5/6 Elasticsearch sink connector $CONNECTOR"
if [ -n "$SHARED_CONNECTOR" ]; then
  # Use an existing connector: add OUT_TOPIC to its topics and keep every other setting as it is.
  # The current config comes from EXISTING_CONNECTOR_CONFIG (a saved copy) when set, otherwise from Connect.
  CFG_FILE="${EXISTING_CONNECTOR_CONFIG:-}"
  if [ -n "$CFG_FILE" ]; then
    [ -f "$CFG_FILE" ] || { echo "  EXISTING_CONNECTOR_CONFIG='$CFG_FILE' does not exist." >&2; exit 1; }
    # Accepts the plain config map, {"name": ..., "config": {...}} (the POST /connectors shape), or a .properties file.
    if jq -e . "$CFG_FILE" >/dev/null 2>&1; then
      jq 'if (.config | type) == "object" then .config else . end | with_entries(.value |= tostring)' "$CFG_FILE" > "$TMP/cur.json"
    else
      jq -Rn '[inputs | sub("^\\s+"; "") | select(. != "" and (startswith("#") or startswith("!") | not))
              | capture("^(?<k>[^=:]+?)\\s*[=:]\\s*(?<v>.*)$")] | map({(.k): .v}) | add // {}' "$CFG_FILE" > "$TMP/cur.json"
    fi
    cfg_name=$(jq -r '.name // empty' "$TMP/cur.json")
    [ -n "$cfg_name" ] && [ "$cfg_name" != "$CONNECTOR" ] && { echo "  $CFG_FILE is for connector '$cfg_name', but EXISTING_CONNECTOR is '$CONNECTOR'." >&2; exit 1; }
    jq -e 'has("connector.class")' "$TMP/cur.json" >/dev/null || { echo "  $CFG_FILE has no connector.class: is it a connector config?" >&2; exit 1; }
    echo "  using the config in $CFG_FILE"
  else
    call "${CONNECT_AUTH:-}" GET "$CONNECT_URL/connectors/$CONNECTOR/config" \
      || { echo "  Could not read EXISTING_CONNECTOR='$CONNECTOR' from $CONNECT_URL. If this login may not read it (HTTP 403)," >&2
           echo "  save its config to a file and set EXISTING_CONNECTOR_CONFIG to that file." >&2; exit 1; }
    cp "$TMP/out" "$TMP/cur.json"
  fi
  stop() { echo "  Not changing $CONNECTOR: $1" >&2; exit 1; }
  jq -e '."connector.class" | test("ElasticsearchSinkConnector")' "$TMP/cur.json" >/dev/null || stop "it is not an Elasticsearch sink."
  # Some platforms hide passwords in GET /config; writing those back would break the connector.
  jq -e '[.[] | select(test("^\\[hidden\\]$|^\\*+$"))] | length == 0' "$TMP/cur.json" >/dev/null \
    || stop "its config comes back with hidden values (e.g. the password), so it cannot be written back safely. Add the topic '$OUT_TOPIC' by hand."
  # The documents are schemaless JSON with no key: the connector must read them that way, or the shared connector's tasks fail.
  jq -e '(."value.converter" // "") | test("JsonConverter$")' "$TMP/cur.json" >/dev/null \
    || stop "value.converter is '$(jq -r '."value.converter" // "worker default"' "$TMP/cur.json")'; this pipeline writes plain JSON and needs org.apache.kafka.connect.json.JsonConverter."
  jq -e '(."value.converter.schemas.enable" // "true") == "false"' "$TMP/cur.json" >/dev/null \
    || stop "value.converter.schemas.enable is not \"false\"; the documents have no schema envelope."
  jq -e --arg t "$OUT_TOPIC" '(."key.ignore" // "false") == "true" or ((."topic.key.ignore" // "") | split(",") | map(gsub("^\\s+|\\s+$"; "")) | index($t))' "$TMP/cur.json" >/dev/null \
    || stop "key.ignore is not \"true\" and the record keys are empty. Set key.ignore=true, or add '$OUT_TOPIC' to its topic.key.ignore."
  [ -n "$(jq -r '.transforms // empty' "$TMP/cur.json")" ] \
    && echo "  NOTE: it has transforms ($(jq -r .transforms "$TMP/cur.json")). If one renames the index, the template for '$INDEX' will not apply."
  cur_url=$(jq -r '."connection.url" // ""' "$TMP/cur.json")
  [ "$cur_url" != "$ES_SINK_URL" ] && echo "  NOTE: it writes to $cur_url; the index template was put on $ES_URL. They must be the same Elasticsearch."
  if jq -e 'has("topics.regex")' "$TMP/cur.json" >/dev/null; then
    jq -e --arg t "$OUT_TOPIC" '."topics.regex" as $r | $t | test("^(" + $r + ")$")' "$TMP/cur.json" >/dev/null \
      || stop "it uses topics.regex '$(jq -r '."topics.regex"' "$TMP/cur.json")', which does not match '$OUT_TOPIC'. Widen the regex by hand."
    echo "  its topics.regex already matches $OUT_TOPIC: nothing to change"
  elif jq -e --arg t "$OUT_TOPIC" '(.topics // "") | split(",") | map(gsub("^\\s+|\\s+$"; "")) | index($t)' "$TMP/cur.json" >/dev/null; then
    echo "  $OUT_TOPIC is already in its topics: nothing to change"
  else
    jq --arg t "$OUT_TOPIC" '.topics = ([(.topics // "") | split(",")[] | gsub("^\\s+|\\s+$"; "") | select(. != "")] + [$t] | join(","))' \
      "$TMP/cur.json" > "$TMP/sink.json"
    # Always keep the new config locally, so it can be applied by someone who may change the connector.
    OUT_CFG="connector-config.$CONNECTOR.json"
    if [ "$DRY" = 1 ]; then
      echo "  [dry run] would write the new config to $OUT_CFG"
      jq -r --arg t "$OUT_TOPIC" '"  [dry run] topics: \(.topics)  (adds \($t))"' "$TMP/sink.json"
    else
      jq 'del(.name)' "$TMP/sink.json" > "$OUT_CFG"; chmod 600 "$OUT_CFG"
      echo "  new config written to $OUT_CFG (it contains the connector's password: do not commit it)"
    fi
    if ! CALL_LABEL=connect-update-existing-connector call "${CONNECT_AUTH:-}" PUT "$CONNECT_URL/connectors/$CONNECTOR/config" "$TMP/sink.json"; then
      echo "  Could not update $CONNECTOR with this login. Ask someone who may change it to apply the file:" >&2
      echo "    curl -u USER:PASS -X PUT -H 'Content-Type: application/json' --data @$OUT_CFG $CONNECT_URL/connectors/$CONNECTOR/config" >&2
      exit 1
    fi
    [ "$DRY" = 0 ] && [ -n "$CFG_FILE" ] && echo "  NOTE: $CFG_FILE is now out of date; $OUT_CFG holds the current config."
    did "added $OUT_TOPIC to the topics of $CONNECTOR (now: $(jq -r .topics "$TMP/sink.json"))"
  fi
else
# key.ignore=true: document id = topic+partition+offset, so a replay of the same topic overwrites instead of duplicating.
# ES_SINK_TRUSTSTORE set = connect over SSL with that truststore (a path on the Connect workers, e.g. for a self-signed ES cert).
jq -n --arg topic "$OUT_TOPIC" --arg dlq "$DLQ_TOPIC" --arg url "$ES_SINK_URL" --arg user "$ES_SINK_USER" --arg pass "$ES_SINK_PASSWORD" --arg rf "$REPLICAS" --arg tasks "$PARTITIONS" \
  --arg ts "${ES_SINK_TRUSTSTORE:-}" --arg tspass "${ES_SINK_TRUSTSTORE_PASSWORD:-}" --arg tstype "${ES_SINK_TRUSTSTORE_TYPE:-JKS}" '{
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
  "behavior.on.malformed.documents": "warn",
  "errors.deadletterqueue.topic.name": $dlq,
  "errors.deadletterqueue.topic.replication.factor": $rf,
  "errors.deadletterqueue.context.headers.enable": "true"
} + (if $ts == "" then {} else {
  "elastic.security.protocol": "SSL",
  "elastic.https.ssl.truststore.location": $ts,
  "elastic.https.ssl.truststore.password": $tspass,
  "elastic.https.ssl.truststore.type": $tstype
} end)' > "$TMP/sink.json"
CALL_LABEL=connect-create-connector call "${CONNECT_AUTH:-}" PUT "$CONNECT_URL/connectors/$CONNECTOR/config" "$TMP/sink.json"
did "connector $CONNECTOR created or updated"
fi

if [ "$DRY" = 1 ]; then
  echo "== 6/6 Status: skipped in a dry run"
  echo
  echo "Dry run done: nothing was changed. $STEP request(s) would change something; review them in:"
  echo "  $PLAN/commands.sh   (the curl commands, in order)"
  ls "$PLAN" | grep -v '^commands.sh$' | sed "s|^|  $PLAN/|"
  exit 0
fi
echo "== 6/6 Status"
sleep 5
./check.sh --quiet || true
echo
echo "Done."
[ -n "$SAMPLE_TOPIC" ] && echo "Test it: ./load-data.sh test   (4 documents)   or   ./load-data.sh week"
echo "Kibana: create a data view '$INDEX' with time field '@timestamp'."
