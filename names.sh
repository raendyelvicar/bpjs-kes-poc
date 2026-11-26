# Every name this pipeline creates, in one place. Sourced by setup.sh, check.sh, reset.sh and load-data.sh after env.sh has loaded the env file.
# NAME_SUFFIX (e.g. _dev) is added to all of them, so several copies can live on one cluster and you can tell yours apart.
# The OTel source topic (OTEL_TOPIC) belongs to OTel and gets no suffix.
SFX="${NAME_SUFFIX:-}"
case "$SFX" in *[!a-z0-9_]*) echo "NAME_SUFFIX may contain only a-z, 0-9 and _ (got '$SFX')." >&2; exit 1 ;; esac
SFX_UP="$(printf '%s' "$SFX" | tr '[:lower:]' '[:upper:]')"

INDEX="otel-logs-error$SFX"                    # Elasticsearch index, and the name of its index template
# EXISTING_CONNECTOR (in .env) = use that sink instead of creating one: OUT_TOPIC is added to its topics, nothing else changes.
SHARED_CONNECTOR="${EXISTING_CONNECTOR:-}"
CONNECTOR="${SHARED_CONNECTOR:-es-sink-otel-logs-error$SFX}"   # Kafka Connect sink (its consumer group is connect-<this>)
OUT_TOPIC="otel-logs-error$SFX"                # topic the sink reads
DLQ_TOPIC="otel-logs-error-dlq$SFX"            # documents Elasticsearch rejected
SAMPLE_TOPIC=""; [ -n "${OTEL_SAMPLE_TOPIC:-}" ] && SAMPLE_TOPIC="$OTEL_SAMPLE_TOPIC$SFX"
SAMPLE_STREAM="otel_logs_sample_raw$SFX"
RAW_STREAM="otel_logs_raw$SFX"
RESOURCE_STREAM="otel_logs_resource$SFX"
# Streams in drop order (last stage first). ksqlDB shows them in upper case.
STREAMS="otel_logs_error$SFX otel_logs_error_records$SFX otel_logs_scope$SFX otel_logs_resource$SFX"
RAW_STREAM_UP="$(printf %s "$RAW_STREAM" | tr a-z A-Z)"; SAMPLE_STREAM_UP="$(printf %s "$SAMPLE_STREAM" | tr a-z A-Z)"
SINKS_JSON="[\"OTEL_LOGS_RESOURCE$SFX_UP\",\"OTEL_LOGS_SCOPE$SFX_UP\",\"OTEL_LOGS_ERROR_RECORDS$SFX_UP\",\"OTEL_LOGS_ERROR$SFX_UP\"]"
