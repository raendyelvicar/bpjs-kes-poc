#!/usr/bin/env bash
# Sends sample OTel log payloads to the sample topic (OTEL_SAMPLE_TOPIC + NAME_SUFFIX).   ./load-data.sh [real|test|week]
#   real -> data/container-id.json and data/deployment-env.json, one message per file. Only INFO inside: expect 0 new documents.
#   test -> data/otel-logs-sample.jsonl: the same two payloads with ERROR/FATAL records added. Expect 4 new documents.
#   week -> data/otel-logs-week.jsonl: 7 days, 4 services, 2 incidents. Made by gen_otel_logs.py if missing.
# Needs docker (runs the Kafka console producer) and jq; python3 for "week" when the file has to be generated.
# Use your own producer instead of docker, e.g.:
#   PRODUCER_CMD='kafka-console-producer --bootstrap-server broker:9092 --topic otlp_logs_sample' ./load-data.sh test
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] && { set -a; . ./.env; set +a; }
. ./names.sh
TOPIC="$SAMPLE_TOPIC"
[ -n "$TOPIC" ] || { echo "Set OTEL_SAMPLE_TOPIC in .env and run ./setup.sh first."; exit 1; }
MODE="${1:-test}"
TMPF="$(mktemp)"; trap 'rm -f "$TMPF"' EXIT
case "$MODE" in
  real) jq -c . data/container-id.json data/deployment-env.json > "$TMPF"; FILE="$TMPF" ;;  # one line = one Kafka message
  test) FILE=data/otel-logs-sample.jsonl ;;
  week) FILE=data/otel-logs-week.jsonl; [ -f "$FILE" ] || python3 gen_otel_logs.py --out "$FILE" ;;
  *) echo "usage: $0 [real|test|week]"; exit 1 ;;
esac
# Expected documents = ERROR/FATAL records in the file, counted with the same rule as the pipeline.
expected=$(jq -s '[.[].resourceLogs[]?.scopeLogs[]?.logRecords[]?
  | ((.severityNumber // 0) as $n | ((.severityText // "") | ascii_upcase) as $t
     | select($n >= 17 or ($n == 0 and ($t == "ERROR" or $t == "FATAL" or $t == "CRITICAL" or $t == "SEVERE"))))] | length' "$FILE")
if [ -z "${PRODUCER_CMD:-}" ]; then
  : "${BOOTSTRAP:?set BOOTSTRAP in .env}"
  CFG=""; [ -f client.properties ] && CFG="--producer.config /work/client.properties"
  PRODUCER_CMD="docker run --rm -i ${DOCKER_ARGS:-} -v $PWD:/work confluentinc/cp-kafka:7.6.0 kafka-console-producer --bootstrap-server $BOOTSTRAP $CFG --topic $TOPIC"
fi
echo "Sending $(wc -l < "$FILE" | tr -d ' ') messages from $MODE to $TOPIC ..."
eval "$PRODUCER_CMD" < "$FILE"
echo "Done. Expected: $expected new ERROR/FATAL documents in $INDEX. Give it a minute, then run ./check.sh"
echo "Note: loading the same file again adds the documents again (each Kafka message gets a new offset, so a new id)."
