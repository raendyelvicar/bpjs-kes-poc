#!/usr/bin/env bash
# Sends sample OTel log payloads to the sample topic (OTEL_SAMPLE_TOPIC + NAME_SUFFIX).   ./load-data.sh [--env local|dev|staging] [real|test|week|aspnet|python]
#   real -> data/container-id.json and data/deployment-env.json, one message per file. Only INFO inside: expect 0 new documents.
#   test -> data/otel-logs-sample.jsonl: the same two payloads with ERROR/FATAL records added. Expect 4 new documents.
#   week -> data/otel-logs-week.jsonl: 7 days, 6 services (Java, .NET, Python), 2 incidents. Made by gen_otel_logs.py if missing.
#   aspnet -> data/otel-logs-aspnet.jsonl: 7 days of the .NET services only, mostly ASP.NET Core request messages
#             ("Request starting" / "Request finished", some failing with 500 + an ERROR record). Made if missing.
#   python -> data/otel-logs-python.jsonl: 7 days of the Python ocrhub services only (pytds login rounds, konektor.putaran,
#             latar.galat errors). Made if missing.
#   Delete a generated file to make it again (e.g. after changing gen_otel_logs.py, or to move the dates to now).
# Needs docker (runs the Kafka console producer) and jq; python3 for "week" when the file has to be generated.
# Use your own producer instead of docker, e.g.:
#   PRODUCER_CMD='kafka-console-producer --bootstrap-server broker:9092 --topic otlp_logs_sample' ./load-data.sh test
set -euo pipefail
cd "$(dirname "$0")"
. ./env.sh
. ./names.sh
TOPIC="$SAMPLE_TOPIC"
[ -n "$TOPIC" ] || { echo "Set OTEL_SAMPLE_TOPIC in $ENV_FILE and run ./setup.sh first."; exit 1; }
MODE="${1:-test}"
TMPF="$(mktemp)"; trap 'rm -f "$TMPF"' EXIT
case "$MODE" in
  real) jq -c . data/container-id.json data/deployment-env.json > "$TMPF"; FILE="$TMPF" ;;  # one line = one Kafka message
  test) FILE=data/otel-logs-sample.jsonl ;;
  week) FILE=data/otel-logs-week.jsonl; [ -f "$FILE" ] || python3 gen_otel_logs.py --out "$FILE" ;;
  aspnet) FILE=data/otel-logs-aspnet.jsonl
          [ -f "$FILE" ] || python3 gen_otel_logs.py --out "$FILE" --services WorkerSDMK.Api,Notification.Api --aspnet-share 0.9 ;;
  python) FILE=data/otel-logs-python.jsonl
          [ -f "$FILE" ] || python3 gen_otel_logs.py --out "$FILE" --services ocrhub-konektor-jkndrive,ocrhub-gateway-latar ;;
  *) echo "usage: $0 [--env NAME] [real|test|week|aspnet|python]"; exit 1 ;;
esac
# Expected documents = ERROR/FATAL records in the file, counted with the same rule as the pipeline.
expected=$(jq -s '[.[].resourceLogs[]?.scopeLogs[]?.logRecords[]?
  | ((.severityNumber // 0) as $n | ((.severityText // "") | ascii_upcase) as $t
     | select($n >= 17 or ($n == 0 and ($t == "ERROR" or $t == "FATAL" or $t == "CRITICAL" or $t == "SEVERE"))))] | length' "$FILE")
if [ -z "${PRODUCER_CMD:-}" ]; then
  : "${BOOTSTRAP:?set BOOTSTRAP in $ENV_FILE}"
  # KAFKA_CLIENT_CONFIG (env file) = producer settings for this environment (TLS, client certificate, SASL login).
  # It and any file it names must be inside this folder: it is mounted at /work in the container.
  KCFG="${KAFKA_CLIENT_CONFIG:-client.properties}"
  CFG=""; [ -f "$KCFG" ] && CFG="--producer.config /work/$KCFG"
  PRODUCER_CMD="docker run --rm -i ${DOCKER_ARGS:-} -v $PWD:/work confluentinc/cp-kafka:7.6.0 kafka-console-producer --bootstrap-server $BOOTSTRAP $CFG --topic $TOPIC"
fi
echo "Sending $(wc -l < "$FILE" | tr -d ' ') messages from $MODE to $TOPIC ..."
eval "$PRODUCER_CMD" < "$FILE"
echo "Done. Expected: $expected new ERROR/FATAL documents in $INDEX. Give it a minute, then run ./check.sh"
echo "Note: loading the same file again adds the documents again (each Kafka message gets a new offset, so a new id)."
