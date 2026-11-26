# OpenTelemetry error logs to Elasticsearch (ERROR and FATAL only)

OTel apps write their logs to a Kafka topic as OTLP/JSON. This pipeline keeps only the ERROR and FATAL records,
turns each one into a flat document and writes it to the Elasticsearch index `otel-logs-error` + `NAME_SUFFIX` (e.g. `otel-logs-error_dev`).

```
OTEL_TOPIC ─┐
            ├─> otel_logs_resource -> otel_logs_scope -> otel_logs_error_records -> otel_logs_error ──> sink ──> index otel-logs-error
OTEL_SAMPLE_TOPIC (optional, for tests) ─┘   1 row per service   1 row per logger   1 row per ERROR/FATAL line   flat document
```

## What you need
- Kafka with **ksqlDB** and **Kafka Connect**. Connect needs the Confluent Elasticsearch sink plugin, in a version that matches your Elasticsearch (16.x for Elasticsearch 9).
- **Elasticsearch 8.3 or newer**. Kibana is optional, for viewing.
- On the machine that runs the scripts: `bash`, `curl`, `jq`, `sed` and `awk`.
  `load-data.sh` also needs `docker`, or your own producer set in `PRODUCER_CMD`. To generate the week file, it also needs `python3`.
- Each Kafka message from OTel must be one OTLP/JSON payload, like the files in `data/`. Protobuf (`otlp_proto`) cannot be read by ksqlDB.

## Install in one go
```bash
cp .env.example .env          # fill in the URLs, logins and topic names
./setup.sh                    # checks the services, then creates everything. Safe to rerun.
./load-data.sh test           # optional: sends 2 test payloads. Expected: 4 documents
./check.sh                    # shows the streams, queries, connector, document counts and rejected documents
```
In Kibana, create a data view for the index (`otel-logs-error` + suffix, e.g. `otel-logs-error_dev`) with time field `@timestamp`.

Remove everything with `./reset.sh` (or `./reset.sh --yes`). The OTel source topic is kept.

## Environments (local, dev, staging)
Each environment has its own settings file: `.env.local`, `.env.dev`, `.env.staging` (all gitignored; start from `.env.example`).
Add `--env NAME` to any script, or set `OTEL_ENV=NAME` once in the shell. Without either, plain `.env` is used.
```bash
./setup.sh --env dev --dry-run    # plan goes to ./dry-run-dev
./setup.sh --env dev
./load-data.sh --env local test
./check.sh --env staging
./reset.sh --env local --yes
OTEL_ENV=staging ./check.sh
```
Every script prints the environment it loaded first. Give each environment its own `NAME_SUFFIX` (`_local`, `_poc` for dev,
`_staging`), so that two of them on one cluster never touch each other's topics, streams, connector or index.

## Files
| File | What it is |
|---|---|
| `env.sh` | Loads `.env.NAME` for `--env NAME` / `OTEL_ENV`, or `.env`. Used by every script |
| `names.sh` | Every name the pipeline creates, built from `NAME_SUFFIX` |
| `setup.sh` | Checks ksqlDB, Connect (and the plugin) and Elasticsearch (version). Then it creates the index template, the streams, the optional sample feed and the sink connector |
| `check.sh` | Read-only status: queries running, connector tasks, number of documents by severity and service, rejected documents |
| `load-data.sh real \| test \| week \| aspnet \| python` | Sends sample payloads to `OTEL_SAMPLE_TOPIC` and prints how many documents to expect |
| `reset.sh` | Removes what `setup.sh` created |
| `ksql/otel-logs.sql` | The ksqlDB streams |
| `gen_otel_logs.py` | Generates `data/otel-logs-week.jsonl` (see below) |
| `data/container-id.json`, `data/deployment-env.json` | Real payloads: Java and .NET. They contain only INFO records, so they produce 0 documents |
| `data/otel-logs-sample.jsonl` | The same two payloads with ERROR, FATAL, Warning and Critical records added. They produce 4 documents |
| `data/otel-logs-week.jsonl` | Generated week of logs. It is large: delete it before you ship this folder, and `load-data.sh week` makes it again |

## Created on the cluster
Every name gets `NAME_SUFFIX` from `.env` at the end. With `NAME_SUFFIX='_dev'` the names are:

| Kind | Names |
|---|---|
| ksqlDB streams (topics) | `otel_logs_raw_dev` (reads `OTEL_TOPIC`, which gets no suffix), `otel_logs_resource_dev` (`otel-logs-resource_dev`), `otel_logs_scope_dev` (`otel-logs-scope_dev`), `otel_logs_error_records_dev` (`otel-logs-error-records_dev`), `otel_logs_error_dev` (`otel-logs-error_dev`) |
| Sample feed (only if `OTEL_SAMPLE_TOPIC` is set) | stream `otel_logs_sample_raw_dev` (topic `OTEL_SAMPLE_TOPIC` + suffix, e.g. `otlp_logs_sample_dev`) and an `INSERT INTO otel_logs_resource_dev` query |
| Kafka Connect | connector `es-sink-otel-logs-error_dev` (consumer group `connect-es-sink-otel-logs-error_dev`), DLQ topic `otel-logs-error-dlq_dev` (not created when `EXISTING_CONNECTOR` is set, see below) |
| Elasticsearch | index template and index `otel-logs-error_dev`. The template matches only that exact index name |

All names are defined in one place, `names.sh`. `check.sh` and `reset.sh` act only on the names with your suffix,
so other copies on the same cluster are never touched. The suffix may contain only `a-z`, `0-9` and `_`.

### Using a sink connector you already have
Set `EXISTING_CONNECTOR` in `.env` to its name. `setup.sh` then adds the topic `otel-logs-error` + suffix to that connector's
`topics` and changes nothing else; no connector or DLQ topic of its own is created. It stops without changing anything if the
connector cannot read these documents: it must use `JsonConverter` with `value.converter.schemas.enable=false`, and
`key.ignore=true` (or list the topic in `topic.key.ignore`). It also warns if the connector has `transforms` (they may rename the
index, so the template would not apply) or writes to another Elasticsearch than `ES_URL`. If it uses `topics.regex`, the regex must
already match the topic. `reset.sh` only removes the topic from the connector's list; `check.sh` watches the connector's own DLQ.

If the `CONNECT_AUTH` login may not read the connector (HTTP 403), save its config to a file and set `EXISTING_CONNECTOR_CONFIG`
to it. The file can be the plain config (`GET /connectors/<name>/config`), `{"name": ..., "config": {...}}`, or a `.properties` file.
`setup.sh` writes the new config to `connector-config.<name>.json` (it holds the password, and `.gitignore` excludes it). If the
login may not change the connector either, setup prints the `curl` command for someone who may.

## What each stream does
1. **`otel_logs_raw`** tells ksqlDB how to read the JSON in `OTEL_TOPIC`. It does not copy data. One schema covers Java and .NET.
   Fields a payload does not have arrive as NULL. Attribute values are read as text: `intValue "1"` becomes `"1"`, and arrays become JSON text.
2. **`otel_logs_resource`**: one row per resource, that is, per service instance (`EXPLODE(resourceLogs)`).
3. **`otel_logs_scope`**: one row per scope, that is, per logger. The resource attribute list `[{key, value}]` becomes a map, e.g. `resource_attrs['service.name']`.
4. **`otel_logs_error_records`**: `FILTER` keeps only the error records, then `EXPLODE` gives one row per record.
   The filter uses `severityNumber` because the text differs by language:
   Java writes `ERROR`/`FATAL`, .NET writes `Error`/`Critical`. The numbers are 17-20 = ERROR and 21-24 = FATAL.
   `severityText` is used only when the number is missing.
5. **`otel_logs_error`**: the flat document. The sink writes it to Elasticsearch.

ksqlDB can unpack only one array per step, and cannot filter on what it unpacked in the same step. That is why there are four steps.

## Document fields
`@timestamp` (from `timeUnixNano`, or from `observedTimeUnixNano` when it is 0), `severity` (always `ERROR` or `FATAL`),
`severity_number`, `severity_text` (the original wording), `message`, `service_name`, `service_version`, `service_instance_id`,
`environment` (`deployment.environment.name`, or `deployment.environment` when that is missing), `host_name`, `container_id`, `sdk_language`,
`scope_name`, `trace_id`, `span_id`, `resource{...}` (all resource attributes) and `attributes{...}` (all record attributes, e.g. `exception.type`,
`exception.message`, `exception.stacktrace`).

`resource` and `attributes` are mapped with `"subobjects": false`. Without it, .NET's `deployment.environment` and
`deployment.environment.name` would clash, and Elasticsearch would reject every .NET document.

## Sample data
- `./load-data.sh real` sends the two real payloads. Expected result: 0 documents, because they contain only INFO.
- `./load-data.sh test` sends the test payloads. Expected result: 4 documents (2 ERROR, 2 FATAL).
- `./load-data.sh week` sends a generated week of logs from 6 services (`smartcorpu-be-java`, `payment-api`, `WorkerSDMK.Api`, `Notification.Api`, `ocrhub-konektor-jkndrive`, `ocrhub-gateway-latar`)
  in dev, staging and prod. It contains a prod database outage and a .NET crash loop, plus some unusual records on purpose: no `severityNumber`,
  `timeUnixNano` "0", and lowercase severity text. The script prints the expected count.
  To rebuild the file so it ends now: `python3 gen_otel_logs.py --out data/otel-logs-week.jsonl --days 7`.
  `--end 2026-11-20T07:00:00` sets another end time (UTC). Avoid future dates: Kibana ranges like "Last 7 days" end at now and would hide those logs.

Each load adds documents. Loading the same file twice gives every document twice, because each Kafka message gets a new offset, so a new id.

## Troubleshooting
| Symptom | Cause / fix |
|---|---|
| `Topic '...' does not exist` | Should not happen: `setup.sh` creates missing topics. Check that `OTEL_TOPIC` is right |
| `otel_logs_raw... already exists but reads '...'` | `OTEL_TOPIC` changed after an earlier setup. Run `./reset.sh`, then `./setup.sh` |
| You want another suffix | Run `./reset.sh` with the old `NAME_SUFFIX`, change it in `.env`, then run `./setup.sh` |
| HTTP 401 | The login for that service is refused. Fix the matching `*_AUTH` value in `.env` |
| `index_not_found_exception` | No ERROR/FATAL record has arrived yet. That is normal: INFO and WARN never arrive |
| `check.sh` warns that the DLQ has messages | Elasticsearch rejected some documents. Read the reason in the Connect worker log. (The DLQ topic itself is created when the connector starts, so an empty one is normal) |
