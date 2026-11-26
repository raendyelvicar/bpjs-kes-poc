-- ksqlDB pipeline for OpenTelemetry logs: keeps only ERROR and FATAL records.
--   __OTEL_TOPIC__ (OTLP/JSON, as OTel writes it to Kafka) -> otel-logs-resource__SFX__ -> otel-logs-scope__SFX__
--   -> otel-logs-error-records__SFX__ -> otel-logs-error__SFX__ (one flat document per log record; the Elasticsearch sink reads this)
-- __OTEL_TOPIC__, __PARTITIONS__, __REPLICAS__, __RAW_TOPIC_OPTS__ and __SFX__ are filled in by setup.sh
-- (__SFX__ is NAME_SUFFIX from .env, added to every stream and topic name. __RAW_TOPIC_OPTS__ adds PARTITIONS/REPLICAS only when the source topic does not exist yet, so ksqlDB creates it). Every statement is safe to rerun.
--
-- Every OTel SDK (Java, .NET, ...) sends the same OTLP envelope. The SDKs differ only in which optional fields
-- they fill in, so one schema covers all of them. Fields that are missing arrive as NULL.
-- Severity is decided by severityNumber, which is the same in every SDK:
--   17-20 = ERROR, 21-24 = FATAL. The text is not: Java says "ERROR", .NET says "Error" / "Critical".
-- severityText is used only when severityNumber is missing (0).
-- Attribute values are read as VARCHAR: numbers and booleans become text, arrays and kvlists become JSON text.

CREATE STREAM IF NOT EXISTS otel_logs_raw__SFX__ (
  `resourceLogs` ARRAY<STRUCT<
    `resource` STRUCT<
      `attributes` ARRAY<STRUCT<`key` VARCHAR, `value` STRUCT<`stringValue` VARCHAR, `intValue` VARCHAR,
        `doubleValue` VARCHAR, `boolValue` VARCHAR, `arrayValue` VARCHAR, `kvlistValue` VARCHAR>>>>,
    `scopeLogs` ARRAY<STRUCT<
      `scope` STRUCT<`name` VARCHAR, `version` VARCHAR>,
      `logRecords` ARRAY<STRUCT<
        `timeUnixNano` VARCHAR,
        `observedTimeUnixNano` VARCHAR,
        `severityNumber` INT,
        `severityText` VARCHAR,
        `body` STRUCT<`stringValue` VARCHAR, `intValue` VARCHAR, `doubleValue` VARCHAR, `boolValue` VARCHAR,
          `arrayValue` VARCHAR, `kvlistValue` VARCHAR>,
        `attributes` ARRAY<STRUCT<`key` VARCHAR, `value` STRUCT<`stringValue` VARCHAR, `intValue` VARCHAR,
          `doubleValue` VARCHAR, `boolValue` VARCHAR, `arrayValue` VARCHAR, `kvlistValue` VARCHAR>>>,
        `traceId` VARCHAR,
        `spanId` VARCHAR>>>>>>
) WITH (KAFKA_TOPIC = '__OTEL_TOPIC__', VALUE_FORMAT = 'JSON'__RAW_TOPIC_OPTS__);

-- Stage 1: one row per resource (one service instance).
CREATE STREAM IF NOT EXISTS otel_logs_resource__SFX__
  WITH (KAFKA_TOPIC = 'otel-logs-resource__SFX__', VALUE_FORMAT = 'JSON', PARTITIONS = __PARTITIONS__, REPLICAS = __REPLICAS__) AS
SELECT EXPLODE(`resourceLogs`) AS rl
FROM otel_logs_raw__SFX__
WHERE `resourceLogs` IS NOT NULL
EMIT CHANGES;

-- Stage 2: one row per scope. The resource attributes become a map, e.g. resource_attrs['service.name'].
CREATE STREAM IF NOT EXISTS otel_logs_scope__SFX__
  WITH (KAFKA_TOPIC = 'otel-logs-scope__SFX__', VALUE_FORMAT = 'JSON', PARTITIONS = __PARTITIONS__, REPLICAS = __REPLICAS__) AS
SELECT
  AS_MAP(
    TRANSFORM(rl->`resource`->`attributes`, a => a->`key`),
    TRANSFORM(rl->`resource`->`attributes`, a => COALESCE(a->`value`->`stringValue`, a->`value`->`intValue`,
      a->`value`->`doubleValue`, a->`value`->`boolValue`, a->`value`->`arrayValue`, a->`value`->`kvlistValue`))
  ) AS resource_attrs,
  EXPLODE(rl->`scopeLogs`) AS sl
FROM otel_logs_resource__SFX__
WHERE rl->`scopeLogs` IS NOT NULL
EMIT CHANGES;

-- Stage 3: one row per log record, ERROR and FATAL only. Everything else is dropped here.
CREATE STREAM IF NOT EXISTS otel_logs_error_records__SFX__
  WITH (KAFKA_TOPIC = 'otel-logs-error-records__SFX__', VALUE_FORMAT = 'JSON', PARTITIONS = __PARTITIONS__, REPLICAS = __REPLICAS__) AS
SELECT
  resource_attrs,
  sl->`scope`->`name` AS scope_name,
  EXPLODE(FILTER(sl->`logRecords`, r =>
    COALESCE(r->`severityNumber`, 0) >= 17
    OR (COALESCE(r->`severityNumber`, 0) = 0
        AND (UCASE(COALESCE(r->`severityText`, '')) = 'ERROR'
          OR UCASE(COALESCE(r->`severityText`, '')) = 'FATAL'
          OR UCASE(COALESCE(r->`severityText`, '')) = 'CRITICAL'
          OR UCASE(COALESCE(r->`severityText`, '')) = 'SEVERE'))
  )) AS rec
FROM otel_logs_scope__SFX__
WHERE sl->`logRecords` IS NOT NULL
EMIT CHANGES;

-- Stage 4: flat document. The Elasticsearch sink reads this topic.
CREATE STREAM IF NOT EXISTS otel_logs_error__SFX__
  WITH (KAFKA_TOPIC = 'otel-logs-error__SFX__', VALUE_FORMAT = 'JSON', PARTITIONS = __PARTITIONS__, REPLICAS = __REPLICAS__) AS
SELECT
  CAST(CASE WHEN rec->`timeUnixNano` IS NULL OR rec->`timeUnixNano` = '0'
            THEN rec->`observedTimeUnixNano` ELSE rec->`timeUnixNano` END AS BIGINT) / 1000000 AS `@timestamp`,
  CASE WHEN COALESCE(rec->`severityNumber`, 0) >= 21 THEN 'FATAL'
       WHEN COALESCE(rec->`severityNumber`, 0) >= 17 THEN 'ERROR'
       WHEN UCASE(COALESCE(rec->`severityText`, '')) = 'FATAL' OR UCASE(COALESCE(rec->`severityText`, '')) = 'CRITICAL' THEN 'FATAL'
       ELSE 'ERROR' END AS `severity`,
  rec->`severityNumber` AS `severity_number`,
  rec->`severityText` AS `severity_text`,
  COALESCE(rec->`body`->`stringValue`, rec->`body`->`kvlistValue`, rec->`body`->`arrayValue`,
           rec->`body`->`intValue`, rec->`body`->`doubleValue`, rec->`body`->`boolValue`) AS `message`,
  resource_attrs['service.name'] AS `service_name`,
  resource_attrs['service.version'] AS `service_version`,
  resource_attrs['service.instance.id'] AS `service_instance_id`,
  COALESCE(resource_attrs['deployment.environment.name'], resource_attrs['deployment.environment']) AS `environment`,
  resource_attrs['host.name'] AS `host_name`,
  resource_attrs['container.id'] AS `container_id`,
  resource_attrs['telemetry.sdk.language'] AS `sdk_language`,
  scope_name AS `scope_name`,
  rec->`traceId` AS `trace_id`,
  rec->`spanId` AS `span_id`,
  -- Dots in resource/attribute keys become underscores (deployment.environment.name -> deployment_environment_name).
  -- Elasticsearch reads dots as nesting, so "deployment.environment" next to "deployment.environment.name" would be
  -- a field and an object at once and the document is rejected. With underscores no index template is needed.
  TRANSFORM(resource_attrs, (k, v) => REPLACE(k, '.', '_'), (k, v) => v) AS `resource`,
  AS_MAP(
    TRANSFORM(rec->`attributes`, a => REPLACE(a->`key`, '.', '_')),
    TRANSFORM(rec->`attributes`, a => COALESCE(a->`value`->`stringValue`, a->`value`->`intValue`,
      a->`value`->`doubleValue`, a->`value`->`boolValue`, a->`value`->`arrayValue`, a->`value`->`kvlistValue`))
  ) AS `attributes`
FROM otel_logs_error_records__SFX__
EMIT CHANGES;
