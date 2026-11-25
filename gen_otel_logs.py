#!/usr/bin/env python3
"""Generate synthetic OpenTelemetry log payloads (OTLP/JSON), one Kafka message per line (stdlib only).

Usage: python3 gen_otel_logs.py --out data/otel-logs-week.jsonl [--days 7] [--per-hour 12] [--end 2026-11-25T07:00:00]

--end (UTC) defaults to now, so the logs end at the moment you generate them. A future --end gives future logs,
which Kibana hides in ranges like "Last 7 days"; the script warns about it.

Each message looks like data/container-id.json (Java) or data/deployment-env.json (.NET):
the resource attributes of those files are reused as templates, with service, host, container and environment changed.
Records are mostly INFO / DEBUG / WARN, with ERROR and FATAL mixed in. There are two incidents:
  - 2 days before --end, 01:00-02:30 UTC: database outage (payment-api and smartcorpu-be-java log many ERRORs, some FATAL)
  - 1 day before --end, 09:00-09:40 UTC: worker crash loop (WorkerSDMK.Api logs Critical on every restart)
Some edge cases are mixed in on purpose: no severityNumber (text only), timeUnixNano "0", lowercase severity text.
The expected document count in Elasticsearch is printed at the end.
"""
import argparse, copy, datetime as dt, json, os, random, uuid

p = argparse.ArgumentParser()
p.add_argument("--out", default="data/otel-logs-week.jsonl")
p.add_argument("--days", type=int, default=7)
p.add_argument("--per-hour", type=int, default=12, help="messages per hour (each holds 3-15 records)")
p.add_argument("--end", default=None, help="UTC, e.g. 2026-11-25T07:00:00 (default: now)")
p.add_argument("--seed", type=int, default=7)
a = p.parse_args()
random.seed(a.seed)
UTC = dt.timezone.utc
NOW = dt.datetime.now(UTC).replace(second=0, microsecond=0)
END = dt.datetime.fromisoformat(a.end).replace(tzinfo=UTC) if a.end else NOW
if END > NOW + dt.timedelta(minutes=5):
    print(f"WARNING: --end {END:%Y-%m-%d %H:%M} UTC is in the future: these logs will not show in Kibana ranges that end at 'now'.")
START = END - dt.timedelta(days=a.days)
HERE = os.path.dirname(os.path.abspath(__file__))

JAVA_TPL = json.load(open(os.path.join(HERE, "data/container-id.json")))["resourceLogs"][0]["resource"]
NET_TPL = json.load(open(os.path.join(HERE, "data/deployment-env.json")))["resourceLogs"][0]["resource"]

# name, sdk, version, scopes
SERVICES = [
    ("smartcorpu-be-java", "java", "1.0.0", ["app.bpjs.smartcorpu.enhance.LoggingAdvice", "app.bpjs.smartcorpu.service.ClaimService",
                                              "org.hibernate.engine.jdbc.spi.SqlExceptionHelper"]),
    ("payment-api", "java", "2.3.1", ["com.corp.payment.PaymentController", "com.zaxxer.hikari.pool.HikariPool",
                                      "org.springframework.web.servlet.DispatcherServlet"]),
    ("WorkerSDMK.Api", "dotnet", "1.4.0", ["Microsoft.AspNetCore.Hosting.Diagnostics", "WorkerSDMK.Api.Jobs.SyncJob",
                                            "Microsoft.Extensions.Hosting.Internal.Host"]),
    ("Notification.Api", "dotnet", "3.0.2", ["Microsoft.AspNetCore.Hosting.Diagnostics", "Notification.Api.Services.SmsSender",
                                              "Microsoft.EntityFrameworkCore.Database.Command"]),
]
ENVS = ["dev", "dev", "staging", "prod", "prod", "prod"]

# (severityNumber, java text, dotnet text)
INFO, DEBUG, WARN, ERROR, FATAL = (9, "INFO", "Information"), (5, "DEBUG", "Debug"), (13, "WARN", "Warning"), \
                                  (17, "ERROR", "Error"), (21, "FATAL", "Critical")

ERRORS = {
    "java": [
        ("java.net.SocketTimeoutException", "Read timed out", "Failed to call core banking: Read timed out"),
        ("org.springframework.dao.DataAccessResourceFailureException", "Unable to acquire JDBC Connection",
         "Could not open JPA EntityManager for transaction"),
        ("java.lang.NullPointerException", "Cannot invoke \"String.length()\" because \"nik\" is null",
         "Unhandled exception in ClaimService.submit"),
        ("com.fasterxml.jackson.core.JsonParseException", "Unexpected character ('<' (code 60))",
         "Invalid response from upstream /api/peserta"),
        ("org.springframework.web.client.HttpServerErrorException$BadGateway", "502 Bad Gateway",
         "Upstream returned 502 for POST /api/claim"),
    ],
    "dotnet": [
        ("System.NullReferenceException", "Object reference not set to an instance of an object.",
         "An unhandled exception has occurred while executing the request."),
        ("System.TimeoutException", "The operation has timed out.", "SyncJob failed after 3 retries"),
        ("Microsoft.Data.SqlClient.SqlException", "A network-related or instance-specific error occurred while establishing a connection to SQL Server.",
         "Failed executing DbCommand"),
        ("System.Net.Http.HttpRequestException", "Connection refused (sms-gateway:443)", "Failed to send SMS notification"),
        ("System.InvalidOperationException", "Sequence contains no elements", "Error processing job batch"),
    ],
}
FATALS = {
    "java": [("org.springframework.beans.factory.BeanCreationException", "Error creating bean with name 'dataSource'",
              "Application run failed"),
             ("java.lang.OutOfMemoryError", "Java heap space", "Uncaught error in thread main, shutting down")],
    "dotnet": [("System.InvalidOperationException", "Unable to resolve service for type 'IJobRepository'",
                "Host terminated unexpectedly"),
               ("System.OutOfMemoryException", "Insufficient memory to continue the execution of the program.",
                "Fatal error, process will exit")],
}
DB_DOWN = {
    "java": ("com.zaxxer.hikari.pool.HikariPool$PoolInitializationException",
             "Failed to initialize pool: Connection to db-prod:5432 refused", "HikariPool-1 - Connection is not available, request timed out after 30000ms"),
    "dotnet": ("Microsoft.Data.SqlClient.SqlException", "Login failed: server db-prod is not accessible",
               "Failed executing DbCommand (30,012ms)"),
}
INFO_MSG = {
    "java": ["[null] - GET /mobile-rest-smartcorpu/api/health\nResponse : {{\"status\":\"UP\"}}", "Claim {id} submitted",
             "Payment {id} authorised", "Token refreshed for client mobile-app", "Scheduled job ReconcileJob finished in 412ms"],
    "dotnet": ["Request starting HTTP/1.1 GET http://10.42.4.84:5000/healthz - - -",
               "Request finished HTTP/1.1 GET http://10.42.4.84:5000/healthz - 200 - - 0.1842ms",
               "SyncJob processed {n} rows", "SMS queued for delivery", "Executed DbCommand (3ms)"],
}
WARN_MSG = ["Slow request {n}ms", "Retrying upstream call (attempt 2/3)", "Connection pool usage above 80%",
            "Deprecated endpoint /v1/claim called"]


def sv(k, v): return {"key": k, "value": {"stringValue": v}}
def iv(k, v): return {"key": k, "value": {"intValue": str(v)}}
def hexid(n): return "%0*x" % (n * 2, random.getrandbits(n * 8))


def stack(sdk, etype, svc):
    if sdk == "java":
        pkg = svc.replace("-", ".")
        return (f"{etype}: ...\n\tat app.{pkg}.service.Handler.process(Handler.java:{random.randint(40, 300)})\n"
                f"\tat app.{pkg}.web.Controller.handle(Controller.java:{random.randint(20, 120)})\n"
                "\tat org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:897)")
    return (f"{etype}: ...\n   at {svc}.Services.Handler.ProcessAsync() in /src/Services/Handler.cs:line {random.randint(20, 300)}\n"
            f"   at {svc}.Controllers.JobController.Run() in /src/Controllers/JobController.cs:line {random.randint(10, 90)}")


class Instance:
    """One running pod of a service: fixed resource attributes."""
    def __init__(self, name, sdk, version, scopes, env):
        self.name, self.sdk, self.scopes, self.env = name, sdk, scopes, env
        res = copy.deepcopy(JAVA_TPL if sdk == "java" else NET_TPL)
        pod = f"{name.lower().replace('.', '-')}-{hexid(5)[:9]}-{hexid(3)[:5]}"
        set_ = {"service.name": name, "service.version": version, "service.instance.id": str(uuid.UUID(int=random.getrandbits(128))),
                "deployment.environment": env, "deployment.environment.name": env, "host.name": pod, "container.id": hexid(32)}
        for attr in res["attributes"]:
            if attr["key"] in set_ and "stringValue" in attr["value"]:
                attr["value"]["stringValue"] = set_[attr["key"]]
        self.resource = res


INSTANCES = [Instance(n, s, v, sc, e) for (n, s, v, sc) in SERVICES for e in sorted(set(ENVS))]
counts = {"messages": 0, "records": 0, "ERROR": 0, "FATAL": 0}


def record(inst, t, sev, kind=None):
    """kind: None (normal), 'db' (database outage), 'crash' (crash loop)."""
    num, jtext, ntext = sev
    text = jtext if inst.sdk == "java" else ntext
    ns = int(t.timestamp() * 1e9) + random.randint(0, 999_999)
    r = {"timeUnixNano": str(ns), "observedTimeUnixNano": str(ns + random.randint(1000, 900_000)),
         "severityNumber": num, "severityText": text, "body": {}, "flags": 1,
         "traceId": hexid(16), "spanId": hexid(8)}
    attrs = []
    if num >= 17:
        if kind == "db":
            etype, emsg, body = DB_DOWN[inst.sdk]
        elif kind == "crash" or num >= 21:
            etype, emsg, body = random.choice(FATALS[inst.sdk])
        else:
            etype, emsg, body = random.choice(ERRORS[inst.sdk])
        attrs = [sv("exception.type", etype), sv("exception.message", emsg), sv("exception.stacktrace", stack(inst.sdk, etype, inst.name))]
        if inst.sdk == "dotnet":
            attrs += [iv("StatusCode", 500), sv("RequestPath", random.choice(["/api/jobs/run", "/api/sms", "/api/claim"]))]
        r["body"]["stringValue"] = body
        counts["FATAL" if num >= 21 else "ERROR"] += 1
    elif num == 13:
        r["body"]["stringValue"] = random.choice(WARN_MSG).format(n=random.randint(800, 5000))
    else:
        r["body"]["stringValue"] = random.choice(INFO_MSG[inst.sdk]).format(id=random.randint(10**6, 10**7), n=random.randint(1, 900))
        if inst.sdk == "dotnet":
            attrs = [sv("Protocol", "HTTP/1.1"), sv("Method", "GET"), sv("Path", "/healthz")]
    if attrs:
        r["attributes"] = attrs
    # Edge cases the pipeline must handle.
    roll = random.random()
    if num >= 17 and roll < 0.03:
        del r["severityNumber"]                      # text only: still kept via severityText
    elif roll < 0.05:
        r["timeUnixNano"] = "0"                     # falls back to observedTimeUnixNano
    elif num >= 17 and roll < 0.08:
        r["severityText"] = r["severityText"].lower()  # still kept: the number decides
    counts["records"] += 1
    return r


def message(inst, t, sevs, kind=None):
    by_scope = {}
    for i, sev in enumerate(sevs):
        scope = random.choice(inst.scopes)
        by_scope.setdefault(scope, []).append(record(inst, t + dt.timedelta(milliseconds=i * random.randint(1, 400)), sev, kind))
    counts["messages"] += 1
    rl = {"resource": inst.resource,
          "scopeLogs": [{"scope": {"name": s}, "logRecords": recs} for s, recs in by_scope.items()]}
    if inst.sdk == "java":
        rl["schemaUrl"] = "https://opentelemetry.io/schemas/1.24.0"
    return {"resourceLogs": [rl]}


def normal_sev(env):
    r = random.random()
    err = 0.06 if env == "prod" else 0.10            # dev is noisier
    if r < 0.003: return FATAL
    if r < 0.003 + err: return ERROR
    if r < 0.12 + err: return WARN
    if r < 0.30 + err: return DEBUG
    return INFO


day0 = END.replace(hour=0, minute=0, second=0, microsecond=0)
db_start, db_end = day0 - dt.timedelta(days=2) + dt.timedelta(hours=1), day0 - dt.timedelta(days=2) + dt.timedelta(hours=2, minutes=30)
crash_start, crash_end = day0 - dt.timedelta(days=1) + dt.timedelta(hours=9), day0 - dt.timedelta(days=1) + dt.timedelta(hours=9, minutes=40)
db_victims = [i for i in INSTANCES if i.env == "prod" and i.name in ("payment-api", "smartcorpu-be-java")]
crash_victim = next(i for i in INSTANCES if i.env == "prod" and i.name == "WorkerSDMK.Api")

out = []
t = START
while t < END:
    hour_load = 0.4 if t.hour in range(15, 23) else 1.0   # 22:00-06:00 WIB is quiet
    for _ in range(max(1, int(a.per_hour * hour_load))):
        ts = t + dt.timedelta(seconds=random.uniform(0, 3600))
        inst = random.choice(INSTANCES)
        out.append((ts, message(inst, ts, [normal_sev(inst.env) for _ in range(random.randint(3, 15))])))
    t += dt.timedelta(hours=1)

ts = db_start
while ts < db_end:                                   # database outage: every 2 minutes, mostly errors
    for inst in db_victims:
        sevs = [ERROR] * random.randint(3, 8) + [WARN] * random.randint(0, 3) + ([FATAL] if random.random() < 0.15 else [])
        out.append((ts, message(inst, ts, sevs, "db")))
    ts += dt.timedelta(minutes=2)

ts = crash_start
while ts < crash_end:                                # crash loop: restart every ~90 s
    out.append((ts, message(crash_victim, ts, [INFO, INFO, ERROR, FATAL], "crash")))
    ts += dt.timedelta(seconds=random.randint(60, 120))

out.sort(key=lambda x: x[0])
os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
with open(a.out, "w") as f:
    for _, m in out:
        f.write(json.dumps(m, separators=(",", ":")) + "\n")

print(f"Wrote {counts['messages']} messages / {counts['records']} log records to {a.out} ({START:%Y-%m-%d %H:%M} to {END:%Y-%m-%d %H:%M} UTC)")
print(f"Expected in Elasticsearch: {counts['ERROR'] + counts['FATAL']} documents ({counts['ERROR']} ERROR, {counts['FATAL']} FATAL)")
