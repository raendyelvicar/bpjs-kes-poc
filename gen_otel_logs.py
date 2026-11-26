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
The .NET services also send ASP.NET Core request messages, like a real one: "Request starting" + "Request finished"
(scope Microsoft.AspNetCore.Hosting.Diagnostics) with the full attribute set ({OriginalFormat}, empty ContentType /
ContentLength values, ElapsedMilliseconds as a JSON number, ...). Some requests fail with 500 and an ERROR record from
Microsoft.AspNetCore.Diagnostics.ExceptionHandlerMiddleware in between. --aspnet-share sets how many .NET messages are requests.
The Python services (ocrhub-*) send messages like the real ones: ocrhub-konektor-jkndrive logs a pytds SQL Server login
sequence (scope pytds) and one "konektor.putaran" summary (scope ocrhub_jkndrive, attribute ocrhub.hasil), all with
flags 3, LogType and code.* attributes. Some rounds fail ("konektor.galat", ERROR). ocrhub-gateway-latar logs
"latar.putaran" and, when its database is down, "latar.galat" with a ConnectionRefusedError traceback.
--services limits the output to some services, e.g. --services WorkerSDMK.Api,Notification.Api for .NET only.
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
p.add_argument("--services", default="", help="comma-separated service names to keep (default: all)")
p.add_argument("--aspnet-share", type=float, default=0.6, help="share of .NET messages that are ASP.NET Core request pairs (0-1)")
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
    ("ocrhub-konektor-jkndrive", "python", "0.1.0", ["pytds", "ocrhub_jkndrive"]),
    ("ocrhub-gateway-latar", "python", "0.1.0", ["ocrhub_gateway.latar"]),
]
# Python SDK resource, in the order the real payload has it (no host.name / container.id).
PY_TPL = {"attributes": [{"key": k, "value": {"stringValue": v}} for k, v in [
    ("telemetry.sdk.language", "python"), ("telemetry.sdk.name", "opentelemetry"), ("telemetry.sdk.version", "1.45.2"),
    ("service.instance.id", ""), ("service.name", ""), ("service.version", ""),
    ("deployment.environment", ""), ("deployment.environment.name", "")]]}
if a.services:
    keep = {x.strip() for x in a.services.split(",") if x.strip()}
    unknown = keep - {s[0] for s in SERVICES}
    if unknown: p.error(f"unknown service(s): {', '.join(sorted(unknown))}. Known: {', '.join(s[0] for s in SERVICES)}")
    SERVICES = [s for s in SERVICES if s[0] in keep]
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
def dv(k, v): return {"key": k, "value": {"doubleValue": v}}       # a JSON number, as the .NET SDK sends it
def ev(k): return {"key": k, "value": {}}                            # empty value: the .NET SDK sends null this way
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
        res = copy.deepcopy({"java": JAVA_TPL, "dotnet": NET_TPL, "python": PY_TPL}[sdk])
        pod = f"{name.lower().replace('.', '-')}-{hexid(5)[:9]}-{hexid(3)[:5]}"
        inst_id = hexid(6) if sdk == "python" else str(uuid.UUID(int=random.getrandbits(128)))   # python: short container id
        set_ = {"service.name": name, "service.version": version, "service.instance.id": inst_id,
                "deployment.environment": env, "deployment.environment.name": env, "host.name": pod, "container.id": hexid(32)}
        for attr in res["attributes"]:
            if attr["key"] in set_ and "stringValue" in attr["value"]:
                attr["value"]["stringValue"] = set_[attr["key"]]
        self.resource = res
        self.resource_id = inst_id
        self.host = f"10.42.{random.randint(0, 9)}.{random.randint(2, 250)}:5000"   # pod address, used by ASP.NET request logs


ASPNET_PATHS = {"WorkerSDMK.Api": [("GET", "/healthz")] * 6 + [("POST", "/api/jobs/run"), ("GET", "/api/jobs/status")],
                "Notification.Api": [("GET", "/healthz")] * 6 + [("POST", "/api/sms"), ("POST", "/api/email")]}
START_FMT = "Request starting {Protocol} {Method} {Scheme}://{Host}{PathBase}{Path}{QueryString} - {ContentType} {ContentLength}"
END_FMT = ("Request finished {Protocol} {Method} {Scheme}://{Host}{PathBase}{Path}{QueryString} - {StatusCode} {ContentLength} "
           "{ContentType} {ElapsedMilliseconds}ms")
UNHANDLED = "An unhandled exception has occurred while executing the request."


def conn_id():
    return "0HN" + "".join(random.choice("0123456789ABCDEFGHIJKLMNOPQRSTUV") for _ in range(10))


def aspnet_request(inst, t, fail):
    """One ASP.NET Core request as the .NET SDK logs it: start + (error) + finish, all in one trace."""
    method, path = random.choice(ASPNET_PATHS.get(inst.name, [("GET", "/healthz")]))
    trace, span, conn = hexid(16), hexid(8), conn_id()
    host = inst.host
    status = 500 if fail else (202 if method == "POST" else 200)
    ms = round(random.uniform(0.15, 0.6) if path == "/healthz" else random.uniform(4, 180), 4)
    if fail: ms = round(random.uniform(20, 900), 4)
    common = [sv("SpanId", span), sv("TraceId", trace), sv("ParentId", "0000000000000000"),
              sv("ConnectionId", conn), sv("RequestId", f"{conn}:00000001"), sv("RequestPath", path)]
    url = [sv("Scheme", "http"), sv("Host", host), sv("PathBase", ""), sv("Path", path), sv("QueryString", "")]
    if method == "POST":
        ctype, clen = sv("ContentType", "application/json"), iv("ContentLength", random.randint(60, 900))
        body_ct, body_cl = "application/json", clen["value"]["intValue"]
    else:
        ctype, clen, body_ct, body_cl = ev("ContentType"), ev("ContentLength"), "-", "-"
    ns = int(t.timestamp() * 1e9) + random.randint(0, 999_999)

    def rec(off_ns, num, text, body, attrs):
        counts["records"] += 1
        return {"timeUnixNano": str(ns + off_ns), "observedTimeUnixNano": str(ns + off_ns), "severityNumber": num,
                "severityText": text, "body": {"stringValue": body}, "attributes": attrs, "flags": 1,
                "traceId": trace, "spanId": span}

    start = rec(0, 9, "Information", f"Request starting HTTP/1.1 {method} http://{host}{path} - {body_ct} {body_cl}",
                [sv("Protocol", "HTTP/1.1"), sv("Method", method), ctype, clen] + url + [sv("{OriginalFormat}", START_FMT)] + common)
    end = rec(int(ms * 1e6), 9, "Information",
              f"Request finished HTTP/1.1 {method} http://{host}{path} - {status} - - {ms}ms",
              [dv("ElapsedMilliseconds", ms), iv("StatusCode", status), ev("ContentType"), ev("ContentLength"),
               sv("Protocol", "HTTP/1.1"), sv("Method", method)] + url + [sv("{OriginalFormat}", END_FMT)] + common)
    hosting = "Microsoft.AspNetCore.Hosting.Diagnostics"
    if not fail:   # like the real payload: one scope holding the start and finish records
        scopes = [{"scope": {"name": hosting}, "logRecords": [start, end]}]
    else:          # start, the unhandled-exception ERROR, then finish with 500, in that order
        etype, emsg, _ = random.choice(ERRORS["dotnet"])
        err = rec(int(ms * 1e6) - 1000, 17, "Error", UNHANDLED,
                  [sv("exception.type", etype), sv("exception.message", emsg), sv("exception.stacktrace", stack("dotnet", etype, inst.name)),
                   sv("{OriginalFormat}", UNHANDLED)] + common)
        counts["ERROR"] += 1
        scopes = [{"scope": {"name": hosting}, "logRecords": [start]},
                  {"scope": {"name": "Microsoft.AspNetCore.Diagnostics.ExceptionHandlerMiddleware"}, "logRecords": [err]},
                  {"scope": {"name": hosting}, "logRecords": [end]}]
    counts["messages"] += 1
    return {"resourceLogs": [{"resource": inst.resource, "scopeLogs": scopes}]}


PYTDS = "/app/.venv/lib/python3.12/site-packages/pytds/"
# (body, file, function, line) of one pytds SQL Server login, as the konektor logs it every round.
PYTDS_LOGIN = [
    ("Opening socket to {db}", "__init__.py", "_connect", 439),
    ("Performing login on the connection", "__init__.py", "_connect", 458),
    ("Sending PRELOGIN lib_ver='1000000' enc_flag='2' inst_name='MSSQLServer' mars=False", "tds_session.py", "send_prelogin", 1212),
    ("Got PRELOGIN response crypt=2 mars=0", "tds_session.py", "parse_prelogin", 1282),
    ("Sending LOGIN tds_ver=74000004 bufsz=4096 pid=1 opt1=f0 opt2=2 opt3=8 cli_tz=0 cli_lcid=1033 cli_host={host} lang= db=dbJKNDrive",
     "tds_session.py", "tds7_send_login", 1390),
    ("switched to database dbJKNDrive", "tds_session.py", "process_env_chg", 480),
    ("switched collation to Collation(lcid=1033, sort_id=52, ignore_case=True, ignore_accent=False, ignore_width=True, "
     "ignore_kana=True, binary=False, binary2=False, version=0)", "tds_session.py", "process_env_chg", 449),
    ("switched language to us_english", "tds_session.py", "process_env_chg", 485),
    ("Got LOGINACK tds_ver=74000004 srv_name=Microsoft SQL Server   srv_ver=f00119e", "tds_session.py", "process_login_tokens", 1561),
    ("Sending RPC <pytds.tds_base.InternalProc object at 0x{addr}> flags=0", "tds_session.py", "submit_rpc", 753),
]
OCRHUB_LOG = ("/app/libs/common/src/ocrhub_common/logging.py", "_emit", 113)
PY_ERRORS = {
    "ocrhub-konektor-jkndrive": [
        ("ConnectionRefusedError", "111", "[Errno 111] Connection refused", "konektor.py", 88, "putaran",
         "with pytds.connect(**cfg) as conn:", "pytds/__init__.py", 439, "_connect", "sock = socket.create_connection((host, port), timeout)"),
        ("pytds.tds_base.LoginError", "Login failed for user 'ocrhub'.", "pytds.tds_base.LoginError: Login failed for user 'ocrhub'.",
         "konektor.py", 88, "putaran", "with pytds.connect(**cfg) as conn:", "pytds/tds_session.py", 1561, "process_login_tokens",
         "raise tds_base.LoginError(msg)"),
        ("httpx.ReadTimeout", "timed out", "httpx.ReadTimeout: timed out", "kirim.py", 51, "kirim_vedika",
         "resp = await client.post(url, json=berkas)", "httpx/_transports/default.py", 118, "map_httpcore_exceptions", "raise mapped_exc(message) from exc"),
    ],
    "ocrhub-gateway-latar": [
        ("ConnectionRefusedError", "111", "ConnectionRefusedError: [Errno 111] Connect call failed ('172.23.5.116', 5432)", "latar.py", 74,
         "relay_outbox_sekali", "async with engine.begin() as conn:", "/usr/local/lib/python3.12/asyncio/selector_events.py", 691, "_sock_connect_cb",
         "raise OSError(err, f'Connect call failed {address}')"),
        ("sqlalchemy.exc.TimeoutError", "QueuePool limit of size 5 overflow 10 reached, connection timed out, timeout 30.00",
         "sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached, connection timed out, timeout 30.00",
         "latar.py", 74, "relay_outbox_sekali", "async with engine.begin() as conn:", "sqlalchemy/pool/impl.py", 179, "_do_get",
         "raise exc.TimeoutError("),
    ],
}


def py_attrs(code, extra=()):
    f, fn, line = code
    return list(extra) + [sv("LogType", "General"), sv("code.file.path", f), sv("code.function.name", fn), iv("code.line.number", line)]


def py_traceback(svc, e):
    etype, _, last, app_file, app_line, app_fn, app_src, lib_file, lib_line, lib_fn, lib_src = e
    svc_dir, module = {"ocrhub-konektor-jkndrive": ("konektor", "ocrhub_jkndrive"),
                       "ocrhub-gateway-latar": ("gateway", "ocrhub_gateway")}[svc]
    lib_path = lib_file if lib_file.startswith("/") else "/app/.venv/lib/python3.12/site-packages/" + lib_file
    return ("Traceback (most recent call last):\n"
            f"  File \"/app/services/{svc_dir}/src/{module}/{app_file}\", line {app_line}, in {app_fn}\n    {app_src}\n"
            f"  File \"{lib_path}\", line {lib_line}, in {lib_fn}\n    {lib_src}\n"
            f"{last if ':' in last.split(' ')[0] else etype + ': ' + last}\n")


def python_message(inst, t, fail):
    """One round of an ocrhub Python service, shaped like its real OTLP payload."""
    trace, span = hexid(16), hexid(8)
    ns = int(t.timestamp() * 1e9) + random.randint(0, 999_999)
    clock = [ns]

    def rec(num, text, body, attrs, gap_ms=(0.2, 35)):
        clock[0] += int(random.uniform(*gap_ms) * 1e6)
        counts["records"] += 1
        return {"timeUnixNano": str(clock[0]), "observedTimeUnixNano": str(clock[0] + random.randint(150_000, 2_600_000)),
                "severityNumber": num, "severityText": text, "body": {"stringValue": body}, "attributes": attrs,
                "flags": 3, "traceId": trace, "spanId": span}

    err = random.choice(PY_ERRORS[inst.name]) if fail else None

    def error_rec(scope_body):
        e = err
        fatal = random.random() < 0.08
        counts["FATAL" if fatal else "ERROR"] += 1
        extra = [sv("exception.type", e[0]), sv("exception.message", e[1])]
        if inst.name == "ocrhub-gateway-latar":
            extra.append(sv("ocrhub.tugas", random.choice(["outbox", "outbox", "sinkron_status"])))
        extra += [sv("exception.stacktrace", py_traceback(inst.name, e)), sv("ocrhub.exc_type", e[0].split(".")[-1])]
        return rec(21 if fatal else 17, "CRITICAL" if fatal else "ERROR", scope_body, py_attrs(OCRHUB_LOG, extra))

    scopes = []
    if inst.name == "ocrhub-konektor-jkndrive":
        if not fail: steps = PYTDS_LOGIN
        elif err[0] == "ConnectionRefusedError": steps = PYTDS_LOGIN[:1]     # refused right after opening the socket
        elif "LoginError" in err[0]: steps = PYTDS_LOGIN[:8]                 # refused at LOGINACK
        else: steps = PYTDS_LOGIN                                            # database fine, the later upload failed
        pytds = [rec(9, "INFO", b.format(db="172.23.5.180:59004", host=inst.resource_id, addr=hexid(6)),
                     py_attrs((PYTDS + f, fn, line))) for b, f, fn, line in steps]
        scopes.append({"scope": {"name": "pytds"}, "logRecords": pytds})
        if fail:
            app = [error_rec("konektor.galat")]
        else:
            dibaca = random.choice([0, 0, 0, random.randint(1, 40)])
            gagal = random.randint(1, 3) if dibaca and random.random() < 0.15 else 0
            hasil = {"vedika": {"dibaca": dibaca, "dikirim": dibaca - gagal, "duplikat": random.randint(0, dibaca // 5) if dibaca else 0,
                                "dengan_data_klaim": max(0, dibaca - gagal - random.randint(0, 2)), "gagal": gagal},
                     "pcare-sigap": {"dilewati": "rute nonaktif"}}
            app = [rec(13 if gagal else 9, "WARNING" if gagal else "INFO", "konektor.putaran",
                       py_attrs(OCRHUB_LOG, [sv("ocrhub.hasil", repr(hasil))]), gap_ms=(20, 60))]
        scopes.append({"scope": {"name": "ocrhub_jkndrive"}, "logRecords": app})
    else:  # ocrhub-gateway-latar
        if fail:
            app = [error_rec("latar.galat")]
        else:
            app = [rec(9, "INFO", "latar.putaran", py_attrs(OCRHUB_LOG, [sv("ocrhub.tugas", "outbox"),
                                                                          iv("ocrhub.dikirim", random.randint(0, 25))]))]
        scopes.append({"scope": {"name": "ocrhub_gateway.latar"}, "logRecords": app})
    counts["messages"] += 1
    return {"resourceLogs": [{"resource": inst.resource, "scopeLogs": scopes}]}


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
crash_victim = next((i for i in INSTANCES if i.env == "prod" and i.name == "WorkerSDMK.Api"), None)

out = []
t = START
while t < END:
    hour_load = 0.4 if t.hour in range(15, 23) else 1.0   # 22:00-06:00 WIB is quiet
    for _ in range(max(1, int(a.per_hour * hour_load))):
        ts = t + dt.timedelta(seconds=random.uniform(0, 3600))
        inst = random.choice(INSTANCES)
        if inst.sdk == "python":
            out.append((ts, python_message(inst, ts, fail=random.random() < (0.05 if inst.env == "prod" else 0.10))))
        elif inst.sdk == "dotnet" and random.random() < a.aspnet_share:
            out.append((ts, aspnet_request(inst, ts, fail=random.random() < (0.04 if inst.env == "prod" else 0.08))))
        else:
            out.append((ts, message(inst, ts, [normal_sev(inst.env) for _ in range(random.randint(3, 15))])))
    t += dt.timedelta(hours=1)

ts = db_start
while ts < db_end:                                   # database outage: every 2 minutes, mostly errors
    for inst in db_victims:
        sevs = [ERROR] * random.randint(3, 8) + [WARN] * random.randint(0, 3) + ([FATAL] if random.random() < 0.15 else [])
        out.append((ts, message(inst, ts, sevs, "db")))
    ts += dt.timedelta(minutes=2)

ts = crash_start
while crash_victim and ts < crash_end:                                # crash loop: restart every ~90 s
    out.append((ts, message(crash_victim, ts, [INFO, INFO, ERROR, FATAL], "crash")))
    ts += dt.timedelta(seconds=random.randint(60, 120))

out.sort(key=lambda x: x[0])
os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
with open(a.out, "w") as f:
    for _, m in out:
        f.write(json.dumps(m, separators=(",", ":")) + "\n")

print(f"Wrote {counts['messages']} messages / {counts['records']} log records to {a.out} ({START:%Y-%m-%d %H:%M} to {END:%Y-%m-%d %H:%M} UTC)")
print(f"Expected in Elasticsearch: {counts['ERROR'] + counts['FATAL']} documents ({counts['ERROR']} ERROR, {counts['FATAL']} FATAL)")
