"""
GAM AdX Sync Bot — production-ready single-file version

Runs every SYNC_INTERVAL minutes:
  1. Auto-fetch all Child Publishers from GAM Ad Manager 360 Parent (MCM)
     and upsert into network_codes table
  2. Fetch fresh AdX report for each ACTIVE network code via a two-phase
     pipeline (submit all report jobs concurrently, poll in parallel, then
     download concurrently)
  3. Upsert rows into adx_daily_stats
  4. Delete rows older than RETENTION_DAYS
  5. Log errors to adx_sync_errors

Transport (USE_REST_API, hardcoded):
  True  = Google's JSON REST API (admanager.googleapis.com/v1) for child
          publishers, reports and mcm earnings — pure JSON, faster.
          Automatic per-call fallback to the legacy SOAP/XML API on failure.
  False = legacy SOAP/XML only.

Non-active publishers (pending, invited, inactive, suspended) are tracked
in the DB but skipped for report generation until they become ACTIVE.

Usage:
  python sync.py                    # run in loop
  python sync.py --once             # single sync, no loop
  python sync.py --interval 15      # custom interval in minutes

Requires .env file with:
  GAM_CLIENT_EMAIL, GAM_PRIVATE_KEY,
  GAM_NETWORK_CODE                  # parent MCM network code (for auto-fetch)
  BIGQUERY_PROJECT_ID               # required — BigQuery is the sole storage
  BIGQUERY_DATASET                  # default "ma_data"
  GOOGLE_APPLICATION_CREDENTIALS    # path to a BigQuery service-account JSON
  BIGQUERY_CREDENTIALS_JSON         # inline service-account JSON (alternative)

Storage: Google BigQuery only (via BQAdapter in bigquery_store.py).
Supabase is NOT used for bot data in any mode.
"""

import os
import re
import sys
import json
import time
import random
import gzip
import logging
import signal
import socket
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport import requests as gauth_requests

try:
    from bigquery_store import BigQueryStore, BQAdapter, TABLES
except ModuleNotFoundError:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    try:
        from bigquery_store import BigQueryStore, BQAdapter, TABLES
    except ModuleNotFoundError:
        TABLES: dict = {}
        class BigQueryStore:  # type: ignore[override]
            def __init__(self, enabled=None):
                self.enabled = False

            @property
            def client(self):
                return None

            def ensure_tables(self):
                return False

            def load(self, _table, _rows):
                return 0

        class BQAdapter:  # type: ignore[override]
            def __init__(self, store):
                self._store = store

# Legacy name kept only for function type hints — storage is BigQuery-only.
SupabaseClient = Any

load_dotenv()

# ── Configuration ───────────────────────────────────────────────

GAM_CLIENT_EMAIL = os.getenv("GAM_CLIENT_EMAIL")
GAM_PRIVATE_KEY = os.getenv("GAM_PRIVATE_KEY")
GAM_NETWORK_CODE = os.getenv("GAM_NETWORK_CODE", "").strip()

SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "13"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))
SUBMIT_CONCURRENCY = int(os.getenv("SUBMIT_CONCURRENCY", "16"))
DOWNLOAD_CONCURRENCY = int(os.getenv("DOWNLOAD_CONCURRENCY", "24"))
POLL_CONCURRENCY = int(os.getenv("POLL_CONCURRENCY", "16"))
REPORT_POLL_INTERVAL = int(os.getenv("REPORT_POLL_INTERVAL", "3"))
REPORT_POLL_TIMEOUT = int(os.getenv("REPORT_POLL_TIMEOUT", "300"))
# Adaptive per-job poll schedule (seconds): first status check after
# POLL_FIRST_DELAY (GAM report jobs never finish in the first seconds —
# checking earlier only burns API quota), then re-checks wait POLL_MIN_INTERVAL
# growing ×1.7 per round up to POLL_MAX_INTERVAL. Rate-limited checks back off
# ×2 up to 4×POLL_MAX_INTERVAL instead of failing the job.
POLL_FIRST_DELAY = float(os.getenv("POLL_FIRST_DELAY", "8"))
POLL_MIN_INTERVAL = float(os.getenv("POLL_MIN_INTERVAL", "5"))
POLL_MAX_INTERVAL = float(os.getenv("POLL_MAX_INTERVAL", "15"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "60"))
ERROR_RETENTION_HOURS = int(os.getenv("ERROR_RETENTION_HOURS", "24"))
LOG_DIR = os.getenv("LOG_DIR", "logs")
MCM_EARNINGS_MONTHS = int(os.getenv("MCM_EARNINGS_MONTHS", "3"))
# First-sync floor: -1 = disabled (fetch from join_date), 0 = only today,
# N = only the last N+1 days. Keeps a fresh dataset from backfilling history.
INITIAL_SYNC_DAYS = int(os.getenv("INITIAL_SYNC_DAYS", "-1"))
GAM_VERSION = "v202602"

# Bound every NEW TCP socket (GAM REST/SOAP, BigQuery, Supabase): a silently
# dead connection otherwise hangs HTTP submission forever — job.result(timeouts)
# in bigquery_store only help once submission returned. Existing pooled sockets
# keep their settings; every request here is short (BQ job polling = many small
# calls, never one >60s response).
socket.setdefaulttimeout(float(os.getenv("SOCKET_TIMEOUT_SECONDS", "60")))

# ── Transport: GAM JSON REST API (v1, beta) vs legacy SOAP/XML ──
# True  = new JSON REST endpoints (admanager.googleapis.com/v1) — faster,
#         pure JSON, automatic per-call fallback to SOAP on any failure.
# False = legacy SOAP/XML only (ads.google.com Publisher API).
# Hardcoded on purpose: this repo is public — behaviour switches live in code,
# only deployment secrets stay in .env.
REST_API_BASE = "https://admanager.googleapis.com/v1"
REST_SCOPE = "https://www.googleapis.com/auth/admanager"
USE_REST_API = True


def env_flag(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() not in ("0", "false", "no", "off")


PREJOIN_CLEANUP_ENABLED = False
RETENTION_CLEANUP_ENABLED = False

# BigQuery is the SOLE storage backend for bot data (adblobex.ma_data).
# BIGQUERY_ENABLED is a safety valve only — with it false the bot has nowhere
# to write and validate_config() will refuse to start.
BIGQUERY_ENABLED = os.getenv("BIGQUERY_ENABLED", "true").strip().lower() not in (
    "0", "false", "no", "off",
)
bq = BigQueryStore(enabled=BIGQUERY_ENABLED)

# Deferred write buffer. BigQuery rate-limits table-update operations
# (~10 per 10 seconds per table), so writing per network code takes ~80 min of
# pure throttling for 313 codes. Rows are accumulated during the cycle (from
# worker threads — hence the lock) and flushed once per table at the end of
# the cycle: ~6 table-update ops instead of ~1,500.
_BQ_BATCHES: dict[str, list[dict]] = {}
_BQ_BATCH_LOCK = threading.Lock()


def add_to_bq_batch(table: str, rows: list[dict]):
    if not rows:
        return
    with _BQ_BATCH_LOCK:
        _BQ_BATCHES.setdefault(table, []).extend(rows)


def _dedupe_bq_rows(table: str, rows: list[dict]) -> list[dict]:
    """Last-write-wins dedupe on the table's primary key."""
    pk = (TABLES.get(table) or {}).get("primary_key") or []
    if not pk or len(rows) < 2:
        return rows
    seen: dict[tuple, dict] = {}
    for r in rows:
        seen[tuple(str(r.get(c)) for c in pk)] = r
    return list(seen.values())


def flush_all_bq_batches():
    """Write every buffered row to BigQuery — one delete+load per table.

    Tables flush in parallel: BigQuery's table-update rate limits are
    per-table (see the batch-buffer note above), the shared client is
    thread-safe, per-table upsert locks keep same-table writes serialized,
    and _load_semaphore still caps global load jobs — so the big geo flush
    no longer runs behind daily/os/fill one after another."""
    with _BQ_BATCH_LOCK:
        batches = {t: rows for t, rows in _BQ_BATCHES.items() if rows}
        _BQ_BATCHES.clear()
    if not batches:
        return

    def _flush_one(table: str, rows: list[dict]) -> None:
        rows = _dedupe_bq_rows(table, rows)
        # Start marker: without it a hung table is indistinguishable from one
        # that simply hasn't logged yet (see 10/15 and 10/16 geo flush hangs).
        log.info("bigquery: flushing %d %s row(s) ...", len(rows), table)
        written = 0
        for i in range(0, len(rows), 5000):
            written += bq.sync_upsert(table, rows[i:i + 5000]) or 0
        log.info("bigquery: flushed %d/%d %s row(s)", written, len(rows), table)

    errors: list[Exception] = []
    with ThreadPoolExecutor(max_workers=min(len(batches), 4)) as executor:
        futures = [executor.submit(_flush_one, t, r) for t, r in batches.items()]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                errors.append(e)  # let the other tables finish regardless
    if errors:
        raise errors[0]


# ── Logging Setup ───────────────────────────────────────────────

os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"sync_{datetime.now().strftime('%Y%m%d')}.log")

# Windows consoles default to cp1252 which cannot encode the box-drawing
# characters used in log banners — replace unmappable chars instead of crashing.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(errors="replace")
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
log = logging.getLogger("gam_sync")

# ── Shutdown Flag ───────────────────────────────────────────────

shutdown_flag = threading.Event()


def handle_signal(signum, frame):
    log.warning("Received signal %s — shutting down gracefully...", signum)
    shutdown_flag.set()


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ═════════════════════════════════════════════════════════════════
#  GAM HELPERS
# ═════════════════════════════════════════════════════════════════


def sanitize_private_key(raw: str) -> str:
    key = raw.strip()
    key = key.replace("\\\\n", "\n")
    key = key.replace("\\n", "\n")
    key = key.replace("\r\n", "\n").replace("\r", "\n")
    key = key.replace('^"|"$', "").strip()
    if not key.endswith("\n"):
        key += "\n"
    return key


def get_access_token(client_email: str, private_key_pem: str) -> str:
    key = sanitize_private_key(private_key_pem)
    creds = service_account.Credentials.from_service_account_info(
        {
            "client_email": client_email.strip(),
            "private_key": key,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        # One token for both transports: SOAP needs .../dfp, the JSON REST
        # API needs .../admanager — a service-account token may carry both.
        scopes=["https://www.googleapis.com/auth/dfp", REST_SCOPE],
    )
    creds.refresh(gauth_requests.Request())
    if not creds.token:
        raise RuntimeError("Failed to obtain GAM access token")
    return creds.token


def build_envelope(network_code: str, body: str) -> str:
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
  xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <soapenv:Header>
    <ns1:RequestHeader
      soapenv:actor="http://schemas.xmlsoap.org/soap/actor/next"
      soapenv:mustUnderstand="0"
      xmlns:ns1="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <ns1:networkCode>{network_code}</ns1:networkCode>
      <ns1:applicationName>AdGlobeX</ns1:applicationName>
    </ns1:RequestHeader>
  </soapenv:Header>
  <soapenv:Body>{body}</soapenv:Body>
</soapenv:Envelope>'''


# Connection-pooled HTTP sessions (one per thread) so SOAP calls and report
# downloads reuse keep-alive connections instead of opening a new TCP/TLS
# handshake for every request (~1000+ per cycle).
_http_local = threading.local()

# Supabase's PostgREST client used httpx HTTP/2 connections that were NOT
# thread-safe, so every worker thread used to get its own client. Storage is
# now BigQuery-only: BigQueryStore serialises per-table writes with locks and
# the BigQuery client is thread-safe, so one shared BQAdapter serves all threads.


def _supabase_client() -> "BQAdapter":
    """Return the BigQuery storage adapter (shared, thread-safe)."""
    return BQAdapter(bq)


def _http_session() -> requests.Session:
    session = getattr(_http_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=16, pool_maxsize=64, max_retries=0
        )
        session.mount("https://", adapter)
        _http_local.session = session
    return session


def soap_call(network_code: str, service: str, body: str, token: str) -> str:
    url = f"https://ads.google.com/apis/ads/publisher/{GAM_VERSION}/{service}"
    envelope = build_envelope(network_code, body)
    try:
        resp = _http_session().post(
            url,
            data=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml;charset=UTF-8",
                "SOAPAction": '""',
                "Authorization": f"Bearer {token}",
            },
            timeout=60,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"GAM SOAP {service} HTTP {resp.status_code}: {resp.text[:500]}")
        return resp.text
    except requests.RequestException as e:
        raise RuntimeError(f"GAM SOAP {service} connection error: {e}")
    except Exception as e:
        raise RuntimeError(f"GAM SOAP {service} error: {e}")


def extract_text(xml: str, tag: str) -> Optional[str]:
    m = re.search(
        rf"<(?:[a-z0-9_]+:)?{tag}[^>]*>([\s\S]*?)</(?:[a-z0-9_]+:)?{tag}>",
        xml,
        re.IGNORECASE,
    )
    return m.group(1).strip() if m else None


def date_to_gam_xml(date_str: str, tag_name: str) -> str:
    parts = date_str.split("-")
    return f"<{tag_name}><year>{parts[0]}</year><month>{int(parts[1])}</month><day>{int(parts[2])}</day></{tag_name}>"


def parse_adx_csv(csv_text: str) -> list[dict]:
    import csv as _csv
    rows_raw = [r for r in _csv.reader(csv_text.splitlines()) if any(c.strip() for c in r)]
    if len(rows_raw) < 2:
        return []

    headers = [h.replace('"', "").strip().lower() for h in rows_raw[0]]

    def idx(keywords: list[str]) -> int:
        for i, h in enumerate(headers):
            for kw in keywords:
                if kw in h:
                    return i
        return -1

    date_idx = idx(["date"])
    revenue_idx = idx(["revenue"])
    ecpm_idx = idx(["ecpm"])
    imp_idx = idx(["impression"])
    clk_idx = idx(["clicks"])
    ctr_idx = idx(["ctr"])
    platform_idx = idx(["app"])
    os_idx = idx(["device"])

    rows: list[dict] = []
    for cols in rows_raw[1:]:
        cols = [c.replace('"', "").strip() for c in cols]
        if len(cols) < 4:
            continue
        try:
            revenue = (float(cols[revenue_idx]) if revenue_idx >= 0 else 0) / 1_000_000
            ecpm = (float(cols[ecpm_idx]) if ecpm_idx >= 0 else 0) / 1_000_000
            impressions = int(float(cols[imp_idx])) if imp_idx >= 0 else 0
            clicks = int(float(cols[clk_idx])) if clk_idx >= 0 else 0
            ctr = (float(cols[ctr_idx]) if ctr_idx >= 0 else 0) / 100.0
        except (ValueError, IndexError):
            continue
        rows.append({
            "date": cols[date_idx] if date_idx >= 0 else "",
            "platform": cols[platform_idx] if platform_idx >= 0 else "Unknown",
            "os": cols[os_idx] if os_idx >= 0 else "Unknown",
            "revenue": round(revenue, 6),
            "ecpm": round(ecpm, 6),
            "impressions": impressions,
            "clicks": clicks,
            "ctr": round(ctr, 6),
        })
    return rows


def aggregate_daily_rows(rows: list[dict]) -> list[dict]:
    """Collapse report rows (now per date + app + OS) back to per date + app so the
    existing adx_daily_stats schema and unique key (network_code,date,platform) stay
    unchanged — the revenue/impression/clicks sums are identical."""
    agg: dict[tuple[str, str], list[float]] = {}
    for r in rows:
        key = (r["date"], r["platform"])
        entry = agg.setdefault(key, [0.0, 0.0, 0.0])
        entry[0] += float(r.get("revenue") or 0)
        entry[1] += float(r.get("impressions") or 0)
        entry[2] += float(r.get("clicks") or 0)
    out: list[dict] = []
    for (date, platform), (rev, imp, clk) in agg.items():
        out.append({
            "date": date,
            "platform": platform,
            "revenue": rev,
            "impressions": int(imp),
            "clicks": int(clk),
            "ecpm": round((rev / imp) * 1000, 6) if imp else 0,
            "ctr": round(clk / imp, 6) if imp else 0,
        })
    return out


def aggregate_os_rows(rows: list[dict]) -> list[dict]:
    """Per date + OS impression counts for the iOS-share breakdown (adx_os_stats)."""
    agg: dict[tuple[str, str], int] = {}
    for r in rows:
        key = (r["date"], r.get("os") or "Unknown")
        agg[key] = agg.get(key, 0) + int(float(r.get("impressions") or 0))
    return [
        {"date": date, "os": os, "impressions": imp}
        for (date, os), imp in sorted(agg.items())
    ]


def is_ios_os(os: str) -> bool:
    s = (os or "").strip().lower()
    return any(k in s for k in ("iphone", "ipad", "ipod", "ios", "apple"))


def build_report_query(start_date: str, end_date: str) -> str:
    return f'''
    <runReportJob xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJob>
        <reportQuery>
          <dimensions>DATE</dimensions>
          <dimensions>MOBILE_APP_NAME</dimensions>
          <dimensions>MOBILE_DEVICE_NAME</dimensions>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_CTR</columns>
          {date_to_gam_xml(start_date, "startDate")}
          {date_to_gam_xml(end_date, "endDate")}
          <dateRangeType>CUSTOM_DATE</dateRangeType>
          <reportCurrency>USD</reportCurrency>
        </reportQuery>
      </reportJob>
    </runReportJob>'''


def soap_call_with_retry(network_code: str, service: str, body: str, token: str, attempts: int = 3) -> str:
    """SOAP call that retries transient GAM failures (server errors HTTP 500 /
    ServerError.SERVER_ERROR, and connection drops like "Server disconnected" /
    ConnectionTerminated) with exponential backoff before giving up."""
    delay = 2.0
    for i in range(attempts):
        try:
            return soap_call(network_code, service, body, token)
        except RuntimeError as e:
            msg = str(e)
            if i >= attempts - 1:
                raise
            lowered = msg.lower()
            if (
                "server_error" in lowered
                or "connection" in lowered
                or "disconnect" in lowered
                or "terminat" in lowered
                or "aborted" in lowered
                or "reset" in lowered
                or "timed out" in lowered
                or "timeout" in lowered
                or "http 500" in lowered
                or "http 502" in lowered
                or "http 503" in lowered
                or "http 429" in lowered
            ):
                time.sleep(delay)
                delay *= 2
                continue
            raise


def submit_report_job(network_code: str, start_date: str, end_date: str, token: str, kind: str = "app") -> str:
    """Phase A — submit one report job (kind: 'app' = per-app/OS; 'geo' =
    country/carrier/browser; 'fill' = ad requests/fill rate per app).
    Returns the GAM job id.

    With USE_REST_API, submits via the JSON REST API and returns the run
    operation name (contains "/", which downstream dispatchers use to tell
    REST jobs from SOAP numeric job ids). App falls back to SOAP on any
    failure; geo is REST-only (no SOAP geo query)."""
    if USE_REST_API:
        try:
            return rest_submit_report(network_code, start_date, end_date, token, kind)
        except Exception as e:
            if kind in ("geo", "fill"):
                raise  # no SOAP equivalent — caller skips this kind for this code
            log.warning(
                "REST submit failed for %s (%s) — falling back to SOAP",
                network_code, str(e)[:200],
            )
    if kind in ("geo", "fill"):
        raise RuntimeError(f"{kind} reports require USE_REST_API")
    log.debug("Submitting report job for %s [%s → %s]", network_code, start_date, end_date)
    run_resp = soap_call_with_retry(network_code, "ReportService", build_report_query(start_date, end_date), token)
    job_id = extract_text(run_resp, "id")
    if not job_id:
        raise RuntimeError(f"Could not extract report job ID for {network_code}")
    return job_id


def submit_report_job_staggered(network_code: str, start_date: str, end_date: str, token: str, kind: str = "app") -> str:
    """Phase A worker — staggers the initial connection opens by a small random
    jitter so concurrent workers don't hit GAM with a simultaneous burst of new
    keep-alive connections (which GAM can terminate, dropping many in-flight jobs)."""
    time.sleep(random.uniform(0, 0.5))
    return submit_report_job(network_code, start_date, end_date, token, kind)


def get_report_job_status(network_code: str, job_id: str, token: str) -> str:
    if "/" in job_id:  # REST run-operation name
        return rest_report_status(network_code, job_id, token)
    status_body = f'''
    <getReportJobStatus xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJobId>{job_id}</reportJobId>
    </getReportJobStatus>'''
    status_resp = soap_call_with_retry(network_code, "ReportService", status_body, token)
    rval = extract_text(status_resp, "rval")
    return rval if rval else "IN_PROGRESS"


def download_report_rows(network_code: str, job_id: str, token: str, kind: str = "app") -> list[dict]:
    """Phase C — resolve the download URL and parse the gzip CSV into rows.
    Retries transient connection drops during the file download."""
    if "/" in job_id:  # REST run-operation name
        return rest_download_report_rows(network_code, job_id, token, kind)
    url_body = f'''
    <getReportDownloadURL xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJobId>{job_id}</reportJobId>
      <exportFormat>CSV_DUMP</exportFormat>
    </getReportDownloadURL>'''
    url_resp = soap_call_with_retry(network_code, "ReportService", url_body, token)
    download_url = extract_text(url_resp, "rval")
    if not download_url:
        raise RuntimeError(f"Could not get download URL for {network_code}")
    download_url = download_url.replace("&amp;", "&")

    delay = 2.0
    raw = None
    for i in range(3):
        try:
            dl_resp = _http_session().get(download_url, timeout=120)
            dl_resp.raise_for_status()
            raw = dl_resp.content
            break
        except Exception as e:
            if i >= 2:
                raise RuntimeError(f"GAM report download failed for {network_code}: {e}")
            time.sleep(delay)
            delay *= 2

    try:
        csv_text = gzip.decompress(raw).decode("utf-8")
    except (gzip.BadGzipFile, OSError):
        csv_text = raw.decode("utf-8")

    rows = parse_adx_csv(csv_text)
    log.info("Fetched %d rows for %s", len(rows), network_code)
    return rows


def fetch_adx_report(network_code: str, start_date: str, end_date: str, token: str) -> list[dict]:
    """Single-code convenience path: submit → poll → download (used for retries)."""
    job_id = submit_report_job(network_code, start_date, end_date, token)
    status = "IN_PROGRESS"
    for _ in range(60):
        time.sleep(2)
        status = get_report_job_status(network_code, job_id, token)
        if status in ("COMPLETED", "FAILED"):
            break
    if status == "FAILED":
        raise RuntimeError(f"Report job {job_id} failed for {network_code}")
    if status != "COMPLETED":
        raise RuntimeError(f"Report job {job_id} timed out for {network_code}")
    return download_report_rows(network_code, job_id, token)


# ═════════════════════════════════════════════════════════════════
#  GAM REST (JSON) — new admanager.googleapis.com/v1 API
# ═════════════════════════════════════════════════════════════════


def _is_rate_limited(exc: BaseException) -> bool:
    """True when an exception came from GAM throttling us (HTTP 429) rather
    than a real failure.

    Matches only the "HTTP 429" status format rest_call/soap_call build —
    deliberately NOT free-text "Too many requests", which can also appear in
    a run operation's terminal error payload (a done-with-error operation is
    a real failure; re-polling it can never succeed)."""
    return "HTTP 429" in str(exc)


def _retry_after_seconds(resp) -> Optional[float]:
    """Parse a Retry-After header (delta-seconds form), capped at 60s so a
    hostile/absurd value can't park a worker for the whole cycle."""
    raw = (resp.headers.get("Retry-After") or "").strip()
    try:
        return max(1.0, min(float(raw), 60.0))
    except ValueError:
        return None


def rest_call(
    method: str,
    path: str,
    token: str,
    params: Optional[dict] = None,
    body: Optional[dict] = None,
    attempts: Optional[int] = None,
) -> dict:
    """Call the Ad Manager JSON REST API with retry on 5xx/429/connection
    drops (mirrors soap_call_with_retry). `path` is a resource name like
    "networks/123/childPublishers" relative to REST_API_BASE.

    429 handling decides cycle length: GAM answers "Retry later", so retries
    honor Retry-After when present and otherwise back off exponentially with
    jitter (2s → 45s, ±30%, ≤60s per sleep) instead of the old 2s/4s schedule
    that gave up after 6s — that early give-up cost ~24 network codes of data
    per cycle. GETs (status polls, fetchRows paging) get more attempts than
    mutating calls. Retries log one INFO line, not the raw error body.
    """
    url = path if path.startswith("http") else f"{REST_API_BASE}/{path.lstrip('/')}"
    if attempts is None:
        attempts = 6 if method == "GET" else 4
    delay = 2.0
    last: Exception = RuntimeError("not attempted")
    for i in range(attempts):
        sleep_for = delay
        try:
            resp = _http_session().request(
                method,
                url,
                params=params,
                json=body,
                headers={"Authorization": f"Bearer {token}"},
                timeout=90,
            )
        except requests.RequestException as e:
            last = e
        else:
            if resp.status_code < 400:
                return resp.json() if resp.content else {}
            last = RuntimeError(
                f"GAM REST {method} {path} HTTP {resp.status_code}: {resp.text[:300]}"
            )
            if resp.status_code < 500 and resp.status_code != 429:
                raise last  # client error — retrying won't help
            if resp.status_code == 429:
                retry_after = _retry_after_seconds(resp)
                if retry_after is not None:
                    sleep_for = max(sleep_for, retry_after)
                if i == 0:
                    # One line per throttled call — not one per retry, or a
                    # 429 storm drowns the log (the old path dumped the raw
                    # multi-line error body once per exhausted call).
                    log.info(
                        "GAM rate limited (429) on %s %s — backing off (attempt %d/%d)",
                        method, path, i + 1, attempts,
                    )
        if i < attempts - 1:
            time.sleep(min(sleep_for * random.uniform(0.7, 1.3), 60.0))
            delay = min(delay * 2, 45.0)
    raise RuntimeError(f"GAM REST {method} {path} failed: {last}")


def rest_fetch_child_publishers(parent_code: str, token: str) -> list[dict]:
    """MCM children via the JSON childPublishers list endpoint. One paginated
    call returns everything the SOAP path needed two calls for (CompanyService
    + child_publisher PQL): status, delegation type, email, approved revshare.
    Returns the same dict shape as fetch_child_publishers, plus inline
    approval_status / revenue_share_millipercent / email keys."""
    children: list[dict] = []
    page_token = None
    while True:
        params: dict = {"pageSize": 1000}
        if page_token:
            params["pageToken"] = page_token
        data = rest_call("GET", f"networks/{parent_code}/childPublishers", token, params=params)
        for c in data.get("childPublishers") or []:
            child_net = (c.get("childNetwork") or "").split("/")[-1]
            if not child_net:
                continue
            status = (c.get("accountStatus") or "").upper()
            inv = (c.get("invitationStatus") or "").upper()
            is_active = status == "APPROVED" and inv == "ACCEPTED"
            try:
                rev = int(c.get("approvedManageAccountRevenueShareMillipercent") or 0)
            except (TypeError, ValueError):
                rev = 0
            children.append({
                "network_code": child_net,
                "name": (c.get("displayName") or "").strip(),
                "seller_id": c.get("sellerId") or "",
                "child_publisher_id": (c.get("name") or "").split("/")[-1],
                "last_modified_at": None,  # REST list has no modified timestamp
                "account_status": "ACTIVE" if is_active else status,
                "invitation_status": inv,
                "delegation_type": c.get("delegationType") or "",
                "approval_status": c.get("approvalStatus") or "",
                "revenue_share_millipercent": rev,
                "email": (c.get("email") or "").strip(),
            })
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return children


# Reusable per-code report resources. The REST API has no reports.delete, so
# each child network gets ONE hidden report that is patched + re-run every
# cycle instead of creating ~34k new report objects per day. App + geo are
# separate report objects (different dimensions) keyed "code" / "code:geo".
_MA_REPORT_DISPLAY_NAME = "MA Bot AdX sync"
_MA_GEO_REPORT_DISPLAY_NAME = "MA Bot AdX geo sync"
_MA_FILL_REPORT_DISPLAY_NAME = "MA Bot AdX fill sync"
_rest_report_cache: dict[str, str] = {}
_rest_report_lock = threading.Lock()


def _rest_date_obj(date_str: str) -> dict:
    y, m, d = date_str.split("-")
    return {"year": int(y), "month": int(m), "day": int(d)}


def _rest_report_definition(start_date: str, end_date: str) -> dict:
    # Probed live 2026-10-09: DATE + MOBILE_APP_NAME + DEVICE_NAME with the
    # AD_EXCHANGE_* metrics is accepted and matches the old SOAP query.
    return {
        "dimensions": ["DATE", "MOBILE_APP_NAME", "DEVICE_NAME"],
        "metrics": [
            "AD_EXCHANGE_REVENUE",
            "AD_EXCHANGE_AVERAGE_ECPM",
            "AD_EXCHANGE_IMPRESSIONS",
            "AD_EXCHANGE_CLICKS",
            "AD_EXCHANGE_CTR",
        ],
        "reportType": "HISTORICAL",
        "currencyCode": "USD",
        "dateRange": {
            "fixed": {
                "startDate": _rest_date_obj(start_date),
                "endDate": _rest_date_obj(end_date),
            }
        },
    }


def _rest_geo_report_definition(start_date: str, end_date: str) -> dict:
    # Probed live 2026-10-10: COUNTRY_NAME + CARRIER_NAME + BROWSER_NAME is the
    # valid dimension set (MOBILE_CARRIER is rejected). Separate report so the
    # per-app report's row count stays small.
    return {
        "dimensions": ["DATE", "COUNTRY_NAME", "CARRIER_NAME", "BROWSER_NAME"],
        "metrics": [
            "AD_EXCHANGE_REVENUE",
            "AD_EXCHANGE_AVERAGE_ECPM",
            "AD_EXCHANGE_IMPRESSIONS",
            "AD_EXCHANGE_CLICKS",
            "AD_EXCHANGE_CTR",
        ],
        "reportType": "HISTORICAL",
        "currencyCode": "USD",
        "dateRange": {
            "fixed": {
                "startDate": _rest_date_obj(start_date),
                "endDate": _rest_date_obj(end_date),
            }
        },
    }


def _rest_fill_report_definition(start_date: str, end_date: str) -> dict:
    # Probed live 2026-10-14: DEVICE_NAME is rejected together with
    # AD_REQUESTS/FILL_RATE (REPORT_ERROR_CONSTRAINTS_INCOMPATIBILITY), so the
    # fill metrics need a third report: DATE + MOBILE_APP_NAME only (accepted).
    # AD_EXCHANGE_* metrics in the same definition are also rejected.
    return {
        "dimensions": ["DATE", "MOBILE_APP_NAME"],
        "metrics": ["AD_REQUESTS", "FILL_RATE"],
        "reportType": "HISTORICAL",
        "currencyCode": "USD",
        "dateRange": {
            "fixed": {
                "startDate": _rest_date_obj(start_date),
                "endDate": _rest_date_obj(end_date),
            }
        },
    }


_REST_KIND_DISPLAY = {
    "app": _MA_REPORT_DISPLAY_NAME,
    "geo": _MA_GEO_REPORT_DISPLAY_NAME,
    "fill": _MA_FILL_REPORT_DISPLAY_NAME,
}
_REST_KIND_DEFN = {
    "app": _rest_report_definition,
    "geo": _rest_geo_report_definition,
    "fill": _rest_fill_report_definition,
}


def _rest_report_cache_key(network_code: str, kind: str) -> str:
    return network_code if kind == "app" else f"{network_code}:{kind}"


def _rest_get_or_create_report(network_code: str, token: str, kind: str = "app") -> str:
    """Return the reusable report resource name for a code, creating it on
    first use (and after restarts, by looking it up by displayName)."""
    cache_key = _rest_report_cache_key(network_code, kind)
    display_name = _REST_KIND_DISPLAY[kind]
    defn_fn = _REST_KIND_DEFN[kind]
    with _rest_report_lock:
        name = _rest_report_cache.get(cache_key)
    if name:
        return name
    try:
        data = rest_call(
            "GET", f"networks/{network_code}/reports", token,
            params={"filter": f'displayName="{display_name}"', "pageSize": 10},
        )
        for r in data.get("reports") or []:
            name = r.get("name")
            if name:
                with _rest_report_lock:
                    _rest_report_cache[cache_key] = name
                return name
    except Exception:
        pass  # list/filter unsupported — fall through to create
    # Placeholder window — patched to the real range before every run.
    # GAM rejects start dates >3 years ago, so use a recent 1-day window.
    _ph = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    created = rest_call("POST", f"networks/{network_code}/reports", token, body={
        "displayName": display_name,
        "visibility": "HIDDEN",
        "reportDefinition": defn_fn(_ph, _ph),
    })
    name = created.get("name")
    if not name:
        raise RuntimeError(f"REST report create returned no name for {network_code}")
    with _rest_report_lock:
        _rest_report_cache[cache_key] = name
    return name


def rest_submit_report(network_code: str, start_date: str, end_date: str, token: str, kind: str = "app") -> str:
    """Phase A (REST) — patch the reusable report to this window and start a
    run. Returns the operation resource name (used as the job id)."""
    report_name = _rest_get_or_create_report(network_code, token, kind)
    definition = _REST_KIND_DEFN[kind](start_date, end_date)
    try:
        rest_call("PATCH", report_name, token,
                  params={"updateMask": "reportDefinition"},
                  body={"reportDefinition": definition})
    except Exception as e:
        if _is_rate_limited(e):
            raise  # throttled, not stale — pointless to churn the report cache
        # Stale/deleted report — recreate once and retry.
        log.warning("REST report patch failed for %s (%s) — recreating", network_code, str(e)[:200])
        with _rest_report_lock:
            _rest_report_cache.pop(_rest_report_cache_key(network_code, kind), None)
        report_name = _rest_get_or_create_report(network_code, token, kind)
        rest_call("PATCH", report_name, token,
                  params={"updateMask": "reportDefinition"},
                  body={"reportDefinition": definition})
    op = rest_call("POST", f"{report_name}:run", token)
    op_name = op.get("name")
    if not op_name:
        raise RuntimeError(f"REST report run returned no operation for {network_code}")
    return op_name


def rest_report_status(network_code: str, op_name: str, token: str) -> str:
    """Phase B (REST) — poll the run operation."""
    op = rest_call("GET", op_name, token)
    if not op.get("done"):
        return "IN_PROGRESS"
    if op.get("error"):
        err = op["error"]
        raise RuntimeError(
            f"REST report run failed for {network_code}: {err.get('code')} {err.get('message', '')}"
        )
    return "COMPLETED"


def _rest_value(v: Any) -> str:
    """Unwrap a REST ReportValue ({stringValue|intValue|doubleValue|...})."""
    if not isinstance(v, dict):
        return "" if v is None else str(v)
    for k in ("stringValue", "intValue", "doubleValue", "boolValue"):
        if k in v and v[k] is not None and v[k] != "":
            return str(v[k])
    return ""


def _parse_rest_report_rows(raw_rows: list[dict]) -> list[dict]:
    """Convert fetchRows JSON into the same row shape parse_adx_csv produces.

    Probed live 2026-10-09 — key differences vs the SOAP CSV dump:
      * DATE comes back as intValue "YYYYMMDD" (not a date string)
      * revenue / eCPM are already plain dollars (CSV had micros — no /1e6)
      * CTR is already a fraction (CSV had percent — no /100)
      * empty MOBILE_APP_NAME (web inventory) → "(Not applicable)" so the
        dashboard's applicable/not-applicable revenue split keeps working
    """
    out: list[dict] = []
    for r in raw_rows:
        dims = r.get("dimensionValues") or []
        metrics = ((r.get("metricValueGroups") or [{}])[0]).get("primaryValues") or []
        raw_date = _rest_value(dims[0]) if dims else ""
        if re.fullmatch(r"\d{8}", raw_date):
            date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
            date = raw_date
        else:
            continue
        platform = (_rest_value(dims[1]).strip() if len(dims) > 1 else "") or "(Not applicable)"
        # REST labels missing device "Unknown"; SOAP CSV used "(unknown)" —
        # normalize to the SOAP value so adx_os_stats keys match history.
        os_name = (_rest_value(dims[2]).strip() if len(dims) > 2 else "") or "(unknown)"
        if os_name == "Unknown":
            os_name = "(unknown)"

        def num(i: int) -> float:
            if i >= len(metrics):
                return 0.0
            try:
                return float(_rest_value(metrics[i]) or 0)
            except ValueError:
                return 0.0

        out.append({
            "date": date,
            "platform": platform,
            "os": os_name,
            "revenue": round(num(0), 6),
            "ecpm": round(num(1), 6),
            "impressions": int(num(2)),
            "clicks": int(num(3)),
            "ctr": round(num(4), 6),
        })
    return out


def _parse_rest_geo_rows(raw_rows: list[dict]) -> list[dict]:
    """Parse the geo report (DATE, COUNTRY_NAME, CARRIER_NAME, BROWSER_NAME).
    Empty country/carrier (Wi-Fi / unidentified) → '(unknown)' / '(none)'."""
    out: list[dict] = []
    for r in raw_rows:
        dims = r.get("dimensionValues") or []
        metrics = ((r.get("metricValueGroups") or [{}])[0]).get("primaryValues") or []
        raw_date = _rest_value(dims[0]) if dims else ""
        if re.fullmatch(r"\d{8}", raw_date):
            date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
            date = raw_date
        else:
            continue
        country = (_rest_value(dims[1]).strip() if len(dims) > 1 else "") or "(unknown)"
        carrier = (_rest_value(dims[2]).strip() if len(dims) > 2 else "") or "(none)"
        browser = (_rest_value(dims[3]).strip() if len(dims) > 3 else "") or "(unknown)"

        def num(i: int) -> float:
            if i >= len(metrics):
                return 0.0
            try:
                return float(_rest_value(metrics[i]) or 0)
            except ValueError:
                return 0.0

        out.append({
            "date": date,
            "country": country,
            "carrier": carrier,
            "browser": browser,
            "revenue": round(num(0), 6),
            "ecpm": round(num(1), 6),
            "impressions": int(num(2)),
            "clicks": int(num(3)),
            "ctr": round(num(4), 6),
        })
    return out


def _parse_rest_fill_rows(raw_rows: list[dict]) -> list[dict]:
    """Parse the fill report (DATE, MOBILE_APP_NAME + AD_REQUESTS, FILL_RATE).
    FILL_RATE is a 0..1 fraction straight from GAM."""
    out: list[dict] = []
    for r in raw_rows:
        dims = r.get("dimensionValues") or []
        metrics = ((r.get("metricValueGroups") or [{}])[0]).get("primaryValues") or []
        raw_date = _rest_value(dims[0]) if dims else ""
        if re.fullmatch(r"\d{8}", raw_date):
            date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
            date = raw_date
        else:
            continue
        platform = (_rest_value(dims[1]).strip() if len(dims) > 1 else "") or "(Not applicable)"

        def num(i: int) -> float:
            if i >= len(metrics):
                return 0.0
            try:
                return float(_rest_value(metrics[i]) or 0)
            except ValueError:
                return 0.0

        out.append({
            "date": date,
            "platform": platform,
            "ad_requests": int(num(0)),
            "fill_rate": round(num(1), 6),
        })
    return out


def rest_download_report_rows(network_code: str, op_name: str, token: str, kind: str = "app") -> list[dict]:
    """Phase C (REST) — fetch the completed report's rows as JSON (paginated)."""
    op = rest_call("GET", op_name, token)
    if not op.get("done"):
        raise RuntimeError(f"REST report {op_name} not done for {network_code}")
    result_path = ((op.get("response") or {}).get("reportResult")) or ""
    if not result_path:
        raise RuntimeError(f"REST report result missing for {network_code}: {str(op)[:300]}")
    if kind == "geo":
        parse = _parse_rest_geo_rows
    elif kind == "fill":
        parse = _parse_rest_fill_rows
    else:
        parse = _parse_rest_report_rows
    rows: list[dict] = []
    page_token = None
    while True:
        params: dict = {"pageSize": 10000}
        if page_token:
            params["pageToken"] = page_token
        data = rest_call("GET", f"{result_path}:fetchRows", token, params=params)
        rows.extend(parse(data.get("rows") or []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    log.info("Fetched %d rows (REST %s) for %s", len(rows), kind, network_code)
    return rows


def rest_fetch_mcm_earnings(parent_code: str, month: str, token: str) -> list[dict]:
    """MCM monthly earnings via JSON mcmEarnings.fetch. Returns rows in the
    same shape parse_pql_result produced (lowercase PQL-style keys, micros)."""
    year, mon = month.split("-")
    out: list[dict] = []
    page_token = None

    def money_micros(m: Any) -> int:
        if not isinstance(m, dict):
            return 0
        try:
            return int(m.get("units") or 0) * 1_000_000 + int(m.get("nanos") or 0) // 1000
        except (TypeError, ValueError):
            return 0

    def money_cc(m: Any) -> str:
        return (m or {}).get("currencyCode") or "" if isinstance(m, dict) else ""

    while True:
        params: dict = {
            "month.year": int(year),
            "month.month": int(mon),
            "month.day": 1,
            "pageSize": 1000,
        }
        if page_token:
            params["pageToken"] = page_token
        data = rest_call("GET", f"networks/{parent_code}/mcmEarnings:fetch", token, params=params)
        for e in data.get("mcmEarnings") or []:
            te = e.get("totalEarnings") or {}
            pp = e.get("parentPayment") or {}
            cp = e.get("childPayment") or {}
            dd = e.get("deductions") or {}
            out.append({
                "childnetworkcode": (e.get("child") or "").split("/")[-1],
                "childname": e.get("childDisplayName") or "",
                "delegationtype": e.get("delegationType") or "",
                "totalearningsmicros": money_micros(te),
                "totalearningscurrencycode": money_cc(te),
                "parentpaymentmicros": money_micros(pp),
                "parentpaymentcurrencycode": money_cc(pp),
                "childpaymentmicros": money_micros(cp),
                "childpaymentcurrencycode": money_cc(cp),
                "deductionsmicros": money_micros(dd),
                "deductionscurrencycode": money_cc(dd),
            })
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return out


# ═════════════════════════════════════════════════════════════════
#  MCM CHILD PUBLISHER FETCH
# ═════════════════════════════════════════════════════════════════


def _extract_result_blocks(xml_str: str) -> list[dict[str, str]]:
    """
    Parse GAM SOAP response with <results> blocks (from getCompaniesByStatement),
    extracting simple child elements as key-value pairs.
    """
    cleaned = re.sub(r'\s+xmlns[^=]*="[^"]*"', "", xml_str)
    blocks: list[dict[str, str]] = []
    for rval in re.finditer(r"<results>([\s\S]*?)</results>", cleaned):
        fields = {}
        for child in re.finditer(r"<(\w+)>([^<]*)</\1>", rval.group(1)):
            fields[child.group(1)] = child.group(2).strip()
        if fields:
            blocks.append(fields)
    return blocks


def parse_pql_result(xml_str: str) -> list[dict[str, str]]:
    """
    Parse a PublisherQueryLanguageService.select SOAP response.
    Returns one dict per row, keyed by the result column labels (lowercased).
    Handles TextValue / NumberValue / DateValue cells.
    """
    def cell_text(cell: str) -> str:
        m = re.search(r"<value>([\s\S]*?)</value>", cell)
        if not m:
            return ""
        inner = m.group(1)
        y = re.search(r"<year>(\d+)</year>", inner)
        mo = re.search(r"<month>(\d+)</month>", inner)
        if y:
            return f"{int(y.group(1)):04d}-{int(mo.group(1)):02d}" if mo else y.group(1)
        return re.sub(r"<[^>]+>", "", inner).strip()

    cleaned = re.sub(r'\s+xmlns[^=]*="[^"]*"', "", xml_str)
    labels = re.findall(r"<labelName>([^<]*)</labelName>", cleaned)
    rows = []
    for block in re.finditer(r"<rows>([\s\S]*?)</rows>", cleaned):
        cells = re.findall(r"<values[^>]*>([\s\S]*?)</values>", block.group(1))
        if not cells or len(cells) != len(labels):
            continue
        row = {}
        for lbl, cell in zip(labels, cells):
            row[lbl.lower()] = cell_text(cell)
        rows.append(row)
    return rows


def fetch_child_publishers(token: str, parent_code: str) -> list[dict]:
    """
    Fetch MCM child publishers. USE_REST_API → JSON childPublishers.list
    (one paginated call; also returns inline approval/revshare/email so the
    separate PQL details pass is skipped). Falls back to the legacy SOAP
    CompanyService path on any REST failure.
    Returns list of dicts with keys:
      network_code, name, seller_id, account_status, invitation_status, delegation_type
    """
    if USE_REST_API:
        try:
            children = rest_fetch_child_publishers(parent_code, token)
            log.info("Fetched %d MCM children via REST childPublishers", len(children))
            return children
        except Exception as e:
            log.warning(
                "REST childPublishers failed for %s (%s) — falling back to SOAP",
                parent_code, str(e)[:200],
            )
    body = f'''<getCompaniesByStatement xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
    <filterStatement>
      <query>WHERE type = 'CHILD_PUBLISHER'</query>
    </filterStatement>
  </getCompaniesByStatement>'''

    try:
        resp = soap_call(parent_code, "CompanyService", body, token)
    except RuntimeError as e:
        log.warning("getCompaniesByStatement error: %s", str(e)[:300])
        return []

    companies = _extract_result_blocks(resp)
    if not companies:
        log.info("No CHILD_PUBLISHER companies found")
        return []

    # Filter to those with a valid childNetworkCode
    children = [c for c in companies if c.get("childNetworkCode")]
    log.info(
        "Fetched %d MCM children from CompanyService (total %d CHILD_PUBLISHER companies)",
        len(children), len(companies),
    )

    mapped = []
    for c in children:
        status = c.get("accountStatus", "").upper()
        inv_status = c.get("invitationStatus", "").upper()
        is_active = status == "APPROVED" and inv_status == "ACCEPTED"

        # GAM returns the Company's last modified DateTime as flat year/month/day/hour/minute/second fields.
        # For declined children this is the date the invitation was rejected / withdrawn.
        last_modified = None
        try:
            tz = c.get("timeZoneId") or "UTC"
            dt_naive = datetime(
                int(c.get("year", 0)), int(c.get("month", 1)), int(c.get("day", 1)),
                int(c.get("hour", 0)), int(c.get("minute", 0)), int(c.get("second", 0)),
            )
            last_modified = dt_naive.replace(tzinfo=ZoneInfo(tz)).astimezone(timezone.utc).isoformat()
        except Exception:
            last_modified = None

        mapped.append({
            "network_code": c.get("childNetworkCode", ""),
            "name": c.get("name", "").strip(),
            "seller_id": c.get("sellerId", ""),
            "child_publisher_id": c.get("id", ""),
            "last_modified_at": last_modified,
            "account_status": "ACTIVE" if is_active else status,
            "invitation_status": inv_status,
            "delegation_type": c.get("approvedDelegationType", ""),
        })

    return mapped


def fetch_child_publisher_details(token: str, parent_code: str) -> dict[str, dict]:
    """
    Query the child_publisher PQL view on the PARENT network for every child's
    detailed approval status + approved Manage Account revenue share.

    Returns { child_network_code: { approval_status, revenue_share_millipercent } }.
    revenue_share_millipercent is the PARENT's share in millipercent (15000 = 15%);
    only populated for approved Manage Account children.
    """
    body = f'''<select xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <selectStatement>
        <query>
          SELECT ChildNetworkCode, Email, ApprovalStatus, ApprovedManageAccountRevshareMillipercent
          FROM child_publisher
        </query>
      </selectStatement>
    </select>'''
    resp = soap_call(parent_code, "PublisherQueryLanguageService", body, token)
    rows = parse_pql_result(resp)

    details: dict[str, dict] = {}
    for r in rows:
        code = (r.get("childnetworkcode") or "").strip()
        if not code:
            continue
        rev = 0
        try:
            rev = int(float(r.get("approvedmanageaccountrevsharemillipercent") or 0))
        except (TypeError, ValueError):
            rev = 0
        details[code] = {
            "approval_status": (r.get("approvalstatus") or "").strip(),
            "revenue_share_millipercent": rev,
            "email": (r.get("email") or "").strip(),
        }
    return details


def fetch_all_network_codes(
    supabase: SupabaseClient,
    columns: str,
    page_size: int = 1000,
) -> list[dict]:
    """Fetch all network_codes rows in pages so we never hit PostgREST's 1000-row cap."""
    rows: list[dict] = []
    offset = 0
    while True:
        resp = supabase.table("network_codes").select(columns).range(offset, offset + page_size - 1).execute()
        chunk = resp.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return rows


def sync_network_codes(supabase: SupabaseClient, token: str) -> dict:
    """
    Fetch ALL MCM child publishers from GAM via CompanyService and
    upsert into network_codes table.
    Every child publisher with a childNetworkCode is stored.
    Only children with account_status='ACTIVE' are synced for reports.
    """
    if not GAM_NETWORK_CODE:
        log.info("GAM_NETWORK_CODE not set — skipping MCM child publisher sync")
        return {"status": "skipped", "reason": "no_parent_code"}

    log.info("Fetching MCM children from parent %s...", GAM_NETWORK_CODE)
    children = fetch_child_publishers(token, GAM_NETWORK_CODE)
    if not children:
        return {"status": "completed", "children_found": 0}

    # Detailed approval status + approved revshare (child_publisher PQL view).
    # REST childPublishers already returns these inline — only needed for SOAP.
    child_details: dict[str, dict] = {}
    if not (children and "approval_status" in children[0]):
        try:
            child_details = fetch_child_publisher_details(token, GAM_NETWORK_CODE)
            log.info("child_publisher details: %d children with status/revshare", len(child_details))
        except Exception as e:
            log.warning("child_publisher details fetch failed: %s", str(e)[:300])

    # Load existing codes from DB with every column the change-detection
    # compares (gracefully drop active_since if the column hasn't been
    # migrated yet, so the bot keeps running pre-migration).
    has_active_since = True
    try:
        existing_rows = fetch_all_network_codes(
            supabase,
            "network_code, label, network_name, account_status, invitation_status, "
            "delegation_type, approval_status, revenue_share_millipercent, email, "
            "seller_id, child_publisher_id, last_modified_at, active_since, "
            "declined_at, created_at, source, last_synced_at",
        )
    except Exception:
        has_active_since = False
        existing_rows = fetch_all_network_codes(
            supabase,
            "network_code, label, network_name, account_status, invitation_status, "
            "delegation_type, approval_status, revenue_share_millipercent, email, "
            "seller_id, child_publisher_id, last_modified_at, declined_at, "
            "created_at, source, last_synced_at",
        )
    existing_codes = {r["network_code"]: r for r in existing_rows}

    now_iso = datetime.now(timezone.utc).isoformat()

    # Build rows in memory; only upsert children whose GAM-sourced fields
    # actually changed. Rewriting all ~2000 rows every cycle cost ~200s+ of
    # BigQuery delete+load time and silently nulled bot-owned columns
    # (created_at/active_since/declined_at) on every pass.
    update_rows: list[dict] = []
    insert_rows: list[dict] = []
    legacy_skipped = 0
    unchanged = 0

    def _iso(v):
        if v is None:
            return None
        if hasattr(v, "isoformat"):
            return v.isoformat()
        s = str(v).strip()
        return s or None

    for child in children:
        code = child["network_code"]
        name = child.get("name", "").strip()
        dt = child.get("delegation_type", "")
        status = child.get("account_status", "INACTIVE")
        inv = child.get("invitation_status", "")
        # REST children carry details inline; SOAP children get them from PQL.
        detail = child_details.get(code) or child

        row = {
            "network_code": code,
            "label": name[:255] or code,
            "network_name": name[:255] or None,
            "account_status": status,
            "invitation_status": inv,
            "delegation_type": dt,
            "approval_status": detail.get("approval_status") or None,
            "revenue_share_millipercent": detail.get("revenue_share_millipercent") or None,
            "email": detail.get("email") or None,
            "seller_id": child.get("seller_id", "").strip() or None,
            "child_publisher_id": child.get("child_publisher_id", "").strip() or None,
            "last_modified_at": child.get("last_modified_at"),
            "source": "mcm_child",
            "updated_at": now_iso,
        }

        # Only auto-insert new records that are MANAGE_ACCOUNT children
        # (or pending invites). MANAGE_INVENTORY children are excluded —
        # MA reporting is for manage-account relationships only.
        is_user_child = (
            dt == "MANAGE_ACCOUNT"
            or (dt == "" and inv == "PENDING")
        )

        if code in existing_codes:
            prev = existing_codes[code]
            # Rejected invitations (declined by child) or withdrawn (declined by parent):
            # stamp the REAL decline date from GAM's last modified DateTime every sync
            # (idempotent — the date doesn't change). Fall back to existing/now if missing.
            if inv == "REJECTED" or inv == "WITHDRAWN":
                row["declined_at"] = child.get("last_modified_at") or prev.get("declined_at") or now_iso
            # First time we observe a child as ACTIVE: stamp the date the bot
            # discovered it. For pre-existing codes that's created_at (the day we
            # first saw them in MCM) — NEVER "now", otherwise max(start, join_date)
            # would clamp their report window to today and break T+1 catch-up.
            # If created_at is unknown (legacy wipe), leave NULL → join_date
            # falls back to the standard 7-day window.
            if (
                has_active_since
                and status == "ACTIVE"
                and not prev.get("active_since")
                and prev.get("created_at")
            ):
                row["active_since"] = _iso(prev.get("created_at"))

            # Preserve bot-owned columns the delete+load upsert would NULL.
            # created_at stays NULL when the legacy wipe cleared it — never
            # re-stamp to now (that would fake the join date and clamp the
            # report window). Inserts set it; updates only carry it forward.
            row["created_at"] = _iso(prev.get("created_at"))
            row["last_modified_at"] = (
                _iso(child.get("last_modified_at")) or _iso(prev.get("last_modified_at"))
            )
            if has_active_since and "active_since" not in row:
                row["active_since"] = _iso(prev.get("active_since"))
            if "declined_at" not in row:
                row["declined_at"] = _iso(prev.get("declined_at"))
            # last_synced_at is owned by the report pipeline (C4 / per-code
            # sync) — never touched here. Bumping it at cycle start killed the
            # first-run-of-day full-window re-pull and masked failed syncs.
            row["last_synced_at"] = _iso(prev.get("last_synced_at"))

            if _mcm_row_changed(prev, row):
                update_rows.append(row)
            else:
                unchanged += 1
        elif is_user_child:
            row["created_at"] = now_iso
            if inv in ("REJECTED", "WITHDRAWN"):
                row["declined_at"] = child.get("last_modified_at") or now_iso
            if has_active_since and status == "ACTIVE":
                row["active_since"] = now_iso
            insert_rows.append(row)
            log.info("Queued new child: %s (%s) status=%s dt=%s (last_synced_at not set — first sync pending)", code, name or "no name", status, dt)
        else:
            legacy_skipped += 1
            log.debug("Skipped legacy child not in DB: %s (%s) dt=%s", code, name or "no name", dt)

    upserted = _upsert_network_codes(supabase, insert_rows, update_rows)

    # Remove codes that are no longer managed under our MCM parent (deleted from
    # GAM or relationship ended). They'll be re-added automatically if they come
    # back. Only touches rows we auto-created (source='mcm_child').
    removed_count = _remove_orphaned_network_codes(
        supabase, existing_codes, {c["network_code"] for c in children}
    )

    log.info(
        "MCM sync complete: %d changed, %d unchanged, %d inserted (%d found, %d in DB, %d legacy skipped, %d removed)",
        len(update_rows), unchanged, len(insert_rows),
        len(children), len(existing_codes), legacy_skipped, removed_count,
    )
    return {
        "status": "completed",
        "children_found": len(children),
        "upserted": upserted,
        "unchanged": unchanged,
    }


# GAM-sourced fields that trigger a network_codes upsert when they differ.
_MCM_CMP_FIELDS = (
    "label", "network_name", "account_status", "invitation_status",
    "delegation_type", "approval_status", "revenue_share_millipercent",
    "email", "seller_id", "child_publisher_id",
    "declined_at", "active_since", "last_modified_at", "source",
)


def _mcm_row_changed(prev: dict, row: dict) -> bool:
    """True when any GAM-sourced field differs from the DB row. Timestamps are
    compared at second precision (BQ returns datetime objects; GAM strings may
    carry millisecond fractions that BQ truncates)."""

    def norm(v):
        if v is None:
            return None
        if hasattr(v, "isoformat"):
            v = v.isoformat()
        if isinstance(v, (int, float)):
            return v or None  # 0 ≡ None (millipercent stored via `or None`)
        s = str(v).strip()
        if not s:
            return None
        # "2026-10-09T14:56:13.123+00:00" → "2026-10-09T14:56:13"
        if len(s) >= 19 and s[4] == "-" and s[7] == "-" and s[10] in ("T", " "):
            s = s[:19]
        return s

    return any(norm(prev.get(f)) != norm(row.get(f)) for f in _MCM_CMP_FIELDS)


def _upsert_network_codes(
    supabase: SupabaseClient,
    insert_rows: list[dict],
    update_rows: list[dict],
    batch_size: int = 200,
) -> int:
    """Bulk-update then bulk-insert existing/new network codes, chunked so a
    single bad row can't fail the whole batch (falls back to per-row for that
    chunk, preserving the legacy error logging). Returns number upserted."""
    upserted = 0

    # Existing codes → coalescing upsert (with last_synced_at)
    for i in range(0, len(update_rows), batch_size):
        batch = update_rows[i:i + batch_size]
        try:
            resp = supabase.table("network_codes").upsert(batch, on_conflict="network_code").execute()
            upserted += len(resp.data) if resp.data else 0
        except Exception:
            for row in batch:
                try:
                    supabase.table("network_codes").update(row).eq("network_code", row["network_code"]).execute()
                    upserted += 1
                except Exception as e:
                    log.error("Failed to sync network_code %s: %s", row["network_code"], str(e))

    # New children → insert only (no last_synced_at; first report fetch pending)
    for i in range(0, len(insert_rows), batch_size):
        batch = insert_rows[i:i + batch_size]
        try:
            resp = supabase.table("network_codes").upsert(batch, on_conflict="network_code").execute()
            upserted += len(resp.data) if resp.data else 0
        except Exception:
            for row in batch:
                try:
                    supabase.table("network_codes").insert(row).execute()
                    upserted += 1
                except Exception as e:
                    err = str(e)
                    if "not-null" in err:
                        log.error(
                            "Cannot insert MCM code %s — run migration.sql first:\n"
                            "  ALTER TABLE network_codes ALTER COLUMN user_id DROP NOT NULL;\n"
                            "  ALTER TABLE network_codes ALTER COLUMN label DROP NOT NULL;",
                            row["network_code"],
                        )
                    else:
                        log.error("Failed to sync network_code %s: %s", row["network_code"], err)

    return upserted


def _remove_orphaned_network_codes(
    supabase: SupabaseClient,
    existing_codes: dict[str, dict],
    current_gam_codes: set[str],
) -> int:
    """
    Delete MCM-child codes we auto-tracked (source='mcm_child') that are no
    longer present under our parent in GAM (child removed / relationship ended).
    Cascades: adx_daily_stats, network_code_performance, adx_sync_errors, and
    the network_codes row itself. They'll be re-added if the child returns.
    """
    removed = [
        code for code, row in existing_codes.items()
        if (row.get("source") or "") == "mcm_child" and code not in current_gam_codes
    ]
    if not removed:
        return 0

    for table in ("adx_daily_stats", "adx_os_stats", "adx_geo_stats", "adx_fill_stats", "network_code_performance", "adx_sync_errors", "network_codes"):
        for i in range(0, len(removed), 100):
            chunk = removed[i:i + 100]
            try:
                supabase.table(table).delete().in_("network_code", chunk).execute()
            except Exception as e:
                log.warning("Cleanup delete failed on %s: %s", table, str(e)[:150])

    log.info("Removed %d network code(s) no longer present in GAM: %s", len(removed), removed)
    return len(removed)


# ═════════════════════════════════════════════════════════════════
#  MCM EARNINGS (parent-attributed, via mcm_earnings PQL view)
# ═════════════════════════════════════════════════════════════════

def fetch_mcm_earnings(token: str, parent_code: str, month: str) -> list[dict]:
    """
    Fetch MCM earnings for a single month from the PARENT network.
    USE_REST_API → JSON mcmEarnings.fetch; falls back to the mcm_earnings PQL
    view on failure. This is the authoritative source for how much each child
    earned WITH US (no pre-join history, no other-parent data).
    Each query MUST be scoped to exactly one month.
    """
    if USE_REST_API:
        try:
            return rest_fetch_mcm_earnings(parent_code, month, token)
        except Exception as e:
            log.warning(
                "REST mcmEarnings failed for %s %s (%s) — falling back to PQL",
                parent_code, month, str(e)[:200],
            )
    body = f'''<select xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <selectStatement>
        <query>
          SELECT ChildName, ChildNetworkCode, ChildPaymentCurrencyCode, ChildPaymentMicros,
                 DeductionsCurrencyCode, DeductionsMicros, DelegationType, Month,
                 ParentName, ParentNetworkCode, ParentPaymentCurrencyCode, ParentPaymentMicros,
                 TotalEarningsCurrencyCode, TotalEarningsMicros
          FROM mcm_earnings
          WHERE month = '{month}'
        </query>
      </selectStatement>
    </select>'''
    resp = soap_call(parent_code, "PublisherQueryLanguageService", body, token)
    return parse_pql_result(resp)


def sync_mcm_earnings(supabase: SupabaseClient, token: str) -> int:
    """Fetch MCM earnings for the last N months and upsert into mcm_earnings table."""
    if not GAM_NETWORK_CODE:
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    months = []
    now = datetime.now(timezone.utc)
    for i in range(MCM_EARNINGS_MONTHS):
        idx = now.year * 12 + (now.month - 1) - i
        months.append(f"{idx // 12:04d}-{idx % 12 + 1:02d}")

    rows = []
    for month in months:
        try:
            data = fetch_mcm_earnings(token, GAM_NETWORK_CODE, month)
        except Exception as e:
            log.warning("mcm_earnings fetch failed for %s: %s", month, str(e)[:300])
            continue
        for r in data:
            rows.append({
                "month": month,
                "child_network_code": r.get("childnetworkcode", ""),
                "child_name": r.get("childname", ""),
                "delegation_type": r.get("delegationtype", ""),
                "total_earnings_micros": int(float(r.get("totalearningsmicros") or 0)),
                "total_earnings_currency_code": r.get("totalearningscurrencycode", ""),
                "parent_payment_micros": int(float(r.get("parentpaymentmicros") or 0)),
                "parent_payment_currency_code": r.get("parentpaymentcurrencycode", ""),
                "child_payment_micros": int(float(r.get("childpaymentmicros") or 0)),
                "child_payment_currency_code": r.get("childpaymentcurrencycode", ""),
                "deductions_micros": int(float(r.get("deductionsmicros") or 0)),
                "deductions_currency_code": r.get("deductionscurrencycode", ""),
                "updated_at": now_iso,
            })
        log.info("mcm_earnings: fetched %d child rows for %s", len(data), month)

    if not rows:
        return 0

    total = 0
    try:
        for i in range(0, len(rows), 500):
            batch = rows[i:i + 500]
            resp = supabase.table("mcm_earnings").upsert(batch, on_conflict="month,child_network_code").execute()
            total += len(resp.data) if resp.data else 0
        log.info("mcm_earnings: upserted %d rows across %d months", total, len(months))
    except Exception as e:
        log.warning("mcm_earnings upsert failed (run migration.sql first?): %s", str(e)[:300])

    return total


# ═════════════════════════════════════════════════════════════════
#  TOKEN CACHE
# ═════════════════════════════════════════════════════════════════

_token_lock = threading.Lock()
_cached_token: Optional[str] = None
_token_obtained: float = 0
_TOKEN_TTL = 55 * 60


def get_gam_token_cached() -> str:
    global _cached_token, _token_obtained
    now = time.monotonic()
    with _token_lock:
        if _cached_token and (now - _token_obtained) < _TOKEN_TTL:
            return _cached_token
        log.info("Refreshing GAM access token...")
        token = get_access_token(GAM_CLIENT_EMAIL, GAM_PRIVATE_KEY)
        _cached_token = token
        _token_obtained = now
        return token


# ═════════════════════════════════════════════════════════════════
#  HELPERS
# ═════════════════════════════════════════════════════════════════

def validate_config():
    missing = []
    if not BIGQUERY_ENABLED:
        missing.append("BIGQUERY_ENABLED must not be disabled — BigQuery is the only storage")
    bq_project = os.getenv("BIGQUERY_PROJECT_ID", "").strip()
    if not bq_project:
        missing.append("BIGQUERY_PROJECT_ID (required — BigQuery is the only storage)")
    if not GAM_CLIENT_EMAIL:
        missing.append("GAM_CLIENT_EMAIL")
    if not GAM_PRIVATE_KEY:
        missing.append("GAM_PRIVATE_KEY")
    if missing:
        log.error("Missing required env vars: %s", ", ".join(missing))
        log.error("Copy .env.example to .env and fill in your credentials")
        sys.exit(1)


def get_date_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")


def to_date_str(value) -> Optional[str]:
    """Normalize a date-ish value to 'YYYY-MM-DD'.

    BigQuery rows return datetime objects while Supabase returned ISO strings,
    so call sites must not assume .split("T") works.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    if not s:
        return None
    return s.split("T")[0].split(" ")[0]


def effective_start(last_synced_at: Optional[str], default_start: str) -> str:
    """
    Returns the earlier of last_synced_at (minus 1 day buffer) or default_start.
    For new children (no last_synced_at), returns default_start.
    """
    if not last_synced_at:
        return default_start
    last_day = to_date_str(last_synced_at) or ""
    # Subtract 1 day buffer for late-reporting data
    from datetime import datetime, timedelta
    try:
        d = datetime.strptime(last_day, "%Y-%m-%d") - timedelta(days=1)
        buffer_day = d.strftime("%Y-%m-%d")
    except ValueError:
        return default_start
    return buffer_day if buffer_day < default_start else default_start


# ═════════════════════════════════════════════════════════════════
#  SYNC LOGIC
# ═════════════════════════════════════════════════════════════════

def cleanup_old_data(supabase: SupabaseClient, cutoff_date: str) -> int:
    deleted = 0
    for table in ("adx_daily_stats", "adx_os_stats", "adx_geo_stats", "adx_fill_stats"):
        try:
            resp = supabase.table(table).delete().lt("date", cutoff_date).execute()
            n = len(resp.data) if resp.data else 0
            deleted += n
            if n:
                log.info("Cleaned up %d %s row(s) older than %s", n, table, cutoff_date)
        except Exception as e:
            log.warning("Cleanup error on %s: %s", table, e)
    return deleted


def cleanup_old_sync_errors(supabase: SupabaseClient, cutoff_iso: str) -> int:
    try:
        resp = supabase.table("adx_sync_errors").delete().lt("failed_at", cutoff_iso).execute()
        deleted = len(resp.data) if resp.data else 0
        if deleted:
            log.info("Cleared %d stale sync error(s) older than %s", deleted, cutoff_iso)
        return deleted
    except Exception as e:
        log.warning("Sync error cleanup failed: %s", e)
        return 0


def cleanup_prejoin_data(supabase: SupabaseClient) -> int:
    """
    Idempotent startup cleanup: delete adx_daily_stats rows older than the
    child's active_since date (fallback created_at). These rows came from
    querying the child's own network before the parent relationship existed.

    Always uses the per-code REST loop (active_since-aware) rather than the
    cleanup_prejoin_data() Postgres function, so it stays correct even if the
    DB function was created with the old created_at-only logic.
    """
    return _cleanup_prejoin_loop(supabase)


def _cleanup_prejoin_loop(supabase: SupabaseClient) -> int:
    deleted = 0
    for entry in fetch_all_network_codes(supabase, "network_code, created_at, active_since"):
        code = entry.get("network_code", "").strip()
        boundary = entry.get("active_since") or entry.get("created_at") or ""
        join = boundary[:10]
        if not code or not join:
            continue
        try:
            dr = supabase.table("adx_daily_stats").delete().eq("network_code", code).lt("date", join).execute()
            deleted += len(dr.data) if dr.data else 0
        except Exception as e:
            log.warning("Pre-join cleanup failed for %s: %s", code, str(e)[:200])
        try:
            orr = supabase.table("adx_os_stats").delete().eq("network_code", code).lt("date", join).execute()
            deleted += len(orr.data) if orr.data else 0
        except Exception as e:
            log.warning("Pre-join os cleanup failed for %s: %s", code, str(e)[:200])
        try:
            gr = supabase.table("adx_geo_stats").delete().eq("network_code", code).lt("date", join).execute()
            deleted += len(gr.data) if gr.data else 0
        except Exception as e:
            log.warning("Pre-join geo cleanup failed for %s: %s", code, str(e)[:200])
        try:
            fr = supabase.table("adx_fill_stats").delete().eq("network_code", code).lt("date", join).execute()
            deleted += len(fr.data) if fr.data else 0
        except Exception as e:
            log.warning("Pre-join fill cleanup failed for %s: %s", code, str(e)[:200])
    if deleted:
        log.info("Pre-join cleanup removed %d rows older than their join date", deleted)
    return deleted


def record_sync_failure(supabase: SupabaseClient, network_code: str, err: BaseException, phase: str = "") -> None:
    """Log a failed sync for a code; mark as NO_ACCESS when the service account
    can't reach the child network so we don't retry it every cycle."""
    err_msg = str(err)
    log.error("Error syncing %s [%s]: %s", network_code, phase or "unknown", err_msg)

    if "NO_NETWORKS_TO_ACCESS" in err_msg:
        try:
            supabase.table("network_codes").update({
                "account_status": "NO_ACCESS",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("network_code", network_code).execute()
            log.info("Marked %s as NO_ACCESS (not a managed child)", network_code)
        except Exception:
            pass

    try:
        normalized_msg = err_msg[:500]
        existing_msg = ""
        try:
            existing = (
                supabase.table("adx_sync_errors")
                .select("error_message")
                .eq("network_code", network_code)
                .limit(1)
                .execute()
            )
            if existing.data:
                existing_msg = str((existing.data[0] or {}).get("error_message") or "")[:500]
        except Exception:
            existing_msg = ""

        # Do not rewrite the same error every cycle.
        # Keep the original failed_at so 24h hiding/cleanup can work as intended.
        if existing_msg == normalized_msg:
            return

        error_row = {
            "network_code": network_code,
            "error_message": normalized_msg,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        supabase.table("adx_sync_errors").upsert(error_row, on_conflict="network_code").execute()
    except Exception:
        pass


def upsert_report_rows(supabase: SupabaseClient, network_code: str, rows: list[dict]) -> int:
    """Upsert fetched report rows into adx_daily_stats and pre-compute the
    today / last-7-days summary for the API (no read-time aggregation)."""
    if not rows:
        supabase.table("adx_sync_errors").delete().eq("network_code", network_code).execute()
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    _dates = sorted({r["date"] for r in rows})
    if _dates:
        try:
            supabase.table("adx_daily_stats").delete().eq("network_code", network_code).gte("date", _dates[0]).lte("date", _dates[-1]).execute()
        except Exception as e:
            log.warning("pre-delete adx_daily_stats failed for %s: %s", network_code, str(e)[:200])
    to_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "platform": r["platform"],
            "revenue": r["revenue"],
            "ecpm": r["ecpm"],
            "impressions": r["impressions"],
            "clicks": r["clicks"],
            "ctr": r["ctr"],
            "updated_at": now_iso,
        }
        for r in aggregate_daily_rows(rows)
    ]

    total = 0
    for i in range(0, len(to_upsert), 500):
        batch = to_upsert[i:i + 500]
        resp = supabase.table("adx_daily_stats").upsert(batch, on_conflict="network_code,date,platform").execute()
        total += len(resp.data) if resp.data else 0

    os_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "os": r["os"],
            "impressions": r["impressions"],
            "updated_at": now_iso,
        }
        for r in aggregate_os_rows(rows)
    ]
    if _dates:
        try:
            supabase.table("adx_os_stats").delete().eq("network_code", network_code).gte("date", _dates[0]).lte("date", _dates[-1]).execute()
        except Exception as e:
            log.warning("pre-delete adx_os_stats failed for %s: %s", network_code, str(e)[:200])
    for i in range(0, len(os_upsert), 500):
        batch = os_upsert[i:i + 500]
        try:
            supabase.table("adx_os_stats").upsert(batch, on_conflict="network_code,date,os").execute()
        except Exception as e:
            log.warning("adx_os_stats upsert failed for %s: %s", network_code, str(e)[:200])

    supabase.table("adx_sync_errors").delete().eq("network_code", network_code).execute()

    # Mark last_synced_at so next cycle only fetches incremental data
    supabase.table("network_codes").update({
        "last_synced_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": now_iso,
    }).eq("network_code", network_code).execute()

    # Pre-compute today + last-7-days summary so the API never aggregates at read time
    refresh_code_performance(supabase, network_code)

    log.info("Upserted %d rows for %s", total, network_code)
    return total


def sync_single_code(network_code: str, start_date: str, end_date: str, supabase: SupabaseClient, token: str) -> dict:
    try:
        rows = fetch_adx_report(network_code, start_date, end_date, token)
        total = upsert_report_rows(supabase, network_code, rows)
        try:
            supabase.rpc("refresh_dashboard_totals").execute()
        except Exception as e:
            log.warning("refresh_dashboard_totals skipped: %s", str(e)[:200])
        return {"network_code": network_code, "rows": total}
    except Exception as e:
        record_sync_failure(supabase, network_code, e)
        return {"network_code": network_code, "error": str(e)}


def refresh_code_performance(supabase: SupabaseClient, network_code: str) -> None:
    """
    Pre-compute today + last-7-days totals (revenue / impressions / clicks) for one
    network code and upsert them into network_code_performance. The API then only
    does a plain SELECT per code — no aggregation at read time (PGRST123 disabled).
    """
    today = get_date_days_ago(0)
    week_start = get_date_days_ago(6)
    try:
        resp = supabase.table("adx_daily_stats").select(
            "date, platform, revenue, impressions, clicks"
        ).eq("network_code", network_code).gte("date", week_start).lte("date", today).execute()
        rows = resp.data or []

        t_rev = t_imp = t_clk = 0
        t_app = t_na = 0
        w_rev = w_imp = w_clk = 0
        w_app = w_na = 0
        for r in rows:
            rev = float(r.get("revenue") or 0)
            imp = int(r.get("impressions") or 0)
            clk = int(r.get("clicks") or 0)
            d = r.get("date") or ""
            is_na = (r.get("platform") or "").strip() == "(Not applicable)"
            if d == today:
                t_rev += rev
                t_imp += imp
                t_clk += clk
                if is_na:
                    t_na += rev
                else:
                    t_app += rev
            w_rev += rev
            w_imp += imp
            w_clk += clk
            if is_na:
                w_na += rev
            else:
                w_app += rev

        first_date = None
        try:
            # BigQuery-only: compute the first date directly (the old
            # Supabase view v_network_code_first_date no longer exists).
            from google.cloud import bigquery as _bql
            _sql = (
                "SELECT MIN(date) AS first_date FROM `%s` "
                "WHERE network_code = @nc" % bq.table_ref("adx_daily_stats")
            )
            _job = bq.client.query(
                _sql,
                job_config=_bql.QueryJobConfig(query_parameters=[
                    _bql.ScalarQueryParameter("nc", "STRING", network_code),
                ]),
            )
            for _row in _job.result(timeout=60):
                first_date = _row.first_date.isoformat() if _row.first_date else None
        except Exception:
            pass

        t_imp_os = w_imp_os = t_ios = w_ios = 0
        try:
            osr = supabase.table("adx_os_stats").select(
                "date, os, impressions"
            ).eq("network_code", network_code).gte("date", week_start).lte("date", today).execute()
            for o in osr.data or []:
                imp = int(o.get("impressions") or 0)
                d = o.get("date") or ""
                is_ios = is_ios_os(o.get("os") or "")
                w_imp_os += imp
                if is_ios:
                    w_ios += imp
                if d == today:
                    t_imp_os += imp
                    if is_ios:
                        t_ios += imp
        except Exception as e:
            log.warning("adx_os_stats read failed for %s: %s", network_code, str(e)[:200])


        now_iso = datetime.now(timezone.utc).isoformat()
        perf_row = {
            "network_code": network_code,
            "today_revenue": round(t_rev, 6),
            "today_impressions": t_imp,
            "today_clicks": t_clk,
            "week_revenue": round(w_rev, 6),
            "week_impressions": w_imp,
            "week_clicks": w_clk,
            "week_applicable_revenue": round(w_app, 6),
            "week_not_applicable_revenue": round(w_na, 6),
            "today_applicable_revenue": round(t_app, 6),
            "today_not_applicable_revenue": round(t_na, 6),
            "today_ios_share_pct": round((t_ios / t_imp_os) * 100, 2) if t_imp_os else 0,
            "week_ios_share_pct": round((w_ios / w_imp_os) * 100, 2) if w_imp_os else 0,
            "first_data_date": first_date,
            "last_synced_at": now_iso,
            "updated_at": now_iso,
        }
        supabase.table("network_code_performance").upsert(perf_row, on_conflict="network_code").execute()
    except Exception as e:
        log.warning("Performance summary failed for %s: %s", network_code, str(e)[:200])


def submit_all_report_jobs(codes: list[tuple[str, str]], end_date: str, supabase: SupabaseClient, token: str) -> list[dict]:
    """Phase A — submit one app job + one geo job + one fill job per ACTIVE
    code concurrently. Returns the successfully-submitted jobs; app failures
    are recorded and excluded, geo/fill failures only warn (supplementary)."""
    jobs: list[dict] = []
    with ThreadPoolExecutor(max_workers=SUBMIT_CONCURRENCY) as executor:
        futures = {}
        for code, start in codes:
            for kind in ("app", "geo", "fill"):
                futures[executor.submit(
                    submit_report_job_staggered, code, start, end_date, token, kind
                )] = (code, kind)
        # wait() loop instead of as_completed(): the 30s heartbeat makes a
        # silent stall (dead worker, system sleep) distinguishable from slow
        #-but-progressing throttled submits.
        remaining = set(futures)
        finished = 0
        next_hb = time.time() + 30
        while remaining:
            done, remaining = wait(remaining, timeout=max(next_hb - time.time(), 0.5))
            for future in done:
                code, kind = futures[future]
                try:
                    jobs.append({"network_code": code, "job_id": future.result(), "kind": kind})
                except Exception as e:
                    if kind in ("geo", "fill"):
                        log.warning("%s submit skipped for %s: %s", kind, code, str(e)[:200])
                    else:
                        record_sync_failure(supabase, code, e, "phase-a-submit")
                finished += 1
            if remaining and time.time() >= next_hb:
                log.info("Submit progress: %d/%d done, %d in flight...",
                         finished, len(futures), len(remaining))
                next_hb = time.time() + 30
    app_n = sum(1 for j in jobs if j["kind"] == "app")
    geo_n = sum(1 for j in jobs if j["kind"] == "geo")
    fill_n = sum(1 for j in jobs if j["kind"] == "fill")
    log.info("Submitted %d/%d app + %d/%d geo + %d/%d fill report jobs",
             app_n, len(codes), geo_n, len(codes), fill_n, len(codes))
    return jobs


def _poll_one(job: dict, token: str) -> tuple[str, Optional[BaseException]]:
    """Status check for one job → (status, error).

    Rate-limited exhaustion maps to ("IN_PROGRESS", err): a throttled round
    must never cost a job — the caller re-polls it after a longer backoff.
    Only genuine failures ("FAILED") are terminal."""
    try:
        return get_report_job_status(job["network_code"], job["job_id"], token), None
    except Exception as e:
        if _is_rate_limited(e):
            return "IN_PROGRESS", e
        return "FAILED", e


def poll_all_report_jobs(
    jobs: list[dict],
    supabase: SupabaseClient,
    token: str,
    on_completed=None,
) -> list[dict]:
    """Phase B — poll job status in parallel on an adaptive per-job schedule.

    Improvements over the old "re-check everything every 3s with 8 workers"
    loop (which fired ~978 GETs per round and drove the 429 storm):

      * per-job schedule: first check after POLL_FIRST_DELAY, then ×1.7 up to
        POLL_MAX_INTERVAL — a report takes 30-120s to generate, so the early
        constant re-polls were pure API waste;
      * workers = POLL_CONCURRENCY (the old code hard-capped this at 8);
      * on_completed streams every COMPLETED job straight to download so the
        download phase overlaps the poll tail instead of starting after it;
      * rate-limited checks re-poll with ×2 backoff (≤60s) instead of failing
        the job — only REPORT_POLL_TIMEOUT (or a real error) is terminal.

    Returns jobs that completed (also passed to on_completed as they do)."""
    now = time.time()
    # id(job) -> [job, next_due_at, current_wait]
    pending: dict[int, list] = {}
    for j in jobs:
        pending[id(j)] = [j, now + POLL_FIRST_DELAY, POLL_MIN_INTERVAL]
    completed: list[dict] = []
    deadline = now + REPORT_POLL_TIMEOUT
    next_wait_log = 0.0

    while pending and time.time() < deadline:
        now = time.time()
        due_items = [item for item in pending.values() if item[1] <= now]
        if not due_items:
            # Nothing due yet — sleep until the nearest job is (capped so the
            # deadline stays responsive). Plain sleep: Ctrl+C must not busy-spin
            # this loop (the flag only gates the between-cycle wait).
            next_due = min(item[1] for item in pending.values())
            time.sleep(min(max(next_due - time.time(), 0.1), REPORT_POLL_INTERVAL))
            continue

        statuses: dict[int, str] = {}
        errors: dict[int, BaseException] = {}
        workers = min(POLL_CONCURRENCY, len(due_items))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_poll_one, item[0], token): item[0]
                for item in due_items
            }
            for future in as_completed(futures):
                job = futures[future]
                try:
                    statuses[id(job)], errors[id(job)] = future.result()
                except Exception as e:  # defensive — _poll_one already traps
                    statuses[id(job)], errors[id(job)] = "FAILED", e

        for item in due_items:
            job = item[0]
            status = statuses.get(id(job), "IN_PROGRESS")
            err = errors.get(id(job))
            if status == "COMPLETED":
                completed.append(job)
                if on_completed:
                    on_completed(job)
                pending.pop(id(job), None)
            elif status == "FAILED":
                if job.get("kind") in ("geo", "fill"):
                    log.warning("%s report failed for %s: %s", job["kind"], job["network_code"], str(err)[:200])
                else:
                    record_sync_failure(supabase, job["network_code"], err, "phase-b-poll")
                pending.pop(id(job), None)
            else:
                # IN_PROGRESS or throttled — schedule the next check.
                if err is not None:
                    item[2] = min(item[2] * 2, POLL_MAX_INTERVAL * 4)
                else:
                    item[2] = min(item[2] * 1.7, POLL_MAX_INTERVAL)
                item[1] = time.time() + item[2] * random.uniform(0.85, 1.15)

        if pending and time.time() >= next_wait_log:
            log.info("Waiting for %d report job(s)...", len(pending))
            next_wait_log = time.time() + 15

    for item in pending.values():
        job = item[0]
        if job.get("kind") in ("geo", "fill"):
            log.warning("%s report timed out for %s", job["kind"], job["network_code"])
        else:
            record_sync_failure(supabase, job["network_code"], RuntimeError(f"Report job {job['job_id']} timed out"), "phase-b-timeout")
    return completed


def upsert_daily_rows(supabase: SupabaseClient, network_code: str, rows: list[dict], now_iso: str) -> int:
    """Buffer fetched report rows for adx_daily_stats (written at cycle-end flush).
    Report rows are per (date, app, OS); they're collapsed back to per (date, app)
    for adx_daily_stats, and the per-OS impression split is stored in adx_os_stats
    (used for the dashboard's iOS % column)."""
    if not rows:
        return 0

    to_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "platform": r["platform"],
            "revenue": r["revenue"],
            "ecpm": r["ecpm"],
            "impressions": r["impressions"],
            "clicks": r["clicks"],
            "ctr": r["ctr"],
            "updated_at": now_iso,
        }
        for r in aggregate_daily_rows(rows)
    ]
    total = len(to_upsert)
    os_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "os": r["os"],
            "impressions": r["impressions"],
            "updated_at": now_iso,
        }
        for r in aggregate_os_rows(rows)
    ]

    if to_upsert:
        add_to_bq_batch("adx_daily_stats", to_upsert)
        log.info("bigquery: queued %d adx_daily_stats row(s) for %s", len(to_upsert), network_code)
    if os_upsert:
        add_to_bq_batch("adx_os_stats", os_upsert)
        log.info("bigquery: queued %d adx_os_stats row(s) for %s", len(os_upsert), network_code)
    return total


def upsert_geo_rows(network_code: str, rows: list[dict], now_iso: str) -> int:
    """Buffer geo report rows (per date + country + carrier + browser) for
    adx_geo_stats (written at cycle-end flush)."""
    if not rows:
        return 0
    to_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "country": r["country"],
            "carrier": r["carrier"],
            "browser": r["browser"],
            "revenue": r["revenue"],
            "ecpm": r["ecpm"],
            "impressions": r["impressions"],
            "clicks": r["clicks"],
            "ctr": r["ctr"],
            "updated_at": now_iso,
        }
        for r in rows
    ]
    add_to_bq_batch("adx_geo_stats", to_upsert)
    log.info("bigquery: queued %d adx_geo_stats row(s) for %s", len(to_upsert), network_code)
    return len(to_upsert)


def upsert_fill_rows(network_code: str, rows: list[dict], now_iso: str) -> int:
    """Buffer fill report rows (per date + app) for adx_fill_stats
    (written at cycle-end flush)."""
    if not rows:
        return 0
    to_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "platform": r["platform"],
            "ad_requests": r["ad_requests"],
            "fill_rate": r["fill_rate"],
            "updated_at": now_iso,
        }
        for r in rows
    ]
    add_to_bq_batch("adx_fill_stats", to_upsert)
    log.info("bigquery: queued %d adx_fill_stats row(s) for %s", len(to_upsert), network_code)
    return len(to_upsert)


def refresh_performance_bulk(supabase: SupabaseClient, downloaded: list[dict], now_iso: str) -> None:
    """Compute today / last-7-days summaries from the just-fetched rows and
    batch-upsert network_code_performance.

    Every cycle re-fetches the FULL [effective_start, today] window, which always
    covers [week_start, today]. Those rows are exactly what the SELECT-based
    per-code refresh reads, so computing from memory is 100% equivalent — but with
    ~2 bulk DB calls instead of 3 per code."""
    if not downloaded:
        return
    today = get_date_days_ago(0)
    week_start = get_date_days_ago(6)  # last 7 days inclusive (matches dashboard "7 days")

    # Existing first_data_date for every code — one paginated query.
    existing: dict[str, str] = {}
    try:
        offset = 0
        while True:
            resp = supabase.table("network_code_performance").select(
                "network_code, first_data_date"
            ).range(offset, offset + 999).execute()
            chunk = resp.data or []
            for r in chunk:
                # BQ DATE columns come back as datetime.date — normalize to
                # 'YYYY-MM-DD' so the str < first comparison below can't crash.
                existing[r.get("network_code")] = to_date_str(r.get("first_data_date"))
            if len(chunk) < 1000:
                break
            offset += 1000
    except Exception as e:
        log.warning("Could not read existing performance rows: %s", str(e)[:200])

    upserts: list[dict] = []
    for d in downloaded:
        rows = d["rows"]
        if not rows:
            # Empty report: keep the previously stored summary untouched.
            continue
        t_rev = t_imp = t_clk = t_app = t_na = 0
        w_rev = w_imp = w_clk = w_app = w_na = 0
        t_imp_os = w_imp_os = t_ios = w_ios = 0
        first = existing.get(d["network_code"])
        for r in rows:
            rev = float(r.get("revenue") or 0)
            imp = int(r.get("impressions") or 0)
            clk = int(r.get("clicks") or 0)
            dt = r.get("date") or ""
            if not first or dt < first:
                first = dt
            is_na = (r.get("platform") or "").strip() == "(Not applicable)"
            is_ios = is_ios_os(r.get("os") or "")
            in_week = dt >= week_start
            if in_week:
                w_imp_os += imp
                if is_ios:
                    w_ios += imp
            if dt == today:
                t_rev += rev
                t_imp += imp
                t_clk += clk
                t_imp_os += imp
                if is_ios:
                    t_ios += imp
                if is_na:
                    t_na += rev
                else:
                    t_app += rev
            if in_week:
                w_rev += rev
                w_imp += imp
                w_clk += clk
                if is_na:
                    w_na += rev
                else:
                    w_app += rev
        upserts.append({
            "network_code": d["network_code"],
            "today_revenue": round(t_rev, 6),
            "today_impressions": t_imp,
            "today_clicks": t_clk,
            "week_revenue": round(w_rev, 6),
            "week_impressions": w_imp,
            "week_clicks": w_clk,
            "week_applicable_revenue": round(w_app, 6),
            "week_not_applicable_revenue": round(w_na, 6),
            "today_applicable_revenue": round(t_app, 6),
            "today_not_applicable_revenue": round(t_na, 6),
            "today_ios_share_pct": round((t_ios / t_imp_os) * 100, 2) if t_imp_os else 0,
            "week_ios_share_pct": round((w_ios / w_imp_os) * 100, 2) if w_imp_os else 0,
            "first_data_date": first,
            "last_synced_at": now_iso,
            "updated_at": now_iso,
        })

    if upserts:
        add_to_bq_batch("network_code_performance", upserts)
        log.info("bigquery: queued %d network_code_performance row(s) for batch load", len(upserts))


def _download_job(job: dict, token: str) -> dict:
    """Phase C1 worker — download + parse one report. Returns the result dict
    instead of raising so one bad report can't kill the download pool."""
    try:
        rows = download_report_rows(
            job["network_code"], job["job_id"], token, job.get("kind", "app")
        )
        return {"network_code": job["network_code"], "kind": job.get("kind", "app"), "rows": rows}
    except Exception as e:
        return {"network_code": job["network_code"], "kind": job.get("kind", "app"), "error": e}


def collect_downloads(results: list[dict], supabase: SupabaseClient) -> tuple[list[dict], int]:
    """Phase C1 classification — app failures are recorded (and counted),
    geo/fill failures only warn (supplementary reports, retried next cycle)."""
    downloaded: list[dict] = []
    error_count = 0
    for res in results:
        if "error" in res:
            if res["kind"] in ("geo", "fill"):
                log.warning("%s download skipped for %s: %s", res["kind"], res["network_code"], str(res["error"])[:200])
            else:
                record_sync_failure(supabase, res["network_code"], res["error"], "phase-c1-download")
                error_count += 1
        else:
            downloaded.append(res)
    return downloaded, error_count


def process_downloaded_jobs(downloaded: list[dict], supabase: SupabaseClient) -> tuple[int, int]:
    """Phases C2-C4 — upsert the already-downloaded rows, refresh summaries,
    advance last_synced_at (no per-code DB round-trips).
    App rows → adx_daily_stats/adx_os_stats/performance; geo rows →
    adx_geo_stats. Geo failures never fail the cycle.
    (C1 downloads are no longer here — they stream into a download pool while
    the poll phase is still running; see run_sync_cycle.)
    Returns (total_rows_upserted, error_count)."""
    now_iso = datetime.now(timezone.utc).isoformat()
    error_count = 0

    app_downloaded = [d for d in downloaded if d["kind"] == "app"]
    geo_downloaded = [d for d in downloaded if d["kind"] == "geo"]
    fill_downloaded = [d for d in downloaded if d["kind"] == "fill"]

    # C2: upsert rows into adx_daily_stats / adx_geo_stats / adx_fill_stats concurrently
    def up(d: dict) -> tuple[str, str]:
        """Returns (network_code, status) — status: 'app-ok' | 'geo-ok' |
        'fill-ok' | 'supp-failed' | 'app-failed'."""
        try:
            db = _supabase_client()
            if d["kind"] == "geo":
                upsert_geo_rows(d["network_code"], d["rows"], now_iso)
                return (d["network_code"], "geo-ok")
            if d["kind"] == "fill":
                upsert_fill_rows(d["network_code"], d["rows"], now_iso)
                return (d["network_code"], "fill-ok")
            upsert_daily_rows(db, d["network_code"], d["rows"], now_iso)
            return (d["network_code"], "app-ok")
        except Exception as e:
            if d["kind"] in ("geo", "fill"):
                log.warning("%s upsert skipped for %s: %s", d["kind"], d["network_code"], str(e)[:200])
                return (d["network_code"], "supp-failed")
            record_sync_failure(_supabase_client(), d["network_code"], e, "phase-c2-upsert")
            return (d["network_code"], "app-failed")

    app_upserted: list[str] = []
    geo_upserted = 0
    fill_upserted = 0
    with ThreadPoolExecutor(max_workers=DOWNLOAD_CONCURRENCY) as executor:
        for code, status in executor.map(up, downloaded):
            if status == "app-ok":
                app_upserted.append(code)
            elif status == "geo-ok":
                geo_upserted += 1
            elif status == "fill-ok":
                fill_upserted += 1
            elif status == "app-failed":
                error_count += 1

    # C3: batch refresh pre-computed summaries. The SQL RPC is the single source
    # of truth — it recomputes today / last-7 / last-30 day totals server-side
    # from the frozen raw tables, so the dashboard never drifts between cycles.
    # The in-memory fallback (refresh_performance_bulk) only runs when the
    # migration (migration_dashboard_totals.sql) hasn't been applied yet.
    rpc_ok = False
    try:
        supabase.rpc("refresh_dashboard_totals").execute()
        rpc_ok = True
    except Exception as e:
        log.warning("refresh_dashboard_totals skipped (migration not applied?): %s", str(e)[:200])
    if not rpc_ok:
        try:
            refresh_performance_bulk(supabase, app_downloaded, now_iso)
        except Exception as e:
            # Never let summary refresh kill the cycle before the BQ flush.
            log.warning("refresh_performance_bulk failed: %s", str(e)[:200])

    # C4: advance last_synced_at + clear errors for non-empty reports (bulk)
    if app_upserted:
        for i in range(0, len(app_upserted), 100):
            chunk = app_upserted[i:i + 100]
            try:
                supabase.table("network_codes").update({
                    "last_synced_at": now_iso,
                    "updated_at": now_iso,
                }).in_("network_code", chunk).execute()
                supabase.table("adx_sync_errors").delete().in_("network_code", chunk).execute()
            except Exception as e:
                log.warning("Batch last_synced update failed: %s", str(e)[:200])

    # Empty (0-row) reports are a success but must NOT advance last_synced_at
    # (matches legacy per-code path) — only clear any stale error.
    empty_codes = [d["network_code"] for d in app_downloaded if not d["rows"]]
    if empty_codes:
        for i in range(0, len(empty_codes), 100):
            chunk = empty_codes[i:i + 100]
            try:
                supabase.table("adx_sync_errors").delete().in_("network_code", chunk).execute()
            except Exception as e:
                log.warning("Batch error clear failed: %s", str(e)[:200])

    app_rows = sum(len(d["rows"]) for d in app_downloaded)
    geo_rows = sum(len(d["rows"]) for d in geo_downloaded)
    fill_rows = sum(len(d["rows"]) for d in fill_downloaded)
    log.info(
        "Processed %d app codes (%d rows) + %d geo codes (%d rows) + %d fill codes (%d rows)",
        len(app_upserted), app_rows, geo_upserted, geo_rows, fill_upserted, fill_rows,
    )
    return app_rows + geo_rows + fill_rows, error_count


def _stale_error_codes(codes: list[str]) -> list[str]:
    """Which of `codes` actually have rows in adx_sync_errors (usually none).

    One array-parameter SELECT replaces the old per-100 blind DELETE sweep
    that spent ~43s/cycle issuing ~18 no-op DML statements against ~1,700
    non-ACTIVE codes. Returns [] when the lookup fails (age-based cleanup in
    cleanup_old_sync_errors still reaps them within ERROR_RETENTION_HOURS)."""
    from google.cloud import bigquery as _bql
    try:
        job = bq.client.query(
            "SELECT DISTINCT network_code FROM `%s` WHERE network_code IN UNNEST(@codes)"
            % bq.table_ref("adx_sync_errors"),
            job_config=_bql.QueryJobConfig(query_parameters=[
                _bql.ArrayQueryParameter("codes", "STRING", list(codes)),
            ]),
        )
        return [row.network_code for row in job.result(timeout=60)]
    except Exception as e:
        log.warning("Stale error lookup skipped: %s", str(e)[:200])
        return []


def run_sync_cycle() -> dict:
    start_time = time.time()
    end_date = get_date_days_ago(0)
    week_start = get_date_days_ago(6)

    if not BIGQUERY_ENABLED:
        log.error("BIGQUERY_ENABLED=false — BigQuery is the only storage backend.")
        sys.exit(1)
    bq.ensure_tables()
    supabase = BQAdapter(bq)
    token = get_gam_token_cached()

    try:
        if hasattr(bq, "begin_cycle"):
            bq.begin_cycle()
    except Exception as e:
        log.warning("bigquery: begin_cycle failed: %s", str(e)[:200])

    # Step 1: Auto-fetch child publishers from MCM parent (if configured)
    mcm_stats = sync_network_codes(supabase, token)
    if mcm_stats.get("children_found", 0) > 0:
        log.info("MCM sync found %d children", mcm_stats["children_found"])

    # Step 1b: Parent-attributed earnings via mcm_earnings view (monthly)
    # DISABLED for now — will be reimplemented later.
    mcm_earnings_rows = 0

    # Step 2: Fetch all network codes from DB
    all_codes = fetch_all_network_codes(supabase, "*")
    log.info("Found %d entries in network_codes", len(all_codes))

    if not all_codes:
        try:
            flush_all_bq_batches()
        except Exception:
            pass
        return {"status": "skipped", "reason": "no codes", "elapsed": time.time() - start_time}

    # Step 3: Filter to only ACTIVE publishers (or codes without status = backward compat)
    active_list = []
    skipped_list = []
    for entry in all_codes:
        code = entry.get("network_code", "").strip()
        if not code:
            continue
        acct_status = entry.get("account_status")
        if acct_status is not None and acct_status.upper() != "ACTIVE":
            skipped_list.append(code)
        else:
            active_list.append(entry)

    if skipped_list:
        log.info(
            "Skipping %d non-ACTIVE code(s) (only tracked in DB): %s",
            len(skipped_list), skipped_list,
        )
        # Non-ACTIVE codes are never re-synced, so clear any stale sync errors
        # for them — otherwise old transient/"deleted" errors would linger
        # forever. SELECT first: only codes that really have error rows get a
        # DELETE (the old blind sweep cost ~43s/cycle deleting nothing).
        stale = _stale_error_codes(skipped_list)
        for i in range(0, len(stale), 100):
            chunk = stale[i:i + 100]
            try:
                supabase.table("adx_sync_errors").delete().in_("network_code", chunk).execute()
            except Exception as e:
                log.warning("Stale error clear for non-ACTIVE codes failed: %s", str(e)[:200])

    if not active_list:
        log.info("No ACTIVE network codes found — nothing to sync")
        try:
            flush_all_bq_batches()
        except Exception:
            pass
        return {
            "status": "skipped",
            "reason": "no active codes",
            "elapsed": time.time() - start_time,
        }

    # Step 4: Deduplicate and sync only ACTIVE codes
    unique: dict[str, str] = {}
    for entry in active_list:
        code = entry.get("network_code", "").strip()
        if not code:
            continue
        # Join date = when this child first became ACTIVE under our parent
        # (active_since), falling back to created_at (first seen in MCM).
        # We never query earnings earlier than this so pre-join history is excluded.
        boundary = entry.get("active_since") or entry.get("created_at") or ""
        join_date = to_date_str(boundary) or week_start
        if not join_date:
            join_date = week_start
        last_synced = entry.get("last_synced_at")
        if last_synced:
            # Incremental: a day is FINAL once it has passed (GAM T+1). Never
            # re-fetch days before yesterday except on the first cycle of a UTC
            # day (which re-pulls the rolling 7-day window to close out the
            # previous day). This freezes completed days — the #1 source of the
            # 7-day total flapping was re-writing finished days every 13 min.
            last_day = to_date_str(last_synced) or ""
            if last_day >= end_date:
                start = get_date_days_ago(1)  # already synced today: yesterday + today only
            else:
                start = effective_start(last_synced, week_start)  # first run of the day: full window
        else:
            # First sync — only fetch from the date the code was added
            start = join_date
            if INITIAL_SYNC_DAYS >= 0:
                # Fresh-dataset guard: don't backfill history on first sync
                # (0 = start at today, N = last N+1 days).
                start = max(start, get_date_days_ago(INITIAL_SYNC_DAYS))
        start = max(start, join_date)
        if INITIAL_SYNC_DAYS >= 0:
            # While the guard is on, never backfill beyond yesterday —
            # GAM T+1 still gets its one re-fetch day, nothing older.
            start = max(start, get_date_days_ago(1))
        if code not in unique or start < (unique.get(code) or week_start):
            unique[code] = start

    codes = list(unique.items())
    log.info("Syncing %d ACTIVE network codes (submit/poll/download=%d/%d/%d)", len(codes), SUBMIT_CONCURRENCY, POLL_CONCURRENCY, DOWNLOAD_CONCURRENCY)

    # Submit everything first so GAM processes all child-network report jobs in
    # parallel, then overlap polling with downloading: every job that flips
    # COMPLETED is handed to the download pool immediately, so phase C1 runs
    # DURING the poll tail instead of starting only after all ~978 polls end
    # (that serialization cost ~5-8 min per cycle).
    submitted_jobs = submit_all_report_jobs(codes, end_date, supabase, token)

    download_futures: list = []
    with ThreadPoolExecutor(max_workers=DOWNLOAD_CONCURRENCY) as dl_pool:
        def _dispatch_download(job: dict) -> None:
            download_futures.append(dl_pool.submit(_download_job, job, token))

        poll_all_report_jobs(submitted_jobs, supabase, token, on_completed=_dispatch_download)
        # Poll over — wait out the tail downloads still in flight (most finished
        # while polling continued). Heartbeat: a stuck download must not look
        # identical to a system sleep in the log.
        downloaded_results = []
        remaining_dl = set(download_futures)
        next_hb = time.time() + 30
        while remaining_dl:
            done_dl, remaining_dl = wait(remaining_dl, timeout=max(next_hb - time.time(), 0.5))
            downloaded_results.extend(f.result() for f in done_dl)
            if remaining_dl and time.time() >= next_hb:
                log.info("Download tail: %d/%d jobs finished...",
                         len(downloaded_results), len(download_futures))
                next_hb = time.time() + 30

    downloaded, download_errors = collect_downloads(downloaded_results, supabase)
    total_rows, error_count = process_downloaded_jobs(downloaded, supabase)
    error_count += download_errors

    # Flush buffered rows immediately so a later cleanup failure can't lose
    # the cycle's data (second flush at end of cycle is an empty no-op).
    try:
        flush_all_bq_batches()
    except Exception as e:
        log.warning("Failed to flush BigQuery batches after phase C: %s", str(e)[:200])

    deleted = 0
    stale_errors_deleted = 0
    if RETENTION_CLEANUP_ENABLED:
        cutoff = get_date_days_ago(RETENTION_DAYS)
        deleted = cleanup_old_data(supabase, cutoff)
    else:
        log.info("Retention cleanup disabled (RETENTION_CLEANUP_ENABLED=false)")

    error_cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=ERROR_RETENTION_HOURS)).isoformat()
    stale_errors_deleted = cleanup_old_sync_errors(supabase, error_cutoff_iso)

    elapsed = round(time.time() - start_time, 1)
    stats = {
        "status": "completed",
        "codes_found": len(all_codes),
        "codes_found_mcm": mcm_stats.get("children_found", 0),
        "mcm_earnings_rows": mcm_earnings_rows,
        "codes_active": len(codes),
        "codes_skipped_non_active": len(skipped_list),
        "codes_errored": error_count,
        "total_rows_upserted": total_rows,
        "rows_deleted": deleted,
        "sync_errors_deleted": stale_errors_deleted,
        "retention_days": RETENTION_DAYS,
        "error_retention_hours": ERROR_RETENTION_HOURS,
        "elapsed_seconds": elapsed,
    }
    try:
        flush_all_bq_batches()
    except Exception as e:
        log.warning("Failed to flush BigQuery batches at end of cycle: %s", str(e)[:200])
    log.info("Sync cycle complete: %s", json.dumps(stats))
    return stats


# ═════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════

def main():
    # Keep the machine awake while the bot runs: Windows otherwise idles into
    # sleep every ~hour, suspending the cycle mid-phase with no error at all
    # (observed 10/15 06:14, 10/16 07:15 and 09:19 — each froze a run at a
    # random phase). Released automatically when this process exits.
    try:
        if sys.platform == "win32":
            import ctypes
            ES_CONTINUOUS = 0x80000000
            ES_SYSTEM_REQUIRED = 0x00000001
            if ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED):
                log.info("System sleep inhibited while the bot runs")
    except Exception as e:
        log.warning("Could not inhibit system sleep: %s", e)
    validate_config()
    once = "--once" in sys.argv

    global SYNC_INTERVAL
    for arg in sys.argv:
        if arg.startswith("--interval="):
            SYNC_INTERVAL = int(arg.split("=")[1])

    log.info("=" * 60)
    log.info("GAM AdX Sync Bot starting")
    log.info("Interval: %d min | Submit/Poll/Download: %d/%d/%d | Retention: %d days", SYNC_INTERVAL, SUBMIT_CONCURRENCY, POLL_CONCURRENCY, DOWNLOAD_CONCURRENCY, RETENTION_DAYS)
    log.info("Storage: Google BigQuery only (%s.%s)", os.getenv("BIGQUERY_PROJECT_ID", "?"), os.getenv("BIGQUERY_DATASET", "ma_data"))
    log.info("=" * 60)

    # One-time cleanup: remove any adx_daily_stats rows older than a child's join date.
    # Idempotent — after the first run there is nothing left to remove.
    if PREJOIN_CLEANUP_ENABLED:
        try:
            cleanup_prejoin_data(BQAdapter(bq))
        except Exception as e:
            log.warning("Pre-join cleanup skipped: %s", str(e)[:200])
    else:
        log.info("Pre-join cleanup disabled (PREJOIN_CLEANUP_ENABLED=false)")

    cycle = 0
    while not shutdown_flag.is_set():
        cycle += 1
        log.info("─── Cycle %d ─────────────────────────────────────", cycle)
        try:
            run_sync_cycle()
        except Exception as e:
            log.exception("Unhandled error in sync cycle: %s", e)

        if once:
            break

        for _ in range((SYNC_INTERVAL * 60) // 5):
            if shutdown_flag.wait(5):
                break

    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
