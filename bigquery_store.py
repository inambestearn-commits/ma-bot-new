"""
BigQuery mirror store for the GAM AdX Sync Bot.

Appends every MA write into Google BigQuery so the full MA dataset
lives in a big-data warehouse and can be queried there.

Design:
  * Write mode is HARDCODED to "upsert" (MIRROR_MODE below) — not env-driven.
    Before each batch load, existing rows matching the batch's primary-key
    grains are DELETEd, then the batch is appended. Result: one row per
    grain, re-running the bot never duplicates data. Per-table locks
    serialize the delete+insert sequence across loader threads.

Config (env vars — deployment-specific only):
  BIGQUERY_PROJECT_ID          required, e.g. "adblobex"
  BIGQUERY_DATASET             default "ma_data"
  GOOGLE_APPLICATION_CREDENTIALS  path to a BigQuery service-account JSON
  BIGQUERY_CREDENTIALS_JSON     inline service-account JSON (alternative)
"""

import datetime as _dt
import json
import logging
import os
import time

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("gam_sync.bigquery")
import threading

# google-auth's AuthorizedSession passes timeout=None to requests/urllib3,
# which then sets the socket to blocking-forever mode — bypassing BOTH
# requests' per-call timeout and socket.setdefaulttimeout(). A silently dead
# BigQuery keep-alive connection therefore parks job polling in ssl.recv for
# hours (the 10/15 and 10/16 adx_geo_stats flush hangs: geo runs ~64 BQ
# operations per cycle, so it hits dead connections while the small tables
# finish first). Inject a default; explicit per-call timeouts still win.
try:
    import google.auth.transport.requests as _gauth_requests

    _orig_auth_session_request = _gauth_requests.AuthorizedSession.request

    def _auth_session_request(self, method, url, *args, **kwargs):
        # Real signature: request(self, method, url, data, headers,
        # max_allowed_time, timeout=...). Forward by position so both keyword
        # and positional callers bind correctly; bound a missing/None timeout
        # (positional timeout would be args[3]).
        if len(args) >= 4:
            args = list(args)
            if args[3] is None:
                args[3] = 60
        elif kwargs.get("timeout") is None:
            kwargs["timeout"] = 60
        return _orig_auth_session_request(self, method, url, *args, **kwargs)

    _gauth_requests.AuthorizedSession.request = _auth_session_request
except Exception:  # pragma: no cover — patch is best effort
    log.warning("bigquery: could not patch AuthorizedSession timeout default")

# Limit concurrent BigQuery load jobs to avoid table.write rate limits
_MAX_CONCURRENT_LOADS = int(os.getenv("BIGQUERY_MAX_CONCURRENT_LOADS", "8"))
_load_semaphore = threading.Semaphore(_MAX_CONCURRENT_LOADS)

# ensure_tables() revalidation window — see BigQueryStore.ensure_tables.
_ENSURE_TTL = float(os.getenv("BQ_ENSURE_TTL_SECONDS", "600"))


def _job_state_suffix(job):
    """' [job=... state=RUNNING error=...]' or '' — job.result() TimeoutError
    has an empty message, so without the state the log line says nothing."""
    if job is None:
        return ""
    err = getattr(job, "error_result", None)
    return " [job=%s state=%s%s]" % (
        getattr(job, "job_id", "?"),
        getattr(job, "state", "?"),
        (" error=%s" % str(err)[:200]) if err else "",
    )

# Write mode is fixed in code so a fresh clone (no .env) is always
# duplicate-safe. Do NOT make this env-driven: "append" would silently
# duplicate every grain on each sync cycle.
MIRROR_MODE = "upsert"

_STRING = "STRING"
_INTEGER = "INTEGER"
_INT64 = "INT64"
_NUMERIC = "NUMERIC"
_DATE = "DATE"
_TIMESTAMP = "TIMESTAMP"
_BOOL = "BOOL"

TABLES = {
    "network_codes": {
        "columns": [
            ("network_code", _STRING),
            ("label", _STRING),
            ("network_name", _STRING),
            ("account_status", _STRING),
            ("invitation_status", _STRING),
            ("delegation_type", _STRING),
            ("approval_status", _STRING),
            ("revenue_share_millipercent", _INTEGER),
            ("email", _STRING),
            ("seller_id", _STRING),
            ("child_publisher_id", _STRING),
            ("last_modified_at", _TIMESTAMP),
            ("active_since", _TIMESTAMP),
            ("declined_at", _TIMESTAMP),
            ("source", _STRING),
            ("last_synced_at", _TIMESTAMP),
            ("created_at", _TIMESTAMP),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code"],
    },
    "adx_daily_stats": {
        "columns": [
            ("network_code", _STRING),
            ("date", _DATE),
            ("platform", _STRING),
            ("revenue", _NUMERIC),
            ("ecpm", _NUMERIC),
            ("impressions", _INTEGER),
            ("clicks", _INTEGER),
            ("ctr", _NUMERIC),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code", "date", "platform"],
        # Date-interval partitioning keeps warehouse reads (which always filter
        # on `date`) from scanning the whole table as it grows. Only applies on
        # table creation — an existing table is never altered by ensure_tables.
        "partition_by": "date",
        "cluster_by": ["network_code"],
    },
    "adx_os_stats": {
        "columns": [
            ("network_code", _STRING),
            ("date", _DATE),
            ("os", _STRING),
            ("impressions", _INTEGER),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code", "date", "os"],
        "partition_by": "date",
        "cluster_by": ["network_code"],
    },
    "adx_geo_stats": {
        "columns": [
            ("network_code", _STRING),
            ("date", _DATE),
            ("country", _STRING),
            ("carrier", _STRING),
            ("browser", _STRING),
            ("revenue", _NUMERIC),
            ("ecpm", _NUMERIC),
            ("impressions", _INTEGER),
            ("clicks", _INTEGER),
            ("ctr", _NUMERIC),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code", "date", "country", "carrier", "browser"],
        "partition_by": "date",
        "cluster_by": ["network_code"],
    },
    "adx_fill_stats": {
        "columns": [
            ("network_code", _STRING),
            ("date", _DATE),
            ("platform", _STRING),
            ("ad_requests", _INTEGER),
            ("fill_rate", _NUMERIC),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code", "date", "platform"],
        "partition_by": "date",
        "cluster_by": ["network_code"],
    },
    "network_code_performance": {
        "columns": [
            ("network_code", _STRING),
            ("today_revenue", _NUMERIC),
            ("today_impressions", _INT64),
            ("today_clicks", _INT64),
            ("week_revenue", _NUMERIC),
            ("week_impressions", _INT64),
            ("week_clicks", _INT64),
            ("week_applicable_revenue", _NUMERIC),
            ("week_not_applicable_revenue", _NUMERIC),
            ("today_applicable_revenue", _NUMERIC),
            ("today_not_applicable_revenue", _NUMERIC),
            ("today_ios_share_pct", _NUMERIC),
            ("week_ios_share_pct", _NUMERIC),
            ("first_data_date", _DATE),
            ("last_synced_at", _TIMESTAMP),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code"],
    },
    "adx_sync_errors": {
        "columns": [
            ("network_code", _STRING),
            ("error_message", _STRING),
            ("failed_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code"],
    },
    "mcm_earnings": {
        "columns": [
            ("month", _STRING),
            ("child_network_code", _STRING),
            ("child_name", _STRING),
            ("delegation_type", _STRING),
            ("total_earnings_micros", _INT64),
            ("total_earnings_currency_code", _STRING),
            ("parent_payment_micros", _INT64),
            ("parent_payment_currency_code", _STRING),
            ("child_payment_micros", _INT64),
            ("child_payment_currency_code", _STRING),
            ("deductions_micros", _INT64),
            ("deductions_currency_code", _STRING),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["month", "child_network_code"],
    },
}

_TYPES = {
    _STRING: "STRING",
    _INTEGER: "INTEGER",
    _INT64: "INT64",
    _NUMERIC: "NUMERIC",
    _DATE: "DATE",
    _TIMESTAMP: "TIMESTAMP",
    _BOOL: "BOOL",
}


def _bq():
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except ImportError as e:
        raise RuntimeError(
            "google-cloud-bigquery is not installed — run: pip install -r requirements.txt"
        ) from e

    project_id = os.getenv("BIGQUERY_PROJECT_ID", "").strip()
    if not project_id:
        raise RuntimeError("BIGQUERY_PROJECT_ID is not set")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    creds_json = os.getenv("BIGQUERY_CREDENTIALS_JSON", "").strip()
    if creds_json:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(creds_json)
        )
        return bigquery.Client(project=project_id, credentials=credentials)
    if creds_path and os.path.isfile(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=project_id, credentials=credentials)
    return bigquery.Client(project=project_id)


def get_bq_client():
    return _bq()


def _as_date(value):
    if not value:
        return None
    try:
        return _dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _as_ts(value):
    if not value:
        return None
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return _dt.datetime.fromisoformat(s)
    except ValueError:
        try:
            return _dt.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None


class BigQueryStore:
    def __init__(self, enabled=None):
        if enabled is None:
            enabled = os.getenv("BIGQUERY_ENABLED", "true").strip().lower() not in (
                "0", "false", "no", "off",
            )
        self.enabled = enabled
        self._client = None
        self._client_error = None
        self._cycle_truncated_tables = set()
        self._cycle_lock = threading.Lock()
        self._upsert_locks = {}

    def begin_cycle(self):
        """Marks the start of a new sync cycle for per-cycle table reset mode."""
        with self._cycle_lock:
            self._cycle_truncated_tables.clear()

    @property
    def client(self):
        if not self.enabled:
            return None
        if self._client is None and self._client_error is None:
            try:
                self._client = _bq()
            except Exception as e:
                self._client_error = e
                log.warning("bigquery: mirror disabled — %s", str(e)[:200])
        return self._client

    @property
    def dataset(self):
        return "%s.%s" % (os.getenv("BIGQUERY_PROJECT_ID", "").strip(), os.getenv("BIGQUERY_DATASET", "ma_data").strip())

    def table_ref(self, table):
        return "%s.%s" % (self.dataset, table)

    def ensure_dataset(self):
        if not self.client:
            return False
        from google.cloud import bigquery, exceptions

        dataset_id = self.dataset
        try:
            self.client.get_dataset(dataset_id)
        except exceptions.NotFound:
            self.client.create_dataset(bigquery.Dataset(dataset_id))
        return True

    def ensure_tables(self):
        # Memoized: sync_upsert calls this on every 5000-row chunk, and a
        # 78k-row geo flush used to re-run ~9 CREATE/DATASET round-trips per
        # chunk (~144 queries/cycle, ~30-60s of pure overhead). The cycle
        # start still re-ensures every _ENSURE_TTL seconds, so a table
        # dropped mid-process is recreated within the TTL.
        now = time.time()
        if now - getattr(self, "_ensured_at", 0.0) < _ENSURE_TTL:
            return True
        if not self.ensure_dataset():
            return False
        for name, spec in TABLES.items():
            self._create_table(name, spec)
        self._ensured_at = now
        return True

    def _create_table(self, name, spec):
        cols = []
        for col, typ in spec["columns"]:
            cols.append("  %s %s" % (col, _TYPES[typ]))
        ddl = "CREATE TABLE IF NOT EXISTS %s (\n%s\n)" % (self.table_ref(name), ",\n".join(cols))
        # Optional date partitioning + clustering. Only affects creation; an
        # existing (already-created) table is left untouched by IF NOT EXISTS.
        partition_by = spec.get("partition_by")
        if partition_by:
            ddl += "\nPARTITION BY %s" % partition_by
        cluster_by = spec.get("cluster_by")
        if cluster_by:
            ddl += "\nCLUSTER BY %s" % ", ".join(cluster_by)
        try:
            self.client.query(ddl).result(timeout=60)
        except Exception as e:
            log.warning("bigquery: create table %s failed: %s", name, str(e)[:300])

    def _convert_row(self, row, spec):
        out = {}
        for col, typ in spec["columns"]:
            value = row.get(col)
            if value is None:
                out[col] = None
                continue
            if typ == _DATE:
                d = _as_date(value)
                out[col] = d.isoformat() if d else None
            elif typ == _TIMESTAMP:
                t = _as_ts(value)
                out[col] = t.isoformat() if t else None
            elif typ in (_INTEGER, _INT64):
                out[col] = int(value)
            elif typ == _NUMERIC:
                out[col] = round(float(value), 6)
            elif typ == _BOOL:
                out[col] = bool(value)
            else:
                out[col] = str(value)
        return out

    def _upsert_lock_for(self, table):
        with self._cycle_lock:
            lock = self._upsert_locks.get(table)
            if lock is None:
                lock = threading.Lock()
                self._upsert_locks[table] = lock
            return lock

    def _delete_grains(self, table, spec, rows):
        """DELETE existing rows matching the batch's primary-key grains.

        Returns the number of distinct grains deleted (best effort; 0 on
        any error — the batch load still proceeds so no data is lost).
        Key values are compared as strings so INT64-vs-STRING column
        mismatches can never break the DELETE.

        Keys are inlined as literals (escaped) instead of query parameters:
        the BQ API rejects ARRAY<STRUCT<...>> query parameters with
        "Invalid value for type ... STRUCT<...> is not a valid value".
        """
        pk = spec.get("primary_key") or []
        if not pk:
            return 0
        seen = set()
        keys = []
        for r in rows:
            tup = tuple("" if r.get(k) is None else str(r.get(k)) for k in pk)
            if tup not in seen:
                seen.add(tup)
                keys.append(tup)
        if not keys:
            return 0

        def lit(v):
            return "'%s'" % str(v).replace("\\", "\\\\").replace("'", "\\'")

        lhs = ", ".join(
            "CAST(%s AS STRING) AS f%d" % (k, i) for i, k in enumerate(pk)
        )
        rhs = ", ".join("k.f%d" % i for i in range(len(pk)))
        deleted = 0
        job = None
        try:
            # Chunk very large key sets so no single DELETE gets too big.
            for i in range(0, len(keys), 2000):
                chunk = keys[i:i + 2000]
                structs = ", ".join(
                    "STRUCT(%s)" % ", ".join(
                        "%s AS f%d" % (lit(v), j) for j, v in enumerate(tup)
                    )
                    for tup in chunk
                )
                sql = (
                    "DELETE FROM `%s` WHERE EXISTS ("
                    " SELECT 1 FROM UNNEST([%s]) AS k"
                    " WHERE STRUCT(%s) = STRUCT(%s)"
                    ")"
                ) % (self.table_ref(table), structs, lhs, rhs)
                job = self.client.query(sql)
                # Bounded wait: an unbounded result() hung cycles for hours
                # when a job sat stuck (see flush-hang history in sync logs).
                job.result(timeout=90)
                deleted += job.num_dml_affected_rows or 0
            return deleted
        except Exception as e:
            log.warning("bigquery: pre-load DELETE for %s failed (load continues): %s%s",
                        table, str(e)[:300], _job_state_suffix(job))
            return 0

    def load(self, table, rows):
        if not self.enabled or not rows or not self.client:
            return 0
        self.ensure_tables()
        from google.cloud.bigquery import LoadJobConfig

        spec = TABLES[table]
        converted = [self._convert_row(r, spec) for r in rows]
        mirror_mode = MIRROR_MODE

        write_disposition = "WRITE_APPEND"
        if mirror_mode == "truncate_each_cycle":
            # Free-tier friendly: avoid TRUNCATE TABLE DDL/DML requirement.
            # First batch for each table in the cycle uses WRITE_TRUNCATE,
            # remaining batches use WRITE_APPEND.
            with self._cycle_lock:
                first_batch_this_cycle = table not in self._cycle_truncated_tables
                if first_batch_this_cycle:
                    self._cycle_truncated_tables.add(table)
            write_disposition = "WRITE_TRUNCATE" if first_batch_this_cycle else "WRITE_APPEND"
        elif mirror_mode == "replace":
            # Replace on every load call (not recommended with tiny batches).
            write_disposition = "WRITE_TRUNCATE"

        upsert = mirror_mode == "upsert" and bool(spec.get("primary_key"))
        upsert_lock = self._upsert_lock_for(table) if upsert else None

        def _do_load():
            if upsert:
                n_keys = self._delete_grains(table, spec, converted)
                if n_keys:
                    log.info("bigquery: upsert cleared %d grain(s) in %s before load",
                             n_keys, table)
            # Retry on rate-limit / transient BigQuery errors with exponential backoff
            max_attempts = int(os.getenv("BIGQUERY_LOAD_RETRIES", "5"))
            base_delay = float(os.getenv("BIGQUERY_LOAD_BASE_DELAY", "1.0"))

            attempt = 0
            job = None
            while attempt < max_attempts:
                attempt += 1
                try:
                    with _load_semaphore:
                        job = self.client.load_table_from_json(
                            converted,
                            self.table_ref(table),
                            job_config=LoadJobConfig(write_disposition=write_disposition),
                        )
                        job.result(timeout=120)
                        return job.output_rows
                except Exception as e:
                    # Log details for diagnosis
                    msg = str(e)
                    log.warning("bigquery: load attempt %d/%d for %s failed: %s%s",
                                attempt, max_attempts, table, msg[:300], _job_state_suffix(job))
                    # If last attempt, log and return 0 (caller may record sync error)
                    if attempt >= max_attempts:
                        log.error("bigquery: load failed for table %s after %d attempts", table, max_attempts)
                        return 0
                    # Exponential backoff
                    delay = base_delay * (2 ** (attempt - 1))
                    time.sleep(delay)

        if upsert_lock is not None:
            # Serialize delete+insert per table so concurrent loader threads
            # can never interleave into duplicate grains.
            with upsert_lock:
                return _do_load()
        return _do_load()

    def clear_sync_errors(self, codes):
        return 0


def _col_bq_type(table, col):
    """Return the BigQuery scalar param type for a column in TABLES."""
    spec = TABLES.get(table, {})
    for c, t in spec.get("columns", []):
        if c == col:
            if t in (_INTEGER, _INT64):
                return "INT64"
            if t == _NUMERIC:
                return "NUMERIC"
            if t == _DATE:
                return "DATE"
            if t == _TIMESTAMP:
                return "TIMESTAMP"
            if t == _BOOL:
                return "BOOL"
            return "STRING"
    return "STRING"


def _col_names(table):
    return {c for c, _ in TABLES.get(table, {}).get("columns", [])}


# ─────────────────────────────────────────────────────────────────────────────
# BigQueryStore read / write helpers used by BQAdapter
# These standalone functions are attached to the existing BigQueryStore class
# below via attribute assignment (monkeypatch style).
# ─────────────────────────────────────────────────────────────────────────────

def _bs_select(self, table, columns, filters, offset, limit):
    """SELECT rows from a BigQuery table with optional WHERE filters."""
    from google.cloud import bigquery as _bq_lib

    if not self.client:
        return []
    known = _col_names(table)
    if columns == "*":
        col_clause = "*"
    else:
        # Validate and sanitise each requested column name
        validated = [c.strip() for c in columns.split(",") if c.strip() in known]
        col_clause = ", ".join(validated) if validated else "*"

    where_parts = []
    params = []
    for col, (op, val) in (filters or {}).items():
        if col not in known:
            continue
        bq_type = _col_bq_type(table, col)
        pname = "f_%s" % col
        if op == "IN":
            where_parts.append("%s IN UNNEST(@%s)" % (col, pname))
            params.append(_bq_lib.ArrayQueryParameter(pname, "STRING", [str(v) for v in val]))
        elif op == "=":
            where_parts.append("CAST(%s AS STRING) = @%s" % (col, pname))
            params.append(_bq_lib.ScalarQueryParameter(pname, "STRING", str(val)))
        else:
            where_parts.append("%s %s @%s" % (col, op, pname))
            params.append(_bq_lib.ScalarQueryParameter(pname, bq_type, val))

    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    sql = "SELECT %s FROM `%s` %s LIMIT %d OFFSET %d" % (
        col_clause, self.table_ref(table), where_clause, limit, offset,
    )
    try:
        job = self.client.query(
            sql,
            job_config=_bq_lib.QueryJobConfig(query_parameters=params, use_legacy_sql=False),
        )
        return [dict(row) for row in job.result(timeout=60)]
    except Exception as e:
        log.warning("bigquery: SELECT %s failed: %s", table, str(e)[:300])
        return []


def _bs_delete_rows(self, table, filters):
    """DELETE rows matching filters from a BigQuery table."""
    from google.cloud import bigquery as _bq_lib

    if not self.client or not filters:
        return 0
    known = _col_names(table)
    where_parts = []
    params = []
    for col, (op, val) in filters.items():
        if col not in known:
            continue
        bq_type = _col_bq_type(table, col)
        pname = "d_%s" % col
        if op == "IN":
            where_parts.append("%s IN UNNEST(@%s)" % (col, pname))
            params.append(_bq_lib.ArrayQueryParameter(pname, "STRING", [str(v) for v in val]))
        elif op == "=":
            where_parts.append("CAST(%s AS STRING) = @%s" % (col, pname))
            params.append(_bq_lib.ScalarQueryParameter(pname, "STRING", str(val)))
        else:
            where_parts.append("%s %s @%s" % (col, op, pname))
            params.append(_bq_lib.ScalarQueryParameter(pname, bq_type, val))

    if not where_parts:
        return 0
    sql = "DELETE FROM `%s` WHERE %s" % (self.table_ref(table), " AND ".join(where_parts))
    try:
        job = self.client.query(
            sql,
            job_config=_bq_lib.QueryJobConfig(query_parameters=params, use_legacy_sql=False),
        )
        job.result(timeout=90)
        count = job.num_dml_affected_rows or 0
        return count
    except Exception as e:
        log.warning("bigquery: DELETE %s failed: %s", table, str(e)[:300])
        return 0


def _bs_update_rows(self, table, data, filters):
    """UPDATE rows in a BigQuery table. data is {col: val}; filters is the WHERE."""
    from google.cloud import bigquery as _bq_lib

    if not self.client or not data or not filters:
        return 0
    known = _col_names(table)
    set_parts = []
    params = []
    for col, val in data.items():
        if col not in known:
            continue
        bq_type = _col_bq_type(table, col)
        pname = "s_%s" % col
        set_parts.append("%s = @%s" % (col, pname))
        params.append(_bq_lib.ScalarQueryParameter(pname, bq_type, val))

    where_parts = []
    for col, (op, val) in filters.items():
        if col not in known:
            continue
        bq_type = _col_bq_type(table, col)
        pname = "w_%s" % col
        if op == "IN":
            where_parts.append("%s IN UNNEST(@%s)" % (col, pname))
            params.append(_bq_lib.ArrayQueryParameter(pname, "STRING", [str(v) for v in val]))
        elif op == "=":
            where_parts.append("CAST(%s AS STRING) = @%s" % (col, pname))
            params.append(_bq_lib.ScalarQueryParameter(pname, "STRING", str(val)))
        else:
            where_parts.append("%s %s @%s" % (col, op, pname))
            params.append(_bq_lib.ScalarQueryParameter(pname, bq_type, val))

    if not set_parts or not where_parts:
        return 0
    sql = "UPDATE `%s` SET %s WHERE %s" % (
        self.table_ref(table), ", ".join(set_parts), " AND ".join(where_parts)
    )
    try:
        job = self.client.query(
            sql,
            job_config=_bq_lib.QueryJobConfig(query_parameters=params, use_legacy_sql=False),
        )
        job.result(timeout=90)
        return job.num_dml_affected_rows or 0
    except Exception as e:
        log.warning("bigquery: UPDATE %s failed: %s", table, str(e)[:300])
        return 0


def _bs_sync_upsert(self, table, rows):
    """Synchronous delete-then-load upsert (same behaviour as MIRROR_MODE)."""
    if not rows or not self.client:
        return 0
    self.ensure_tables()
    spec = TABLES.get(table)
    if not spec:
        log.warning("bigquery: unknown table %s — skipping upsert", table)
        return 0
    from google.cloud.bigquery import LoadJobConfig
    converted = [self._convert_row(r, spec) for r in rows]
    lock = self._upsert_lock_for(table)
    with lock:
        max_attempts = int(os.getenv("BIGQUERY_LOAD_RETRIES", "5"))
        base_delay = float(os.getenv("BIGQUERY_LOAD_BASE_DELAY", "1.0"))
        for attempt in range(max_attempts):
            # Re-delete before EVERY attempt: a previous load that timed out
            # may have completed server-side, and WRITE_APPEND retry without
            # a fresh delete would duplicate grains.
            self._delete_grains(table, spec, converted)
            job = None
            try:
                with _load_semaphore:
                    job = self.client.load_table_from_json(
                        converted,
                        self.table_ref(table),
                        job_config=LoadJobConfig(write_disposition="WRITE_APPEND"),
                    )
                    job.result(timeout=120)
                    return len(converted)
            except Exception as e:
                log.warning("bigquery: sync_upsert attempt %d/%d %s: %s%s",
                            attempt + 1, max_attempts, table, str(e)[:200],
                            _job_state_suffix(job))
                if attempt + 1 >= max_attempts:
                    return 0
                time.sleep(base_delay * (2 ** attempt))
    return 0


# Attach methods to BigQueryStore (extending the class without a full rewrite)
BigQueryStore._do_select = _bs_select
BigQueryStore._do_delete_rows = _bs_delete_rows
BigQueryStore._do_update_rows = _bs_update_rows
BigQueryStore.sync_upsert = _bs_sync_upsert


# ─────────────────────────────────────────────────────────────────────────────
# BQAdapter — drop-in replacement for the Supabase PostgREST client
# ─────────────────────────────────────────────────────────────────────────────

class _BQResult:
    """Mimics the Supabase APIResponse.data field."""
    __slots__ = ("data",)

    def __init__(self, data):
        self.data = data


class _BQExecutable:
    """Chainable query builder; .execute() dispatches to BigQueryStore."""

    def __init__(self, store, table, mode, payload=None):
        self._store = store
        self._table = table
        self._mode = mode       # "select" | "upsert" | "insert" | "update" | "delete"
        self._payload = payload  # rows list or update-data dict
        self._filters = {}      # {col: (op, val)}
        self._columns = "*"
        self._offset = 0
        self._limit = 10000

    def eq(self, col, val):
        self._filters[col] = ("=", val)
        return self

    def in_(self, col, vals):
        self._filters[col] = ("IN", list(vals))
        return self

    def lt(self, col, val):
        self._filters[col] = ("<", val)
        return self

    def lte(self, col, val):
        self._filters[col] = ("<=", val)
        return self

    def gte(self, col, val):
        self._filters[col] = (">=", val)
        return self

    def range(self, start, end):
        self._offset = start
        self._limit = max(1, end - start + 1)
        return self

    def execute(self):
        if self._mode == "select":
            rows = self._store._do_select(
                self._table, self._columns, self._filters, self._offset, self._limit
            )
            return _BQResult(rows)
        if self._mode in ("upsert", "insert"):
            self._store.sync_upsert(self._table, self._payload)
            return _BQResult(self._payload)
        if self._mode == "update":
            self._store._do_update_rows(self._table, self._payload, self._filters)
            return _BQResult([])
        if self._mode == "delete":
            self._store._do_delete_rows(self._table, self._filters)
            return _BQResult([])
        return _BQResult([])


class _BQTableRef:
    """Mimics supabase.table("name")."""

    def __init__(self, store, table):
        self._store = store
        self._table = table

    def select(self, columns="*"):
        ex = _BQExecutable(self._store, self._table, "select")
        ex._columns = columns
        return ex

    def upsert(self, rows, on_conflict=None):
        if isinstance(rows, dict):
            rows = [rows]
        return _BQExecutable(self._store, self._table, "upsert", list(rows))

    def insert(self, row):
        if isinstance(row, dict):
            row = [row]
        return _BQExecutable(self._store, self._table, "insert", list(row))

    def update(self, data):
        return _BQExecutable(self._store, self._table, "update", data)

    def delete(self):
        return _BQExecutable(self._store, self._table, "delete")


class BQAdapter:
    """Drop-in storage client backed by BigQuery (sole bot storage backend).

    Always active — Supabase is not used for bot data in any mode.
    All data reads and writes go to BigQuery (BIGQUERY_DATASET, default ma_data).

    Thread-safe: multiple threads can share one BQAdapter instance because
    BigQueryStore serialises per-table upserts with per-table locks, and the
    BigQuery client itself is thread-safe.
    """

    def __init__(self, store: "BigQueryStore"):
        self._store = store

    def table(self, name: str) -> "_BQTableRef":
        return _BQTableRef(self._store, name)

    def rpc(self, name: str):
        """Raise NotImplementedError so callers fall through to their fallback."""
        raise NotImplementedError(
            "BQAdapter does not support Supabase RPCs (%s). "
            "The caller should have a BigQuery-based fallback." % name
        )


def print_schema():
    lines = [
        "-- Google BigQuery MA data schema (mirrors scripts/gam-sync-bot synced tables)",
        "-- Run with: bq query --use_legacy_sql=false < bigquery_schema.sql",
        "-- Or auto-create with: py bigquery_store.py --create-schema",
        "",
        "-- Requires the dataset to exist (see BIGQUERY_DATASET).",
    ]
    for name, spec in TABLES.items():
        cols = [("  %s %s" % (c, _TYPES[t])) for c, t in spec["columns"]]
        lines.append("")
        lines.append("CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<DATASET>.%s` (" % name)
        lines.append(",\n".join(cols))
        lines.append(")")
    lines.append("")
    lines.append("-- All-MA-data query (counts every MA table written by the bot):")
    lines.append("-- SELECT 'network_codes' AS source, COUNT(*) AS rows FROM `<PROJECT_ID>.<DATASET>.network_codes`")
    lines.append("-- UNION ALL SELECT 'adx_daily_stats', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.adx_daily_stats`")
    lines.append("-- UNION ALL SELECT 'adx_os_stats', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.adx_os_stats`")
    lines.append("-- UNION ALL SELECT 'adx_geo_stats', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.adx_geo_stats`")
    lines.append("-- UNION ALL SELECT 'adx_fill_stats', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.adx_fill_stats`")
    lines.append("-- UNION ALL SELECT 'network_code_performance', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.network_code_performance`")
    lines.append("-- UNION ALL SELECT 'adx_sync_errors', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.adx_sync_errors`")
    lines.append("-- UNION ALL SELECT 'mcm_earnings', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.mcm_earnings`;")
    print("\n".join(lines))


if __name__ == "__main__":
    import sys

    if "--print-schema" in sys.argv:
        print_schema()
    elif "--create-schema" in sys.argv:
        logging.basicConfig(level=logging.INFO)
        store = BigQueryStore()
        if store.ensure_tables():
            print("BigQuery schema ready (dataset %s)" % store.dataset)
        else:
            print("BigQuery mirror is disabled (BIGQUERY_ENABLED=false) or not configured")
