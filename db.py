"""
db.py — forecast.wbn | PostgreSQL persistence layer  v7
────────────────────────────────────────────────────────
REPLACES /db.py entirely.

Changes vs v6:
  • Connection pooling via psycopg2.pool.ThreadedConnectionPool  (fixes thread-safety)
  • Added indexes on (email) and (qkey, email)
  • New table: wbn_feature_flags  – admin-controlled feature toggles
  • New table: wbn_ticker         – admin broadcast messages shown on forecast page
  • db_get_feature_flags / db_set_feature_flag
  • db_get_ticker / db_set_ticker
  • Retry wrapper on _execute (2 attempts, transparent to callers)
"""

import os, json, datetime, time
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_ENABLED   = bool(DATABASE_URL)

# ── Thread-safe connection pool ───────────────────────────────────────────────
_pool: psycopg2.pool.ThreadedConnectionPool | None = None

def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL,
        )
    return _pool

def _execute(sql: str, params=None, fetch: str | None = None, _retry: int = 2):
    """
    Execute SQL via the connection pool.
    fetch = 'one' | 'all' | None
    Retries once on OperationalError (stale connection).
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            if fetch == "one":
                result = cur.fetchone()
                conn.commit()
                return dict(result) if result else None
            elif fetch == "all":
                result = cur.fetchall()
                conn.commit()
                return [dict(r) for r in result]
            else:
                conn.commit()
                return None
    except psycopg2.OperationalError as e:
        conn.rollback()
        if _retry > 0:
            pool.putconn(conn, close=True)   # discard stale connection
            return _execute(sql, params, fetch, _retry - 1)
        raise
    except Exception as e:
        conn.rollback()
        raise
    finally:
        # Only return to pool if not already closed/discarded above
        try:
            pool.putconn(conn)
        except Exception:
            pass


# ── Schema initialisation ─────────────────────────────────────────────────────
def init_db():
    """Create / migrate tables. Safe to call on every startup."""

    # ── Quarters ──────────────────────────────────────────────────────────────
    _execute("""
        CREATE TABLE IF NOT EXISTS wbn_quarters (
            qkey           VARCHAR(4) PRIMARY KEY,
            initiated      BOOLEAN DEFAULT FALSE,
            initiated_at   VARCHAR(32),
            drr_data       JSONB,
            channels_found JSONB,
            updated_at     TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # ── Submissions ───────────────────────────────────────────────────────────
    _execute("""
        CREATE TABLE IF NOT EXISTS wbn_submissions (
            id                    SERIAL PRIMARY KEY,
            qkey                  VARCHAR(4)   NOT NULL,
            email                 VARCHAR(255) NOT NULL,
            submitted             BOOLEAN      DEFAULT FALSE,
            submitted_at          VARCHAR(32),
            submitted_at_dt       VARCHAR(64),
            user_name             VARCHAR(255),
            revision              INTEGER      DEFAULT 0,
            refill_requested      BOOLEAN      DEFAULT FALSE,
            refill_reason         TEXT,
            refill_cooldown_until VARCHAR(16),
            data                  JSONB,
            file                  VARCHAR(512),
            excel_bytes           BYTEA,
            updated_at            TIMESTAMPTZ  DEFAULT NOW(),
            UNIQUE (qkey, email)
        )
    """)

    # ── Indexes ───────────────────────────────────────────────────────────────
    _execute("""
        CREATE INDEX IF NOT EXISTS idx_wbn_submissions_qkey
        ON wbn_submissions (qkey)
    """)
    _execute("""
        CREATE INDEX IF NOT EXISTS idx_wbn_submissions_email
        ON wbn_submissions (email)
    """)
    _execute("""
        CREATE INDEX IF NOT EXISTS idx_wbn_submissions_qkey_email
        ON wbn_submissions (qkey, email)
    """)

    # ── Activity log ──────────────────────────────────────────────────────────
    _execute("""
        CREATE TABLE IF NOT EXISTS wbn_activity_log (
            id         SERIAL PRIMARY KEY,
            timestamp  VARCHAR(32),
            action     VARCHAR(128),
            "user"     VARCHAR(255),
            detail     TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # ── Feature flags ─────────────────────────────────────────────────────────
    # Stores admin-controlled on/off toggles.
    # Seeded with defaults on first run; subsequent runs only INSERT missing rows.
    _execute("""
        CREATE TABLE IF NOT EXISTS wbn_feature_flags (
            flag_key    VARCHAR(64) PRIMARY KEY,
            enabled     BOOLEAN     DEFAULT TRUE,
            updated_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Seed defaults (INSERT … DO NOTHING keeps existing values)
    for flag, default in [
        ("load_sample_values",   True),
        ("download_template",    True),
        ("upload_excel_fill",    True),
    ]:
        _execute("""
            INSERT INTO wbn_feature_flags (flag_key, enabled)
            VALUES (%s, %s)
            ON CONFLICT (flag_key) DO NOTHING
        """, (flag, default))

    # ── Ticker ────────────────────────────────────────────────────────────────
    # Single-row table: admin sets a broadcast message shown as a banner.
    _execute("""
        CREATE TABLE IF NOT EXISTS wbn_ticker (
            id         INTEGER PRIMARY KEY DEFAULT 1,
            message    TEXT    DEFAULT '',
            active     BOOLEAN DEFAULT FALSE,
            style      VARCHAR(16) DEFAULT 'info',
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            CHECK (id = 1)
        )
    """)
    # Ensure the single row exists
    _execute("""
        INSERT INTO wbn_ticker (id, message, active, style)
        VALUES (1, '', FALSE, 'info')
        ON CONFLICT (id) DO NOTHING
    """)

    print("  [DB] Schema initialised OK (v7)")


# ── Quarter helpers ───────────────────────────────────────────────────────────
def db_save_quarter(qkey: str, data: dict):
    drr_data       = data.get("drr_data")
    channels_found = data.get("channels_found", [])
    _execute("""
        INSERT INTO wbn_quarters (qkey, initiated, initiated_at, drr_data, channels_found, updated_at)
        VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, NOW())
        ON CONFLICT (qkey) DO UPDATE SET
            initiated      = EXCLUDED.initiated,
            initiated_at   = EXCLUDED.initiated_at,
            drr_data       = EXCLUDED.drr_data,
            channels_found = EXCLUDED.channels_found,
            updated_at     = NOW()
    """, (
        qkey,
        data.get("initiated", False),
        data.get("initiated_at", ""),
        json.dumps(drr_data) if drr_data is not None else None,
        json.dumps(channels_found),
    ))


def db_get_quarter(qkey: str) -> dict | None:
    row = _execute("SELECT * FROM wbn_quarters WHERE qkey = %s", (qkey,), fetch="one")
    if not row:
        return None
    drr_raw = row.get("drr_data")
    return {
        "initiated":      row["initiated"],
        "initiated_at":   row["initiated_at"] or "",
        "drr_data":       drr_raw if isinstance(drr_raw, list) else
                          (json.loads(drr_raw) if drr_raw else []),
        "channels_found": row.get("channels_found") or [],
    }


def db_revoke_quarter(qkey: str):
    _execute("DELETE FROM wbn_quarters WHERE qkey = %s", (qkey,))
    _execute("DELETE FROM wbn_submissions WHERE qkey = %s", (qkey,))


# ── Submission helpers ────────────────────────────────────────────────────────
def db_save_submission(qkey: str, email: str, sub: dict):
    excel_bytes = sub.get("excel_bytes")
    data        = sub.get("data")
    _execute("""
        INSERT INTO wbn_submissions
            (qkey, email, submitted, submitted_at, submitted_at_dt, user_name,
             revision, refill_requested, refill_reason, refill_cooldown_until,
             data, file, excel_bytes, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,NOW())
        ON CONFLICT (qkey, email) DO UPDATE SET
            submitted             = EXCLUDED.submitted,
            submitted_at          = EXCLUDED.submitted_at,
            submitted_at_dt       = EXCLUDED.submitted_at_dt,
            user_name             = EXCLUDED.user_name,
            revision              = EXCLUDED.revision,
            refill_requested      = EXCLUDED.refill_requested,
            refill_reason         = EXCLUDED.refill_reason,
            refill_cooldown_until = EXCLUDED.refill_cooldown_until,
            data                  = EXCLUDED.data,
            file                  = EXCLUDED.file,
            excel_bytes           = EXCLUDED.excel_bytes,
            updated_at            = NOW()
    """, (
        qkey,
        email.lower(),
        sub.get("submitted", False),
        sub.get("submitted_at", ""),
        sub.get("submitted_at_dt", ""),
        sub.get("user_name", ""),
        sub.get("revision", 0),
        sub.get("refill_requested", False),
        sub.get("refill_reason", ""),
        sub.get("refill_cooldown_until"),
        json.dumps(data) if data is not None else None,
        sub.get("file", ""),
        psycopg2.Binary(excel_bytes) if excel_bytes else None,
    ))


def db_get_submission(qkey: str, email: str) -> dict | None:
    row = _execute(
        "SELECT * FROM wbn_submissions WHERE qkey=%s AND email=%s",
        (qkey, email.lower()), fetch="one"
    )
    if not row:
        return None
    data_raw = row.get("data")
    return {
        "submitted":             row["submitted"],
        "submitted_at":          row["submitted_at"] or "",
        "submitted_at_dt":       row["submitted_at_dt"] or "",
        "user_name":             row["user_name"] or "",
        "revision":              row["revision"] or 0,
        "refill_requested":      row["refill_requested"],
        "refill_reason":         row["refill_reason"] or "",
        "refill_cooldown_until": row["refill_cooldown_until"],
        "data":  data_raw if isinstance(data_raw, dict) else
                 (json.loads(data_raw) if data_raw else None),
        "file":  row["file"] or "",
        "excel_bytes": bytes(row["excel_bytes"]) if row.get("excel_bytes") else None,
    }


def db_get_all_subs_for_quarter(qkey: str) -> dict:
    rows = _execute(
        "SELECT * FROM wbn_submissions WHERE qkey=%s", (qkey,), fetch="all"
    ) or []
    result = {}
    for row in rows:
        data_raw = row.get("data")
        result[row["email"]] = {
            "submitted":             row["submitted"],
            "submitted_at":          row["submitted_at"] or "",
            "submitted_at_dt":       row["submitted_at_dt"] or "",
            "user_name":             row["user_name"] or "",
            "revision":              row["revision"] or 0,
            "refill_requested":      row["refill_requested"],
            "refill_reason":         row["refill_reason"] or "",
            "refill_cooldown_until": row["refill_cooldown_until"],
            "data": data_raw if isinstance(data_raw, dict) else
                    (json.loads(data_raw) if data_raw else None),
            "file": row["file"] or "",
            "excel_bytes": bytes(row["excel_bytes"]) if row.get("excel_bytes") else None,
        }
    return result


# ── Activity log ──────────────────────────────────────────────────────────────
def db_save_log(entry: dict):
    _execute("""
        INSERT INTO wbn_activity_log (timestamp, action, "user", detail)
        VALUES (%s, %s, %s, %s)
    """, (entry.get("timestamp",""), entry.get("action",""),
          entry.get("user",""), entry.get("detail","")))


def db_get_log(limit: int = 50) -> list:
    rows = _execute(
        'SELECT * FROM wbn_activity_log ORDER BY created_at DESC LIMIT %s',
        (limit,), fetch="all"
    ) or []
    return [{"timestamp": r["timestamp"], "action": r["action"],
             "user": r["user"], "detail": r["detail"]} for r in rows]


# ── Feature flags ─────────────────────────────────────────────────────────────
def db_get_feature_flags() -> dict:
    """Returns {flag_key: bool} for all flags."""
    rows = _execute("SELECT flag_key, enabled FROM wbn_feature_flags", fetch="all") or []
    return {r["flag_key"]: r["enabled"] for r in rows}


def db_set_feature_flag(flag_key: str, enabled: bool):
    _execute("""
        INSERT INTO wbn_feature_flags (flag_key, enabled, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (flag_key) DO UPDATE SET
            enabled    = EXCLUDED.enabled,
            updated_at = NOW()
    """, (flag_key, enabled))


# ── Ticker ────────────────────────────────────────────────────────────────────
def db_get_ticker() -> dict:
    """Returns {message, active, style}."""
    row = _execute("SELECT message, active, style FROM wbn_ticker WHERE id=1", fetch="one")
    if not row:
        return {"message": "", "active": False, "style": "info"}
    return {"message": row["message"] or "", "active": row["active"], "style": row["style"] or "info"}


def db_set_ticker(message: str, active: bool, style: str = "info"):
    _execute("""
        UPDATE wbn_ticker
        SET message=%s, active=%s, style=%s, updated_at=NOW()
        WHERE id=1
    """, (message.strip(), active, style))


# ── Auto-init on import ───────────────────────────────────────────────────────
if DB_ENABLED:
    try:
        init_db()
    except Exception as e:
        print(f"  [DB] WARNING: Could not initialise DB — {e}")
        print("  [DB] Falling back to in-memory mode.")
        DB_ENABLED = False
