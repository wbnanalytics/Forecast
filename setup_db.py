#!/usr/bin/env python3
"""
setup_db.py  —  forecast.wbn | One-time database setup script
══════════════════════════════════════════════════════════════
Run this ONCE after you create your PostgreSQL database.
It creates all tables, indexes, and seeds default rows.

Usage:
    python setup_db.py

It reads DATABASE_URL from your .env file automatically.
If DATABASE_URL is not set, it will prompt you for it.
"""

import os, sys

# ── Step 0: Load .env if present ──────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓  Loaded .env")
except ImportError:
    print("⚠  python-dotenv not installed — reading DATABASE_URL from environment only.")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if not DATABASE_URL:
    print("\n  DATABASE_URL not found in .env or environment.")
    DATABASE_URL = input("  Paste your PostgreSQL connection URL: ").strip()
    if not DATABASE_URL:
        print("  ✗  No URL provided. Exiting.")
        sys.exit(1)

# ── Step 1: Test psycopg2 is installed ────────────────────────────────────────
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    print("✓  psycopg2 is installed")
except ImportError:
    print("\n  ✗  psycopg2 is not installed.")
    print("     Run:  pip install psycopg2-binary")
    sys.exit(1)

# ── Step 2: Connect ───────────────────────────────────────────────────────────
print(f"\n  Connecting to database...")
try:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    print("✓  Connected")
except Exception as e:
    print(f"\n  ✗  Connection failed: {e}")
    print("\n  Checklist:")
    print("    • Is the DATABASE_URL correct? (postgresql://user:pass@host:port/dbname)")
    print("    • Is the database server running?")
    print("    • Has the database been created? (CREATE DATABASE wbn_forecast)")
    sys.exit(1)

# ── Step 3: Create tables ─────────────────────────────────────────────────────
SCHEMA = [
    # wbn_quarters
    """
    CREATE TABLE IF NOT EXISTS wbn_quarters (
        qkey           VARCHAR(4) PRIMARY KEY,
        initiated      BOOLEAN DEFAULT FALSE,
        initiated_at   VARCHAR(32),
        drr_data       JSONB,
        channels_found JSONB,
        updated_at     TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    # wbn_submissions
    """
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
    """,
    # wbn_activity_log
    """
    CREATE TABLE IF NOT EXISTS wbn_activity_log (
        id         SERIAL PRIMARY KEY,
        timestamp  VARCHAR(32),
        action     VARCHAR(128),
        "user"     VARCHAR(255),
        detail     TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    # wbn_feature_flags
    """
    CREATE TABLE IF NOT EXISTS wbn_feature_flags (
        flag_key   VARCHAR(64) PRIMARY KEY,
        enabled    BOOLEAN     DEFAULT TRUE,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    # wbn_ticker
    """
    CREATE TABLE IF NOT EXISTS wbn_ticker (
        id         INTEGER PRIMARY KEY DEFAULT 1,
        message    TEXT    DEFAULT '',
        active     BOOLEAN DEFAULT FALSE,
        style      VARCHAR(16) DEFAULT 'info',
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        CHECK (id = 1)
    )
    """,
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_wbn_submissions_qkey       ON wbn_submissions (qkey)",
    "CREATE INDEX IF NOT EXISTS idx_wbn_submissions_email      ON wbn_submissions (email)",
    "CREATE INDEX IF NOT EXISTS idx_wbn_submissions_qkey_email ON wbn_submissions (qkey, email)",
]

SEEDS = [
    # Feature flag defaults
    ("INSERT INTO wbn_feature_flags (flag_key, enabled) VALUES ('load_sample_values', TRUE)  ON CONFLICT (flag_key) DO NOTHING", None),
    ("INSERT INTO wbn_feature_flags (flag_key, enabled) VALUES ('download_template',  TRUE)  ON CONFLICT (flag_key) DO NOTHING", None),
    ("INSERT INTO wbn_feature_flags (flag_key, enabled) VALUES ('upload_excel_fill',  TRUE)  ON CONFLICT (flag_key) DO NOTHING", None),
    # Ticker single row
    ("INSERT INTO wbn_ticker (id, message, active, style) VALUES (1, '', FALSE, 'info') ON CONFLICT (id) DO NOTHING", None),
]

# Migration: if upgrading from v6 (may be missing new columns)
MIGRATIONS = [
    "ALTER TABLE wbn_submissions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE wbn_quarters    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
]

print("\n  Creating tables...")
errors = 0
with conn.cursor() as cur:
    for sql in SCHEMA:
        try:
            cur.execute(sql)
            print("  ✓ ", sql.strip().split('\n')[1].strip()[:60])
        except Exception as e:
            print(f"  ✗  Error: {e}")
            conn.rollback(); errors += 1; continue

    print("\n  Creating indexes...")
    for sql in INDEXES:
        try:
            cur.execute(sql)
            print(f"  ✓  {sql.split('idx_')[1].split(' ')[0] if 'idx_' in sql else sql[:50]}")
        except Exception as e:
            print(f"  ✗  Error: {e}")
            conn.rollback(); errors += 1; continue

    print("\n  Running migrations (safe on fresh DB)...")
    for sql in MIGRATIONS:
        try:
            cur.execute(sql)
            print(f"  ✓  {sql[:70]}")
        except Exception as e:
            print(f"  ⚠  {e} (probably already exists, safe to ignore)")
            conn.rollback()

    print("\n  Seeding defaults...")
    for sql, params in SEEDS:
        try:
            cur.execute(sql, params)
            print(f"  ✓  {sql[:60]}")
        except Exception as e:
            print(f"  ✗  Error: {e}")
            conn.rollback(); errors += 1; continue

conn.commit()
conn.close()

# ── Step 4: Summary ───────────────────────────────────────────────────────────
print("\n" + "─"*54)
if errors == 0:
    print("  ✅  Database setup complete — no errors.")
    print("\n  Tables created:")
    print("    • wbn_quarters       — quarter metadata + DRR data")
    print("    • wbn_submissions    — per-user forecast submissions")
    print("    • wbn_activity_log   — admin audit trail")
    print("    • wbn_feature_flags  — admin-controlled feature toggles")
    print("    • wbn_ticker         — broadcast message for forecast page")
    print("\n  Next steps:")
    print("    1. Make sure DATABASE_URL is set in your .env")
    print("    2. Deploy your app — it will connect automatically")
    print("    3. Visit /admin to set ticker messages and feature flags")
else:
    print(f"  ⚠   Setup completed with {errors} error(s). Review output above.")
print("─"*54)
