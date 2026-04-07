"""
Meridiano CRM — Database layer
Single connection factory.  Schema init + migrations.
"""

import sqlite3
from contextlib import contextmanager

from app.config import (
    DB_PATH, TABLE_FIRMANTES, TABLE_GROUPS, TABLE_IMPORT_BATCHES,
    TABLE_CHEQ_SEEN, TABLE_CHEQ_RUNS, TABLE_CHEQ_LOCK, TABLE_CHEQ_RAW,
    TABLE_CHEQ_PROGRESS, TABLE_CHEQ_MONITOR_EVENTS,
    TABLE_CREDIT_POLICY_CONFIG, TABLE_EXTRAORDINARY_LIMITS,
    TABLE_APPROVAL_REQUESTS, TABLE_ALERTS_WHITELIST, TABLE_MONITOR_SETTINGS,
    CHEQ_WORKERS,
)
from app.utils import cuit_digits, now_str, today_str


# ── Connection factory ─────────────────────────────────────

def get_db(path: str | None = None) -> sqlite3.Connection:
    """Return a configured SQLite connection.  WAL + busy_timeout for
    multi-reader / single-writer concurrency."""
    conn = sqlite3.connect(path or DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def db_session(path: str | None = None):
    """Context-managed connection: auto-closes on exit."""
    conn = get_db(path)
    try:
        yield conn
    finally:
        conn.close()


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


# ── Schema initialisation ─────────────────────────────────

def init_db(path: str | None = None) -> None:
    conn = get_db(path)
    cur = conn.cursor()

    # ── Users ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            pass_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active)")

    # ── Firmantes ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_FIRMANTES} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            razon_social TEXT NOT NULL,
            cuit TEXT NOT NULL,
            cuit_digits TEXT NOT NULL UNIQUE,
            credit_limit_3ros REAL NOT NULL DEFAULT 0,
            credit_limit_propio REAL NOT NULL DEFAULT 0,
            credit_limit_fce REAL NOT NULL DEFAULT 0,
            limit_expiry_3ros TEXT NOT NULL DEFAULT '',
            limit_expiry_propio TEXT NOT NULL DEFAULT '',
            line_active_3ros INTEGER NOT NULL DEFAULT 1,
            line_active_propio INTEGER NOT NULL DEFAULT 1,
            is_active INTEGER NOT NULL DEFAULT 1,
            grupo_id INTEGER,
            primera_linea INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # ── Grupos económicos ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_GROUPS} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL UNIQUE,
            credit_limit_grupal REAL NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # ── Import staging ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_IMPORT_BATCHES} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # ── Audit log ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            firmante_cuit_digits TEXT NOT NULL,
            action TEXT NOT NULL,
            username TEXT NOT NULL,
            details TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # ── Daily blocks ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS limit_blocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            firmante_cuit_digits TEXT NOT NULL,
            scope TEXT NOT NULL,
            username TEXT NOT NULL,
            amount REAL NOT NULL,
            block_date TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # ── Cheques: seen events (SIN FONDOS alerts) ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CHEQ_SEEN} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cuit_digits TEXT NOT NULL,
            event_id TEXT NOT NULL UNIQUE,
            entidad INTEGER,
            nro_cheque TEXT,
            fecha_rechazo TEXT,
            monto REAL,
            pagado INTEGER NOT NULL DEFAULT 0,
            detected_at TEXT NOT NULL
        )
    """)

    # ── Cheques: raw events (ALL causales for audit) ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CHEQ_RAW} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cuit_digits TEXT NOT NULL,
            event_id TEXT NOT NULL UNIQUE,
            causal TEXT,
            entidad INTEGER,
            payload_json TEXT NOT NULL,
            detected_at TEXT NOT NULL
        )
    """)

    # ── Cheques: monitor runs ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CHEQ_RUNS} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_type TEXT NOT NULL,
            scope TEXT NOT NULL,
            scope_cuit TEXT,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            total_cuits INTEGER NOT NULL DEFAULT 0,
            new_events INTEGER NOT NULL DEFAULT 0,
            alerts_sent INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        )
    """)

    # ── Cheques: monitor lock ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CHEQ_LOCK} (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            locked_at TEXT,
            locked_by TEXT,
            expires_at TEXT
        )
    """)
    cur.execute(f"INSERT OR IGNORE INTO {TABLE_CHEQ_LOCK}(id) VALUES (1)")

    # ── Cheques: progress (for UI polling) ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CHEQ_PROGRESS} (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            run_id TEXT,
            mode TEXT,
            status TEXT,
            total INTEGER NOT NULL DEFAULT 0,
            done INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            started_at TEXT,
            updated_at TEXT,
            message TEXT,
            result_json TEXT
        )
    """)
    cur.execute(f"INSERT OR IGNORE INTO {TABLE_CHEQ_PROGRESS}(id, status, total, done, errors) VALUES (1, 'idle', 0, 0, 0)")

    # ── Monitor runtime settings (local/Lambda orchestration) ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_MONITOR_SETTINGS} (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            local_workers INTEGER NOT NULL DEFAULT 1,
            lambda_enabled INTEGER NOT NULL DEFAULT 0,
            lambda_workers INTEGER NOT NULL DEFAULT 0,
            lambda_region TEXT NOT NULL DEFAULT '',
            lambda_function_name TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT ''
        )
    """)
    cur.execute(
        f"INSERT OR IGNORE INTO {TABLE_MONITOR_SETTINGS}(id, local_workers, lambda_enabled, lambda_workers, lambda_region, lambda_function_name, updated_at, updated_by) VALUES (1, ?, 0, 0, '', '', ?, 'system')",
        (CHEQ_WORKERS, now_str()),
    )

    # ── Cheques: monitor events (snapshot-based) ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CHEQ_MONITOR_EVENTS} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            cuit_digits TEXT NOT NULL,
            razon_social TEXT,
            event_id TEXT NOT NULL,
            causal TEXT,
            entidad TEXT,
            nro_cheque TEXT,
            fecha_rechazo TEXT,
            monto REAL,
            pagado INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT,
            detected_at TEXT NOT NULL,
            notified INTEGER NOT NULL DEFAULT 0,
            UNIQUE(cuit_digits, event_id)
        )
    """)

    # ── Alerts whitelist (CUIT2 clientes silenciados) ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ALERTS_WHITELIST} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cuit2_digits TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL DEFAULT '',
            created_by TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)

    # ── Credit policy config ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CREDIT_POLICY_CONFIG} (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            nyps REAL NOT NULL DEFAULT 0,
            pymes REAL NOT NULL DEFAULT 0,
            t1_t2_ar REAL NOT NULL DEFAULT 0,
            t2_directo REAL NOT NULL DEFAULT 0,
            garantizado REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT ''
        )
    """)
    cur.execute(
        f"INSERT OR IGNORE INTO {TABLE_CREDIT_POLICY_CONFIG}"
        f"(id, nyps, pymes, t1_t2_ar, t2_directo, garantizado, updated_at, updated_by)"
        f" VALUES (1, 0, 0, 0, 0, 0, ?, 'system')",
        (now_str(),),
    )

    # ── Extraordinary limits ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_EXTRAORDINARY_LIMITS} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cuit TEXT NOT NULL,
            cuit_digits TEXT NOT NULL,
            razon_social TEXT NOT NULL,
            segmento TEXT NOT NULL,
            limite REAL NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT NOT NULL
        )
    """)


    # ── Approval requests ──
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_APPROVAL_REQUESTS} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            action TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            current_snapshot_json TEXT NOT NULL DEFAULT '',
            requested_by TEXT NOT NULL,
            requested_to TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            reason TEXT NOT NULL DEFAULT '',
            decision_note TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            decided_at TEXT NOT NULL DEFAULT '',
            decided_by TEXT NOT NULL DEFAULT ''
        )
    """)


    # Ensure approval requests table exists on older DBs
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_APPROVAL_REQUESTS} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                action TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                current_snapshot_json TEXT NOT NULL DEFAULT '',
                requested_by TEXT NOT NULL,
                requested_to TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                reason TEXT NOT NULL DEFAULT '',
                decision_note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                decided_at TEXT NOT NULL DEFAULT '',
                decided_by TEXT NOT NULL DEFAULT ''
            )
        """)
    except Exception:
        pass

    conn.commit()

    # ── Indexes (idempotent) ──
    _create_indexes(conn)

    # ── Migrations ──
    _run_migrations(conn)

    conn.close()


def _create_indexes(conn: sqlite3.Connection) -> None:
    """Create all indexes (idempotent)."""
    stmts = [
        f"CREATE INDEX IF NOT EXISTS idx_firmantes_rs ON {TABLE_FIRMANTES}(razon_social)",
        f"CREATE INDEX IF NOT EXISTS idx_firmantes_active ON {TABLE_FIRMANTES}(is_active)",
        f"CREATE INDEX IF NOT EXISTS idx_firmantes_cuitdigits ON {TABLE_FIRMANTES}(cuit_digits)",
        f"CREATE INDEX IF NOT EXISTS idx_firmantes_grupo ON {TABLE_FIRMANTES}(grupo_id)",
        f"CREATE INDEX IF NOT EXISTS idx_firmantes_primera_linea ON {TABLE_FIRMANTES}(primera_linea)",
        f"CREATE INDEX IF NOT EXISTS idx_grupos_nombre ON {TABLE_GROUPS}(nombre)",
        f"CREATE INDEX IF NOT EXISTS idx_grupos_active ON {TABLE_GROUPS}(is_active)",
        f"CREATE INDEX IF NOT EXISTS idx_batches_user ON {TABLE_IMPORT_BATCHES}(username)",
        "CREATE INDEX IF NOT EXISTS idx_audit_firmante ON audit_log(firmante_cuit_digits)",
        "CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_blocks_firmante_date ON limit_blocks(firmante_cuit_digits, block_date)",
        "CREATE INDEX IF NOT EXISTS idx_blocks_scope_date ON limit_blocks(scope, block_date)",
        f"CREATE INDEX IF NOT EXISTS idx_cheq_seen_cuit ON {TABLE_CHEQ_SEEN}(cuit_digits)",
        f"CREATE INDEX IF NOT EXISTS idx_cheq_raw_cuit ON {TABLE_CHEQ_RAW}(cuit_digits)",
        f"CREATE INDEX IF NOT EXISTS idx_cheq_raw_causal ON {TABLE_CHEQ_RAW}(causal)",
        f"CREATE INDEX IF NOT EXISTS idx_cheq_runs_started ON {TABLE_CHEQ_RUNS}(started_at)",
        f"CREATE INDEX IF NOT EXISTS idx_cheq_monitor_events_detected ON {TABLE_CHEQ_MONITOR_EVENTS}(detected_at DESC)",
        f"CREATE INDEX IF NOT EXISTS idx_extra_limits_cuit ON {TABLE_EXTRAORDINARY_LIMITS}(cuit_digits)",
        f"CREATE INDEX IF NOT EXISTS idx_extra_limits_segmento ON {TABLE_EXTRAORDINARY_LIMITS}(segmento)",
        f"CREATE INDEX IF NOT EXISTS idx_approvals_status_to ON {TABLE_APPROVAL_REQUESTS}(status, requested_to)",
        f"CREATE INDEX IF NOT EXISTS idx_approvals_created ON {TABLE_APPROVAL_REQUESTS}(created_at DESC)",
        f"CREATE INDEX IF NOT EXISTS idx_alerts_whitelist_cuit2 ON {TABLE_ALERTS_WHITELIST}(cuit2_digits)",
    ]
    for s in stmts:
        try:
            conn.execute(s)
        except Exception:
            pass
    conn.commit()


def _run_migrations(conn: sqlite3.Connection) -> None:
    """Backfill and migrate data from older schema versions."""
    cols = table_columns(conn, TABLE_FIRMANTES)

    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_ALERTS_WHITELIST} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cuit2_digits TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
        """)
    except Exception:
        pass

    migrations = [
        "credit_limit_3ros REAL NOT NULL DEFAULT 0",
        "credit_limit_propio REAL NOT NULL DEFAULT 0",
        "credit_limit_fce REAL NOT NULL DEFAULT 0",
        "limit_expiry_3ros TEXT NOT NULL DEFAULT ''",
        "limit_expiry_propio TEXT NOT NULL DEFAULT ''",
        "line_active_3ros INTEGER NOT NULL DEFAULT 1",
        "line_active_propio INTEGER NOT NULL DEFAULT 1",
        "is_active INTEGER NOT NULL DEFAULT 1",
        "created_at TEXT NOT NULL DEFAULT ''",
        "updated_at TEXT NOT NULL DEFAULT ''",
        "cuit_digits TEXT NOT NULL DEFAULT ''",
        "grupo_id INTEGER",
        "primera_linea INTEGER NOT NULL DEFAULT 0",
    ]
    for coldef in migrations:
        col_name = coldef.split()[0]
        if col_name not in cols:
            try:
                conn.execute(f"ALTER TABLE {TABLE_FIRMANTES} ADD COLUMN {coldef}")
            except Exception:
                pass

    # Backfill cuit_digits
    for rr in conn.execute(f"SELECT id, cuit, cuit_digits FROM {TABLE_FIRMANTES}").fetchall():
        if not (rr["cuit_digits"] or "").strip():
            cd = cuit_digits(rr["cuit"] or "")
            conn.execute(f"UPDATE {TABLE_FIRMANTES} SET cuit_digits=? WHERE id=?", (cd, rr["id"]))

    # Backfill timestamps
    now = now_str()
    conn.execute(f"UPDATE {TABLE_FIRMANTES} SET created_at=? WHERE created_at=''", (now,))
    conn.execute(f"UPDATE {TABLE_FIRMANTES} SET updated_at=? WHERE updated_at=''", (now,))

    conn.commit()
