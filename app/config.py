"""
Meridiano CRM — Configuration
Single source of truth for paths, secrets, roles, and feature flags.
"""

import os
import secrets
import shutil

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_data_dir() -> str:
    """Choose a writable runtime directory.

    Priority:
    1) CRM_DATA_DIR
    2) RAILWAY_VOLUME_MOUNT_PATH (auto-injected by Railway when a volume is attached)
    3) APP_DIR (local development / legacy behavior)
    """
    return os.path.abspath(
        os.environ.get("CRM_DATA_DIR")
        or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
        or APP_DIR
    )


DATA_DIR = _resolve_data_dir()
DB_PATH = os.environ.get("CRM_DB_PATH", os.path.join(DATA_DIR, "crm.db"))
CARTERA_XLSX = os.environ.get("CRM_CARTERA_XLSX", os.path.join(APP_DIR, "cartera.xlsx"))
FACTURAS_XLSX = os.environ.get("CRM_FACTURAS_XLSX", os.path.join(APP_DIR, "Facturas.xlsx"))
SECRET_PATH = os.environ.get("CRM_SECRET_PATH", os.path.join(DATA_DIR, ".secret_key"))

# Seed paths bundled with the repo (used only on first boot when enabled)
SEED_DB_PATH = os.path.join(APP_DIR, "crm.db")
SEED_SECRET_PATH = os.path.join(APP_DIR, ".secret_key")
SEED_FROM_REPO = (os.environ.get("CRM_SEED_FROM_REPO", "1") or "1").strip().lower() not in {"0", "false", "no"}

# ── Roles ──────────────────────────────────────────────────
ROLE_ADMIN = "admin"
ROLE_RISK = "risk"
ROLE_SALES = "sales"

ROLE_LABELS = {
    ROLE_ADMIN: "Admin",
    ROLE_RISK: "Analista de riesgos",
    ROLE_SALES: "Ejecutivo comercial",
}

# ── Integrations ───────────────────────────────────────────
SLACK_WEBHOOK_URL = (
    os.environ.get("CRM_SLACK_WEBHOOK_URL")
    or os.environ.get("SLACK_WEBHOOK_URL")
    or ""
).strip()

BCRA_CHEQ_ENDPOINT = "https://api.bcra.gob.ar/centraldedeudores/v1.0/Deudas/ChequesRechazados"

# ── Monitor tuning ─────────────────────────────────────────
CHEQ_WORKERS = max(1, min(16, int(os.environ.get("CHEQ_WORKERS", "6") or "6")))
CHEQ_LOCK_TTL_SECONDS = 20 * 60
CHEQ_MONITOR_INTERVAL_MINUTES = 30
CHEQ_MONITOR_START_HOUR = int(os.environ.get("CHEQ_MONITOR_START_HOUR", "8") or "8")
BCRA_RATE_LIMIT_REQUESTS = max(1, int(os.environ.get("BCRA_RATE_LIMIT_REQUESTS", "8") or "8"))
BCRA_RATE_LIMIT_WINDOW_SECONDS = max(1, int(os.environ.get("BCRA_RATE_LIMIT_WINDOW_SECONDS", "10") or "10"))
BCRA_RATE_LIMIT_COOLDOWN_SECONDS = max(1, int(os.environ.get("BCRA_RATE_LIMIT_COOLDOWN_SECONDS", "11") or "11"))

# ── Table names (centralised) ─────────────────────────────
TABLE_FIRMANTES = "firmantes"
TABLE_GROUPS = "grupos_economicos"
TABLE_IMPORT_BATCHES = "import_batches"
TABLE_CHEQ_SEEN = "cheq_seen_events"
TABLE_CHEQ_RUNS = "cheq_monitor_runs"
TABLE_CHEQ_LOCK = "cheq_monitor_lock"
TABLE_CHEQ_RAW = "cheq_events_raw"
TABLE_CHEQ_PROGRESS = "cheq_monitor_progress"
TABLE_CHEQ_MONITOR_EVENTS = "cheq_monitor_events"
TABLE_CREDIT_POLICY_CONFIG = "credit_policy_config"
TABLE_EXTRAORDINARY_LIMITS = "extraordinary_limits"
TABLE_APPROVAL_REQUESTS = "approval_requests"
TABLE_ALERTS_WHITELIST = "alerts_cuit2_whitelist"
TABLE_MONITOR_SETTINGS = "monitor_runtime_settings"


def ensure_runtime_storage() -> None:
    """Prepare writable runtime paths and optionally seed persistent files.

    On Railway, when a volume is mounted, DB_PATH and SECRET_PATH should live there.
    If those files do not exist yet and a repo-bundled seed exists, copy it once.
    """
    for target in {DATA_DIR, os.path.dirname(DB_PATH), os.path.dirname(SECRET_PATH)}:
        if target:
            os.makedirs(target, exist_ok=True)

    if not SEED_FROM_REPO:
        return

    try:
        if os.path.abspath(DB_PATH) != os.path.abspath(SEED_DB_PATH):
            if (not os.path.exists(DB_PATH)) and os.path.exists(SEED_DB_PATH):
                shutil.copy2(SEED_DB_PATH, DB_PATH)
        if os.path.abspath(SECRET_PATH) != os.path.abspath(SEED_SECRET_PATH):
            if (not os.path.exists(SECRET_PATH)) and os.path.exists(SEED_SECRET_PATH):
                shutil.copy2(SEED_SECRET_PATH, SECRET_PATH)
    except Exception as e:
        print(f"[config] runtime storage prepare warning: {e}")


def ensure_secret_key() -> str:
    """Load or generate a stable secret key persisted to disk."""
    ensure_runtime_storage()
    if os.path.exists(SECRET_PATH):
        try:
            with open(SECRET_PATH, "r", encoding="utf-8") as f:
                key = f.read().strip()
                if key:
                    return key
        except Exception:
            pass
    key = os.environ.get("CRM_SECRET_KEY") or secrets.token_hex(32)
    try:
        with open(SECRET_PATH, "w", encoding="utf-8") as f:
            f.write(key)
    except Exception:
        pass
    return key
