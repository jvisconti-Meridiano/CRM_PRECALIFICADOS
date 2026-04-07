"""
Meridiano CRM — Alerts API
Control cheques monitor, whitelist de CUIT2 y progreso de corridas.
"""

import threading
from flask import Blueprint, request, jsonify, current_app
from app.auth import roles_required, current_user
from app.config import ROLE_ADMIN, ROLE_RISK, TABLE_ALERTS_WHITELIST, TABLE_MONITOR_SETTINGS, CHEQ_WORKERS
from app.utils import cuit_digits, now_str
from app.services.monitor import (
    run_monitor, progress_get, recent_events, recent_runs,
    _monitoreable_cuits, whitelist_cuit2_set, reset_monitor_runtime_state,
    get_monitor_runtime_settings,
)
from app.database import get_db

bp = Blueprint("alerts", __name__, url_prefix="/api/alerts")

_bg_lock = threading.Lock()
_bg_running = False


def _launch_bg(*, scope: str, scope_cuit: str, alerts: bool) -> tuple[bool, str]:
    global _bg_running
    reset_monitor_runtime_state()
    with _bg_lock:
        if _bg_running:
            return False, "Ya hay una corrida en ejecución."
        _bg_running = True

    app = current_app._get_current_object()

    def _worker():
        global _bg_running
        try:
            with app.app_context():
                run_monitor(scope=scope, scope_cuit=scope_cuit, alert_enabled=alerts)
        finally:
            with _bg_lock:
                _bg_running = False

    threading.Thread(target=_worker, daemon=True, name=f"alerts-bg-{scope}").start()
    return True, "Corrida lanzada."



def _get_monitor_settings() -> dict:
    base = get_monitor_runtime_settings()
    base["local_workers"] = max(1, int(base.get("local_workers") or CHEQ_WORKERS))
    base["lambda_workers"] = max(0, int(base.get("lambda_workers") or 0))
    return base

def _list_whitelist_items() -> list[dict]:
    try:
        conn = get_db()
        rows = conn.execute(
            f"SELECT id, cuit2_digits, label, created_by, created_at FROM {TABLE_ALERTS_WHITELIST} ORDER BY cuit2_digits"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


@bp.route("/stats", methods=["GET"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def stats():
    monitoreables = len(_monitoreable_cuits())
    conn = get_db()
    try:
        total_ev = conn.execute("SELECT COUNT(1) FROM cheq_monitor_events").fetchone()[0] or 0
    except Exception:
        total_ev = 0
    try:
        last_run_row = conn.execute("SELECT started_at, ended_at FROM cheq_monitor_runs ORDER BY id DESC LIMIT 1").fetchone()
        last_run = (last_run_row[1] or last_run_row[0] or "—") if last_run_row else "—"
    except Exception:
        last_run = "—"
    try:
        wl_count = conn.execute(f"SELECT COUNT(1) FROM {TABLE_ALERTS_WHITELIST}").fetchone()[0] or 0
    except Exception:
        wl_count = 0
    conn.close()
    return jsonify({
        "monitoreables": monitoreables,
        "total_events": total_ev,
        "last_run": last_run,
        "whitelist_count": wl_count,
    })


@bp.route("/progress", methods=["GET"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def progress():
    return jsonify(progress_get())


@bp.route("/run", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def run():
    data = request.get_json(silent=True) or {}
    scope = (data.get("scope") or "all").strip().lower()
    scope_cuit = cuit_digits(data.get("cuit") or "")
    if scope == "one" and not scope_cuit:
        return jsonify({"ok": False, "error": "CUIT requerido."}), 400
    ok, msg = _launch_bg(scope=scope, scope_cuit=scope_cuit, alerts=True)
    return jsonify({"ok": ok, "message": msg}), 202 if ok else 409


@bp.route("/sync", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def sync():
    data = request.get_json(silent=True) or {}
    scope = (data.get("scope") or "all").strip().lower()
    scope_cuit = cuit_digits(data.get("cuit") or "")
    if scope == "one" and not scope_cuit:
        return jsonify({"ok": False, "error": "CUIT requerido."}), 400
    ok, msg = _launch_bg(scope=scope, scope_cuit=scope_cuit, alerts=False)
    return jsonify({"ok": ok, "message": msg}), 202 if ok else 409


@bp.route("/events", methods=["GET"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def events():
    limit = min(int(request.args.get("limit") or 80), 500)
    return jsonify({"events": recent_events(limit)})


@bp.route("/runs", methods=["GET"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def runs():
    limit = min(int(request.args.get("limit") or 20), 100)
    return jsonify({"runs": recent_runs(limit)})


@bp.route("/settings", methods=["GET"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def settings_get():
    return jsonify(_get_monitor_settings())


@bp.route("/settings", methods=["PUT"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def settings_put():
    data = request.get_json(silent=True) or {}
    local_workers = max(1, min(16, int(data.get("local_workers") or CHEQ_WORKERS)))
    lambda_enabled = 1 if str(data.get("lambda_enabled") or "").lower() in ("1", "true", "yes", "on") or bool(data.get("lambda_enabled")) else 0
    lambda_workers = max(0, min(16, int(data.get("lambda_workers") or 0)))
    lambda_region = (data.get("lambda_region") or "").strip()[:80]
    lambda_function_name = (data.get("lambda_function_name") or "").strip()[:200]
    try:
        conn = get_db()
        conn.execute(
            f"UPDATE {TABLE_MONITOR_SETTINGS} SET local_workers=?, lambda_enabled=?, lambda_workers=?, lambda_region=?, lambda_function_name=?, updated_at=?, updated_by=? WHERE id=1",
            (local_workers, lambda_enabled, lambda_workers, lambda_region, lambda_function_name, now_str(), current_user() or ""),
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "message": "Configuración de monitoreo actualizada.", **_get_monitor_settings()})
    except Exception as e:
        return jsonify({"ok": False, "error": f"No se pudo guardar la configuración: {e}"}), 500


@bp.route("/runtime/reset", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def runtime_reset():
    global _bg_running
    with _bg_lock:
        if _bg_running:
            return jsonify({"ok": False, "error": "Hay una corrida activa. Esperá a que termine antes de resetear."}), 409
    reset_monitor_runtime_state(force=True)
    return jsonify({"ok": True, "message": "Estado del monitor reseteado."})


@bp.route("/whitelist", methods=["GET"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def whitelist_list():
    return jsonify({"items": _list_whitelist_items(), "cuit2_set": sorted(whitelist_cuit2_set())})


@bp.route("/whitelist", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def whitelist_add():
    data = request.get_json(silent=True) or {}
    cuit2 = cuit_digits(data.get("cuit2") or data.get("cuit2_digits") or "")
    label = (data.get("label") or "").strip()
    if not cuit2:
        return jsonify({"ok": False, "error": "CUIT2 inválido."}), 400
    try:
        conn = get_db()
        conn.execute(
            f"INSERT OR REPLACE INTO {TABLE_ALERTS_WHITELIST}(id, cuit2_digits, label, created_by, created_at) "
            f"VALUES (COALESCE((SELECT id FROM {TABLE_ALERTS_WHITELIST} WHERE cuit2_digits=?), NULL), ?, ?, ?, ?) ",
            (cuit2, cuit2, label, current_user() or "", now_str()),
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "message": "CUIT2 agregado a la whitelist."})
    except Exception as e:
        return jsonify({"ok": False, "error": f"No se pudo guardar la whitelist: {e}"}), 500


@bp.route("/whitelist/<int:item_id>", methods=["DELETE"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def whitelist_delete(item_id: int):
    try:
        conn = get_db()
        cur = conn.execute(f"DELETE FROM {TABLE_ALERTS_WHITELIST} WHERE id=?", (item_id,))
        conn.commit()
        conn.close()
        if cur.rowcount == 0:
            return jsonify({"ok": False, "error": "Registro no encontrado."}), 404
        return jsonify({"ok": True, "message": "CUIT2 quitado de la whitelist."})
    except Exception as e:
        return jsonify({"ok": False, "error": f"No se pudo eliminar: {e}"}), 500
