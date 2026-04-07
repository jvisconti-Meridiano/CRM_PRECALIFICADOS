"""
Meridiano CRM — Cheques monitor service
Orchestrates BCRA fetch, DB persistence, and Slack alerts.
Thread-safe progress tracking for UI polling.

Mejoras aplicadas:
- Mensaje Slack incluye monto total pagado (campo que estaba truncado).
- Commit inmediato tras INSERT → garantia fuerte de no-duplicar alertas
  aunque el scheduler dispare dos corridas solapadas.
- Formato Slack legible con bullets (Slack no renderiza tablas Markdown).
- Entidad bancaria incluida por cheque cuando está disponible.
- Modo sync también persiste eventos SIN FONDOS en cheq_seen_events,
  de modo que una corrida de alerta posterior no los re-alerta.
"""

import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from app.config import (
    TABLE_CHEQ_SEEN, TABLE_CHEQ_RUNS, TABLE_CHEQ_LOCK, TABLE_CHEQ_RAW,
    TABLE_CHEQ_PROGRESS, TABLE_CHEQ_MONITOR_EVENTS, TABLE_ALERTS_WHITELIST,
    TABLE_MONITOR_SETTINGS, CHEQ_WORKERS, CHEQ_LOCK_TTL_SECONDS,
)
from app.database import get_db
from app.utils import cuit_digits, now_str, ARG_TZ, fmt_ars, effective_line_active
from app.services import bcra, slack
from app.services.cartera import cartera_al_dia_map, cartera_clientes_map
from app.models.firmante import get_firmante_rs


# ─────────────────────────── progress helpers ────────────────────────────────

def progress_get() -> dict:
    try:
        _progress_autofix_if_stale()
        conn = get_db()
        row = conn.execute(f"SELECT * FROM {TABLE_CHEQ_PROGRESS} WHERE id=1").fetchone()
        conn.close()
        if not row:
            return {"status": "idle", "total": 0, "done": 0, "errors": 0, "message": ""}
        d = dict(row)
        rj = (d.pop("result_json", None) or "").strip()
        d["result"] = json.loads(rj) if rj else None
        return d
    except Exception:
        return {"status": "idle", "total": 0, "done": 0, "errors": 0, "message": ""}


def _progress_set(*, mode: str, status: str, total: int, done: int, errors: int,
                  message: str = "", run_id: str | None = None, result: dict | None = None) -> None:
    try:
        conn = get_db()
        now_s = datetime.now(ARG_TZ).isoformat(timespec="seconds")
        rj = json.dumps(result, ensure_ascii=False) if isinstance(result, dict) else None
        conn.execute(
            f"UPDATE {TABLE_CHEQ_PROGRESS} SET run_id=?, mode=?, status=?, total=?, done=?, errors=?,"
            f" started_at=CASE WHEN ?='running' THEN CASE WHEN run_id=? AND status='running' AND started_at IS NOT NULL THEN started_at ELSE ? END ELSE started_at END,"
            f" updated_at=?, message=?, result_json=COALESCE(?, result_json)"
            f" WHERE id=1",
            (run_id, (mode or "")[:20], (status or "")[:20], int(total or 0), int(done or 0),
             int(errors or 0), (status or "")[:20], run_id, now_s, now_s, (message or "")[:400], rj),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _progress_reset() -> None:
    try:
        conn = get_db()
        conn.execute(
            f"UPDATE {TABLE_CHEQ_PROGRESS} SET run_id=NULL, mode=NULL, status='idle',"
            f" total=0, done=0, errors=0, started_at=NULL, updated_at=NULL, message='', result_json=NULL"
            f" WHERE id=1"
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


# ─────────────────────────── lock helpers ────────────────────────────────────

def _try_lock(locked_by: str = "monitor") -> bool:
    now = datetime.now(ARG_TZ)
    now_s = now.isoformat(timespec="seconds")
    exp_s = (now + timedelta(seconds=CHEQ_LOCK_TTL_SECONDS)).isoformat(timespec="seconds")
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        row = conn.execute(f"SELECT expires_at FROM {TABLE_CHEQ_LOCK} WHERE id=1").fetchone()
        if row:
            exp = (row["expires_at"] or "").strip()
            if exp and exp > now_s:
                conn.execute("ROLLBACK;")
                return False
        conn.execute(
            f"INSERT OR REPLACE INTO {TABLE_CHEQ_LOCK}(id, locked_at, locked_by, expires_at) VALUES (1, ?, ?, ?)",
            (now_s, locked_by[:200], exp_s),
        )
        conn.execute("COMMIT;")
        return True
    except Exception:
        try:
            conn.execute("ROLLBACK;")
        except Exception:
            pass
        return False
    finally:
        conn.close()


def _refresh_lock(locked_by: str = "monitor") -> None:
    try:
        now = datetime.now(ARG_TZ)
        now_s = now.isoformat(timespec="seconds")
        exp_s = (now + timedelta(seconds=CHEQ_LOCK_TTL_SECONDS)).isoformat(timespec="seconds")
        conn = get_db()
        conn.execute(
            f"UPDATE {TABLE_CHEQ_LOCK} SET locked_at=?, locked_by=?, expires_at=? WHERE id=1",
            (now_s, locked_by[:200], exp_s),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _release_lock() -> None:
    try:
        conn = get_db()
        conn.execute(
            f"UPDATE {TABLE_CHEQ_LOCK} SET locked_at=NULL, locked_by=NULL, expires_at=NULL WHERE id=1"
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _lock_is_active() -> bool:
    try:
        conn = get_db()
        row = conn.execute(f"SELECT expires_at FROM {TABLE_CHEQ_LOCK} WHERE id=1").fetchone()
        conn.close()
        if not row:
            return False
        exp = (row["expires_at"] or "").strip()
        return bool(exp and exp > datetime.now(ARG_TZ).isoformat(timespec="seconds"))
    except Exception:
        return False


def _progress_autofix_if_stale() -> None:
    try:
        conn = get_db()
        row = conn.execute(f"SELECT status, updated_at, mode FROM {TABLE_CHEQ_PROGRESS} WHERE id=1").fetchone()
        conn.close()
        if not row or (row["status"] or "") != "running":
            return
        if not _lock_is_active():
            _progress_reset()
            return
        upd = (row["updated_at"] or "").strip()
        if upd:
            dt = datetime.fromisoformat(upd)
            if (datetime.now(ARG_TZ) - dt).total_seconds() >= 180:
                _release_lock()
                _progress_set(mode=(row["mode"] or "alert"), status="error", total=0, done=0, errors=1,
                              message="Se interrumpió la corrida. Reintentá.")
    except Exception:
        pass


def reset_monitor_runtime_state(force: bool = False) -> None:
    """Limpia locks/progreso viejos al arrancar la app o antes de lanzar una corrida."""
    try:
        conn = get_db()
        lock_row = conn.execute(f"SELECT expires_at FROM {TABLE_CHEQ_LOCK} WHERE id=1").fetchone()
        prog_row = conn.execute(f"SELECT status, updated_at FROM {TABLE_CHEQ_PROGRESS} WHERE id=1").fetchone()
        now_iso = datetime.now(ARG_TZ).isoformat(timespec="seconds")

        progress_running = bool(prog_row and (prog_row["status"] or "") == "running")
        lock_active = False
        stale_lock = False
        if lock_row:
            exp = (lock_row["expires_at"] or "").strip()
            if exp and exp > now_iso:
                lock_active = True
            else:
                stale_lock = True

        stale_progress = False
        if progress_running:
            upd = (prog_row["updated_at"] or "").strip()
            if not upd:
                stale_progress = True
            else:
                try:
                    stale_progress = (datetime.now(ARG_TZ) - datetime.fromisoformat(upd)).total_seconds() >= 180
                except Exception:
                    stale_progress = True

        inconsistent_lock = lock_active and not progress_running

        if force or stale_lock or stale_progress or inconsistent_lock:
            conn.execute(f"UPDATE {TABLE_CHEQ_LOCK} SET locked_at=NULL, locked_by=NULL, expires_at=NULL WHERE id=1")
        if force or stale_progress or inconsistent_lock or (stale_lock and progress_running):
            conn.execute(
                f"UPDATE {TABLE_CHEQ_PROGRESS} SET run_id=NULL, mode=NULL, status='idle', total=0, done=0, errors=0,"
                f" started_at=NULL, updated_at=NULL, message='', result_json=NULL WHERE id=1"
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


# ─────────────────────────── universe helpers ────────────────────────────────

def _precal_cuits() -> set[str]:
    from app.database import get_db as _db
    from app.config import TABLE_FIRMANTES

    precal: set[str] = set()
    try:
        conn = _db()
        rows = conn.execute(
            f"SELECT cuit_digits, credit_limit_3ros, credit_limit_propio, credit_limit_fce,"
            f" line_active_3ros, line_active_propio, limit_expiry_3ros, limit_expiry_propio, is_active"
            f" FROM {TABLE_FIRMANTES}"
        ).fetchall()
        conn.close()
        for r in rows:
            cd = (r["cuit_digits"] or "").strip()
            if not cd or int(r["is_active"] or 0) != 1:
                continue
            lim3 = float(r["credit_limit_3ros"] or 0)
            limp = float(r["credit_limit_propio"] or 0)
            limf = float(r["credit_limit_fce"] or 0)
            l3a = effective_line_active(r["line_active_3ros"], r["limit_expiry_3ros"] or "")
            lpa = effective_line_active(r["line_active_propio"], r["limit_expiry_propio"] or "")
            if (lim3 > 0 and l3a) or (limp > 0 and lpa) or limf > 0:
                precal.add(cd)
    except Exception:
        pass
    return precal


def whitelist_cuit2_set() -> set[str]:
    try:
        conn = get_db()
        rows = conn.execute(f"SELECT cuit2_digits FROM {TABLE_ALERTS_WHITELIST}").fetchall()
        conn.close()
        return {str(r["cuit2_digits"] or "").strip() for r in rows if str(r["cuit2_digits"] or "").strip()}
    except Exception:
        return set()


def monitor_source_map() -> dict[str, dict]:
    precal = _precal_cuits()
    cartera_map = cartera_al_dia_map()
    cartera_clients = cartera_clientes_map()
    whitelist = whitelist_cuit2_set()

    all_cuits = set(precal) | set(cartera_map.keys()) | set(cartera_clients.keys())
    out: dict[str, dict] = {}
    for cd in all_cuits:
        has_cartera = cd in cartera_map or cd in cartera_clients
        clients = set(cartera_clients.get(cd) or set())
        if has_cartera and not clients:
            has_non_whitelisted = True
        else:
            has_non_whitelisted = any((not c2) or (c2 not in whitelist) for c2 in clients)
        cartera_only_whitelisted = bool(has_cartera and clients and not has_non_whitelisted and cd not in precal)
        out[cd] = {
            "precal": cd in precal,
            "has_cartera": has_cartera,
            "cartera_clients": sorted(clients),
            "cartera_only_whitelisted": cartera_only_whitelisted,
            "has_non_whitelisted_clients": bool(has_non_whitelisted),
        }
    return out


def _monitoreable_cuits() -> list[str]:
    return sorted(monitor_source_map().keys())


# ─────────────────────────── Slack message builder ───────────────────────────


def get_monitor_runtime_settings() -> dict:
    out = {
        "local_workers": CHEQ_WORKERS,
        "lambda_enabled": False,
        "lambda_workers": 0,
        "lambda_region": "",
        "lambda_function_name": "",
    }
    try:
        conn = get_db()
        row = conn.execute(
            f"SELECT local_workers, lambda_enabled, lambda_workers, lambda_region, lambda_function_name FROM {TABLE_MONITOR_SETTINGS} WHERE id=1"
        ).fetchone()
        conn.close()
        if not row:
            return out
        out.update({
            "local_workers": max(1, int(row["local_workers"] or CHEQ_WORKERS)),
            "lambda_enabled": bool(row["lambda_enabled"]),
            "lambda_workers": max(0, int(row["lambda_workers"] or 0)),
            "lambda_region": (row["lambda_region"] or "").strip(),
            "lambda_function_name": (row["lambda_function_name"] or "").strip(),
        })
    except Exception:
        pass
    return out

def _build_slack_message(title: str, cd: str, new_items: list[dict]) -> str:
    """
    Construye el mensaje Slack para nuevos eventos SIN FONDOS.

    Incluye:
    - Encabezado con razón social y CUIT.
    - Totales: cantidad, monto total rechazado, monto ya pagado, monto impago.
    - Lista de hasta 10 cheques con fecha, monto, estado de pago y entidad bancaria.

    Usa bullets (Slack no renderiza tablas Markdown).
    """
    total_monto = sum(float(e.get("monto") or 0) for e in new_items)
    total_pagado = sum(float(e.get("monto") or 0) for e in new_items if e.get("pagado"))
    total_impago = total_monto - total_pagado

    lines = [
        f"*:rotating_light: Cheques rechazados SIN FONDOS* — *{title}* (`{cd}`)",
        (
            f"Nuevos eventos: *{len(new_items)}*  |  "
            f"Monto total: *{fmt_ars(total_monto)}*  |  "
            f"Ya pagado: *{fmt_ars(total_pagado)}*  |  "
            f"Impago: *{fmt_ars(total_impago)}*"
        ),
        "",
    ]

    # Hasta 10 cheques, del más reciente al más antiguo
    muestra = sorted(
        new_items,
        key=lambda x: (x.get("fecha_rechazo") or "", float(x.get("monto") or 0)),
        reverse=True,
    )[:10]

    for e in muestra:
        fecha = e.get("fecha_rechazo") or "—"
        monto = fmt_ars(float(e.get("monto") or 0))
        pagado_str = ":white_check_mark: Pagado" if e.get("pagado") else ":x: Impago"
        entidad = e.get("entidad")
        entidad_str = f"  |  Ent. {entidad}" if entidad is not None else ""
        lines.append(f"• {fecha}  |  {monto}  |  {pagado_str}{entidad_str}")

    extra = len(new_items) - len(muestra)
    if extra > 0:
        lines.append(f"_… y {extra} cheque{'s' if extra > 1 else ''} más_")

    return "\n".join(lines)


# ─────────────────────────── persistence helpers ─────────────────────────────

def _persist_sin_fondos_events(conn_w, cd: str, title: str, run_id, events: list[dict]) -> list[dict]:
    """
    Inserta eventos SIN FONDOS en cheq_seen_events y cheq_monitor_events.

    Hace COMMIT inmediatamente después de los inserts: esto garantiza que
    corridas solapadas (ej. scheduler + manual) vean los registros antes de
    decidir si deben alertar, evitando duplicados de notificación.

    Devuelve los eventos que son *nuevos* (rowcount == 1 en cheq_seen_events).
    """
    new_items: list[dict] = []

    for e in events:
        eid = bcra.cheq_event_id(cd, e)

        # cheq_seen_events — fuente de verdad para deduplicación
        try:
            cur = conn_w.execute(
                f"INSERT OR IGNORE INTO {TABLE_CHEQ_SEEN}"
                f"(cuit_digits, event_id, entidad, nro_cheque, fecha_rechazo, monto, pagado, detected_at)"
                f" VALUES (?,?,?,?,?,?,?,?)",
                (cd, eid, e.get("entidad"), e.get("nro_cheque"), e.get("fecha_rechazo"),
                 float(e.get("monto") or 0), 1 if e.get("pagado") else 0, now_str()),
            )
            inserted = (cur.rowcount == 1)
        except Exception:
            inserted = False

        # cheq_monitor_events — historial enriquecido para la UI
        try:
            conn_w.execute(
                f"INSERT OR REPLACE INTO {TABLE_CHEQ_MONITOR_EVENTS}"
                f"(run_id, cuit_digits, razon_social, event_id, causal, entidad, nro_cheque,"
                f" fecha_rechazo, monto, pagado, payload_json, detected_at, notified)"
                f" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,"
                f"  COALESCE((SELECT notified FROM {TABLE_CHEQ_MONITOR_EVENTS}"
                f"            WHERE cuit_digits=? AND event_id=?), 0))",
                (
                    run_id, cd, title, eid,
                    e.get("causal") or "SIN FONDOS",
                    str(e.get("entidad") or ""),
                    e.get("nro_cheque"), e.get("fecha_rechazo"),
                    float(e.get("monto") or 0), 1 if e.get("pagado") else 0,
                    json.dumps(e.get("payload_json") or {}, ensure_ascii=False),
                    now_str(),
                    cd, eid,
                ),
            )
        except Exception:
            pass

        if inserted:
            new_items.append(e)

    # Commit inmediato para que otras corridas concurrentes vean los registros
    try:
        conn_w.commit()
    except Exception:
        pass

    return new_items


# ─────────────────────────── main entry point ────────────────────────────────

def run_monitor(*, scope: str = "all", scope_cuit: str = "", alert_enabled: bool = True) -> dict:
    stats = {
        "ok": True, "scope": scope, "scope_cuit": scope_cuit,
        "started_at": now_str(), "ended_at": "",
        "total_cuits": 0, "new_events": 0, "alerts_sent": 0, "errors": 0,
    }
    mode = "sync" if not alert_enabled else "alert"
    run_uuid = str(uuid.uuid4())

    if not _try_lock(f"monitor:{run_uuid}"):
        _progress_set(mode=mode, status="error", total=0, done=0, errors=1,
                      message="Ya hay una corrida en ejecución.", run_id=run_uuid)
        return {**stats, "ok": False, "errors": 1, "ended_at": now_str()}

    _progress_set(mode=mode, status="running", total=0, done=0, errors=0,
                  message="Preparando…", run_id=run_uuid)

    run_id = None
    conn_w = None
    try:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute(
                f"INSERT INTO {TABLE_CHEQ_RUNS}(run_type, scope, scope_cuit, started_at) VALUES (?,?,?,?)",
                ("sync" if not alert_enabled else "monitor", scope, scope_cuit or None, stats["started_at"]),
            )
            run_id = cur.lastrowid
            conn.commit()
            conn.close()
        except Exception:
            pass

        source_map = monitor_source_map()
        worker_cfg = get_monitor_runtime_settings()
        local_workers = max(1, int(worker_cfg.get("local_workers") or CHEQ_WORKERS))
        if scope in ("single", "one"):
            cd1 = cuit_digits(scope_cuit)
            cuits = [cd1] if cd1 else []
        else:
            cuits = sorted(source_map.keys())

        stats["total_cuits"] = len(cuits)
        _progress_set(mode=mode, status="running", total=len(cuits), done=0, errors=0,
                      message=f"Iniciando (0/{len(cuits)})", run_id=run_uuid)

        from app.models.firmante import firmantes_rs_map
        rs_map = firmantes_rs_map(cuits) if cuits else {}

        err_samples: list[str] = []
        done_count = 0
        counter_lock = threading.Lock()

        def _fetch_one(cd: str):
            return cd, bcra.fetch_cheques_all(cd)

        conn_w = get_db()
        try:
            conn_w.execute("PRAGMA temp_store=MEMORY;")
        except Exception:
            pass

        with ThreadPoolExecutor(max_workers=local_workers) as ex:
            fut_map = {ex.submit(_fetch_one, cd): cd for cd in cuits}
            for fut in as_completed(fut_map):
                _refresh_lock(f"monitor:{run_uuid}")
                cd = fut_map[fut]

                try:
                    cd, bcra_all = fut.result()
                except Exception as e:
                    with counter_lock:
                        stats["errors"] += 1
                        done_count += 1
                        if len(err_samples) < 10:
                            err_samples.append(f"{cd}: {e}")
                    _progress_set(mode=mode, status="running", total=stats["total_cuits"],
                                  done=done_count, errors=stats["errors"],
                                  message=f"Error interno: {cd}", run_id=run_uuid)
                    continue

                if bcra_all.get("error"):
                    with counter_lock:
                        stats["errors"] += 1
                        done_count += 1
                        if len(err_samples) < 10:
                            err_samples.append(f"{cd}: {bcra_all['error']}")
                    _progress_set(mode=mode, status="running", total=stats["total_cuits"],
                                  done=done_count, errors=stats["errors"],
                                  message=f"Error BCRA: {cd}", run_id=run_uuid)
                    continue

                # Persistir eventos crudos (todas las causales) siempre
                for ev in (bcra_all.get("events") or []):
                        causal = (ev or {}).get("causal") or ""
                        entidad_raw = (ev or {}).get("entidad")
                        detalle_raw = (ev or {}).get("detalle_raw") or {}
                        eid_raw = bcra.cheq_raw_event_id(cd, str(causal), entidad_raw, detalle_raw)
                        try:
                            conn_w.execute(
                                f"INSERT OR IGNORE INTO {TABLE_CHEQ_RAW}"
                                f"(cuit_digits, event_id, causal, entidad, payload_json, detected_at)"
                                f" VALUES (?,?,?,?,?,?)",
                                (cd, eid_raw, str(causal), entidad_raw,
                                 json.dumps(detalle_raw, ensure_ascii=False), now_str()),
                            )
                        except Exception:
                            pass

                # Filtrar SIN FONDOS y persistir siempre
                events = bcra.extract_sin_fondos(bcra_all)
                title = (
                    rs_map.get(cd)
                    or get_firmante_rs(cd)
                    or (bcra_all.get("denominacion") or "").strip()
                    or cd
                )

                # commit inmediato dentro de _persist_sin_fondos_events
                new_items = _persist_sin_fondos_events(conn_w, cd, title, run_id, events)

                with counter_lock:
                    done_count += 1
                    if new_items:
                        stats["new_events"] += len(new_items)

                # Alertar SOLO si hay eventos nuevos, modo alert y no está silenciado por whitelist CUIT2
                src = source_map.get(cd) or {}
                suppress_slack = bool(src.get("cartera_only_whitelisted")) and not bool(src.get("precal"))
                if new_items and alert_enabled and not suppress_slack:
                    msg = _build_slack_message(title, cd, new_items)
                    sent = False
                    try:
                        sent = slack.send_message(msg)
                    except Exception:
                        sent = False
                    if sent:
                        with counter_lock:
                            stats["alerts_sent"] += 1
                        try:
                            eids = [bcra.cheq_event_id(cd, item) for item in new_items]
                            conn_w.execute(
                                f"UPDATE {TABLE_CHEQ_MONITOR_EVENTS} SET notified=1"
                                f" WHERE cuit_digits=? AND event_id IN ({','.join(['?']*len(eids))})",
                                tuple([cd] + eids),
                            )
                            conn_w.commit()
                        except Exception:
                            pass

                _progress_set(mode=mode, status="running", total=stats["total_cuits"],
                              done=done_count, errors=stats["errors"],
                              message=f"[{done_count}/{stats['total_cuits']}] {cd}",
                              run_id=run_uuid)

        stats["ended_at"] = now_str()
        if conn_w is not None:
            conn_w.commit()

        if run_id is not None:
            try:
                conn = get_db()
                conn.execute(
                    f"UPDATE {TABLE_CHEQ_RUNS} SET ended_at=?, total_cuits=?, new_events=?,"
                    f" alerts_sent=?, errors=?, notes=? WHERE id=?",
                    (stats["ended_at"], stats["total_cuits"], stats["new_events"],
                     stats["alerts_sent"], stats["errors"],
                     " ; ".join(err_samples[:10]) if err_samples else None, run_id),
                )
                conn.commit()
                conn.close()
            except Exception:
                pass

        final_status = "done" if stats["ok"] else "error"
        _progress_set(mode=mode, status=final_status, total=stats["total_cuits"],
                      done=stats["total_cuits"], errors=stats["errors"],
                      message="Listo." if stats["ok"] else "Finalizó con errores.",
                      run_id=run_uuid, result=stats)
        return stats

    except Exception as e:
        stats["ok"] = False
        stats["errors"] += 1
        stats["ended_at"] = now_str()
        if run_id is not None:
            try:
                conn = get_db()
                conn.execute(
                    f"UPDATE {TABLE_CHEQ_RUNS} SET ended_at=?, total_cuits=?, new_events=?, alerts_sent=?, errors=?, notes=? WHERE id=?",
                    (stats["ended_at"], stats["total_cuits"], stats["new_events"], stats["alerts_sent"], stats["errors"], str(e)[:500], run_id),
                )
                conn.commit()
                conn.close()
            except Exception:
                pass
        _progress_set(mode=mode, status="error", total=stats["total_cuits"],
                      done=stats.get("total_cuits", 0), errors=stats["errors"],
                      message=f"Finalizó con error: {e}", run_id=run_uuid, result=stats)
        return stats
    finally:
        try:
            if conn_w is not None:
                conn_w.commit()
                conn_w.close()
        except Exception:
            pass
        _release_lock()


# ─────────────────────────── query helpers (UI) ──────────────────────────────

def recent_events(limit: int = 80) -> list[dict]:
    conn = get_db()
    rows = conn.execute(
        f"SELECT detected_at, cuit_digits, razon_social, fecha_rechazo, nro_cheque,"
        f" monto, pagado, causal, notified"
        f" FROM {TABLE_CHEQ_MONITOR_EVENTS} ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def recent_runs(limit: int = 20) -> list[dict]:
    conn = get_db()
    rows = conn.execute(
        f"SELECT * FROM {TABLE_CHEQ_RUNS} ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
