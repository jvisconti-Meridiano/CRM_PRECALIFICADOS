
"""
Meridiano CRM — Approval requests model
"""

import json
from app.database import get_db
from app.config import TABLE_APPROVAL_REQUESTS
from app.utils import now_str, cuit_digits, parse_es_number
from app.models.audit import audit_log, append_observation
from app.models.user import get_active_admin
from app.models.firmante import get_firmante, upsert_firmante, set_firmante_active, delete_firmante_hard, _KEEP
from app.models.grupo import get_group_by_name


def _json_load(s, fallback):
    try:
        return json.loads(s) if s else fallback
    except Exception:
        return fallback


def _serialize_snapshot(row):
    if not row:
        return {}
    d = dict(row)
    return {
        "razon_social": d.get("razon_social"),
        "cuit": d.get("cuit"),
        "cuit_digits": d.get("cuit_digits"),
        "lim3": d.get("credit_limit_3ros"),
        "limp": d.get("credit_limit_propio"),
        "limf": d.get("credit_limit_fce"),
        "exp3": d.get("limit_expiry_3ros"),
        "expp": d.get("limit_expiry_propio"),
        "primera_linea": bool(d.get("primera_linea")),
        "grupo_id": d.get("grupo_id"),
        "is_active": bool(d.get("is_active")),
    }


def create_approval_request(*, entity_type: str, entity_key: str, action: str, payload: dict,
                            requested_by: str, requested_to: str, reason: str = "",
                            current_snapshot: dict | None = None) -> tuple[bool, str, int | None]:
    admin = get_active_admin(requested_to)
    if not admin:
        return False, "El aprobador indicado no existe o no es un admin activo.", None

    conn = get_db()
    cur = conn.execute(
        f"""INSERT INTO {TABLE_APPROVAL_REQUESTS}
            (entity_type, entity_key, action, payload_json, current_snapshot_json, requested_by,
             requested_to, status, reason, decision_note, created_at, decided_at, decided_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, '', ?, '', '')""",
        (
            entity_type, entity_key, action,
            json.dumps(payload or {}, ensure_ascii=False),
            json.dumps(current_snapshot or {}, ensure_ascii=False),
            requested_by, requested_to, (reason or "").strip(), now_str(),
        ),
    )
    req_id = cur.lastrowid
    conn.commit()
    conn.close()

    if entity_type == "firmante":
        cd = cuit_digits(entity_key) or (payload or {}).get("cuit_digits") or cuit_digits((payload or {}).get("cuit") or "")
        who = (payload or {}).get("razon_social") or cd
        audit_log(cd or "pending", "SOLICITUD", requested_by,
                  f"Solicitud {action} enviada a {requested_to} para {who}.")
    return True, "Solicitud enviada para aprobación.", req_id


def get_request(req_id: int):
    conn = get_db()
    row = conn.execute(f"SELECT * FROM {TABLE_APPROVAL_REQUESTS} WHERE id=?", (req_id,)).fetchone()
    conn.close()
    return _hydrate(row) if row else None


def list_requests(*, status: str = "", requested_to: str = "", requested_by: str = "") -> list[dict]:
    where = []
    params = []
    if status:
        where.append("status=?")
        params.append(status)
    if requested_to:
        where.append("requested_to=?")
        params.append(requested_to)
    if requested_by:
        where.append("requested_by=?")
        params.append(requested_by)
    sql = f"SELECT * FROM {TABLE_APPROVAL_REQUESTS}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, id DESC"
    conn = get_db()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_hydrate(r) for r in rows]


def pending_count(username: str) -> int:
    conn = get_db()
    row = conn.execute(
        f"SELECT COUNT(*) AS n FROM {TABLE_APPROVAL_REQUESTS} WHERE status='pending' AND requested_to=?",
        (username,),
    ).fetchone()
    conn.close()
    return int(row["n"] or 0)


def approve_request(req_id: int, approver: str, note: str = "") -> tuple[bool, str]:
    conn = get_db()
    row = conn.execute(
        f"SELECT * FROM {TABLE_APPROVAL_REQUESTS} WHERE id=? AND status='pending' AND requested_to=?",
        (req_id, approver),
    ).fetchone()
    if not row:
        conn.close()
        return False, "Solicitud no encontrada o ya resuelta."

    req = _hydrate(row)
    ok, msg = _apply_request(req, approver, note)
    if not ok:
        conn.close()
        return False, msg

    conn.execute(
        f"UPDATE {TABLE_APPROVAL_REQUESTS} SET status='approved', decision_note=?, decided_at=?, decided_by=? WHERE id=?",
        ((note or "").strip(), now_str(), approver, req_id),
    )
    conn.commit()
    conn.close()
    return True, "Solicitud aprobada."


def reject_request(req_id: int, approver: str, note: str = "") -> tuple[bool, str]:
    conn = get_db()
    row = conn.execute(
        f"SELECT * FROM {TABLE_APPROVAL_REQUESTS} WHERE id=? AND status='pending' AND requested_to=?",
        (req_id, approver),
    ).fetchone()
    if not row:
        conn.close()
        return False, "Solicitud no encontrada o ya resuelta."
    req = _hydrate(row)
    conn.execute(
        f"UPDATE {TABLE_APPROVAL_REQUESTS} SET status='rejected', decision_note=?, decided_at=?, decided_by=? WHERE id=?",
        ((note or "").strip(), now_str(), approver, req_id),
    )
    conn.commit()
    conn.close()

    if req["entity_type"] == "firmante":
        cd = cuit_digits(req["entity_key"]) or cuit_digits((req["payload"] or {}).get("cuit") or "")
        audit_log(cd or "pending", "RECHAZO", approver,
                  append_observation(f"Solicitud {req['action']} rechazada. Solicitado por {req['requested_by']}.", note))
    return True, "Solicitud rechazada."


def _hydrate(row) -> dict:
    d = dict(row)
    d["payload"] = _json_load(d.get("payload_json"), {})
    d["current_snapshot"] = _json_load(d.get("current_snapshot_json"), {})
    return d


def _resolve_group_id(group_name: str | None):
    name = (group_name or "").strip()
    if not name:
        return None
    g = get_group_by_name(name)
    return int(g["id"]) if g else None


def _apply_request(req: dict, approver: str, note: str = "") -> tuple[bool, str]:
    payload = req.get("payload") or {}
    action = (req.get("action") or "").strip().lower()
    entity_key = req.get("entity_key") or ""
    requested_by = req.get("requested_by") or ""
    approval_obs = f"Solicitado por: {requested_by} | Aprobado por: {approver}"
    if note:
        approval_obs += f" | Nota aprobación: {(note or '').strip()}"

    if req.get("entity_type") != "firmante":
        return False, "Tipo de solicitud no soportado."

    if action == "create":
        return upsert_firmante(
            (payload.get("razon_social") or "").strip(),
            (payload.get("cuit") or "").strip(),
            parse_es_number(payload.get("lim3")) or 0,
            parse_es_number(payload.get("limp")) or 0,
            parse_es_number(payload.get("limf")) or 0,
            payload.get("exp3") or "",
            payload.get("expp") or "",
            requested_by,
            allow_update=False,
            primera_linea=bool(payload.get("primera_linea", False)),
            observacion=append_observation(payload.get("observacion") or "", approval_obs),
        )

    cd = cuit_digits(entity_key) or cuit_digits(payload.get("cuit") or "")
    f = get_firmante(cd)
    if not f:
        return False, "Firmante no encontrado."

    if action == "update":
        group_name = (payload.get("group_name") or "").strip()
        group_id = _KEEP
        if "group_name" in payload:
            if not group_name:
                group_id = None
            else:
                gid = _resolve_group_id(group_name)
                if gid is None:
                    return False, f"El grupo '{group_name}' no existe."
                group_id = gid
        primera = payload.get("primera_linea", _KEEP)
        return upsert_firmante(
            f["razon_social"], f["cuit"],
            parse_es_number(payload.get("lim3")),
            parse_es_number(payload.get("limp")),
            parse_es_number(payload.get("limf")),
            payload.get("exp3", ""),
            payload.get("expp", ""),
            requested_by,
            allow_update=True,
            group_id=group_id,
            primera_linea=primera if primera is not None else _KEEP,
            observacion=append_observation(payload.get("observacion") or "", approval_obs),
        )

    if action == "deactivate":
        return set_firmante_active(cd, False, requested_by, observacion=append_observation(payload.get("observacion") or "", approval_obs))

    if action == "reactivate":
        return set_firmante_active(cd, True, requested_by, observacion=append_observation(payload.get("observacion") or "", approval_obs))

    if action == "delete":
        return delete_firmante_hard(cd, requested_by, observacion=append_observation(payload.get("observacion") or "", approval_obs))

    return False, "Acción de solicitud no soportada."
