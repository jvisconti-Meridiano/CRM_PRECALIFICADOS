"""
Meridiano CRM — Firmantes API
"""

import json
import uuid
from flask import Blueprint, request, jsonify

from app.auth import login_required, roles_required, current_user, current_role
from app.config import ROLE_ADMIN, ROLE_RISK, ROLE_SALES, TABLE_IMPORT_BATCHES
from app.utils import (
    cuit_digits, parse_es_number, parse_bool_si, normalize_header,
    effective_line_active, line_status_label, today_str, now_str,
    decode_bytes_best_effort, read_csv_dictreader,
)
from app.models.firmante import (
    get_firmante, list_firmantes, upsert_firmante,
    set_firmante_active, delete_firmante_hard, _KEEP,
)
from app.models.grupo import (
    get_group_by_name, upsert_group, compute_group_available_map,
)
from app.models.block import blocks_agg_today, add_block, delete_block
from app.services.cartera import load_cartera, load_facturas
from app.database import get_db
from app.models.approval import create_approval_request

bp = Blueprint("firmantes", __name__, url_prefix="/api/firmantes")


def _payload() -> dict:
    return request.get_json(silent=True) or {}


def _pick(row_norm: dict, *names):
    for n in names:
        key = normalize_header(n)
        if key in row_norm:
            return row_norm.get(key)
    return None




def _approval_payload(data: dict) -> tuple[bool, str, str]:
    require_approval = bool(data.get("require_approval"))
    approver = (data.get("approver_username") or "").strip()
    if require_approval and current_role() == ROLE_RISK and not approver:
        return False, "Indicá el nombre del aprobador admin.", ""
    return True, "", approver


def _current_snapshot_dict(cd: str) -> dict:
    f = get_firmante(cd)
    return dict(f) if f else {}

def _parse_partial_number(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    n = parse_es_number(s)
    return None if n is None else float(n)


def _parse_firmantes_csv(file_storage) -> tuple[list, list[str]]:
    raw = file_storage.read()
    txt = decode_bytes_best_effort(raw)
    reader = read_csv_dictreader(txt)
    if not reader.fieldnames:
        raise ValueError("CSV inválido: no se detectaron encabezados.")

    rows_in = []
    unknown = set()

    for i, row in enumerate(reader, start=2):
        row_norm = {normalize_header(k): v for k, v in (row or {}).items()}
        rs = (_pick(row_norm, "razon social", "razon_social", "razonsocial", "firmante", "nombre", "razon") or "").strip()
        cuit = (_pick(row_norm, "cuit", "cuil") or "").strip()
        lim3_raw = _pick(row_norm, "lim3", "lim 3", "limite 3", "limite_3", "limite 3ros", "limite credito 3ros", "credito 3ros", "lim_3ros")
        limp_raw = _pick(row_norm, "lim prop", "limprop", "limite prop", "limite propio", "limite credito propio", "credito propio", "lim_propio")
        limf_raw = _pick(row_norm, "lim fce", "limf", "limite fce", "limite credito fce", "credito fce", "lim_fce")
        exp3_raw = _pick(row_norm, "venc 3", "venc3", "vto 3", "vencimiento 3", "vencimiento 3ros", "vto 3ros", "fecha vencimiento 3ros")
        expp_raw = _pick(row_norm, "venc prop", "vencprop", "vto prop", "vencimiento prop", "vencimiento propio", "vto propio", "fecha vencimiento propio")
        group_name = (_pick(row_norm, "grupo", "grupo economico", "grupo_economico", "grupoeconomico") or "").strip()
        primera_linea = parse_bool_si(_pick(row_norm, "primera linea", "primera_linea", "primera linea?", "es primera linea"))

        cd = cuit_digits(cuit)
        if not rs:
            rows_in.append({"_row": i, "_err": "falta Razón Social"})
            continue
        if not cd:
            rows_in.append({"_row": i, "_err": "CUIT inválido/vacío"})
            continue

        gid = _KEEP
        if group_name:
            g = get_group_by_name(group_name)
            if g:
                gid = int(g["id"])
            else:
                unknown.add(group_name)
                gid = {"__unknown__": group_name}

        rows_in.append({
            "_row": i,
            "_err": None,
            "razon_social": rs,
            "cuit": cuit,
            "lim3": _parse_partial_number(lim3_raw),
            "limp": _parse_partial_number(limp_raw),
            "limf": _parse_partial_number(limf_raw),
            "exp3": None if exp3_raw is None or str(exp3_raw).strip() == "" else str(exp3_raw).strip(),
            "expp": None if expp_raw is None or str(expp_raw).strip() == "" else str(expp_raw).strip(),
            "group": gid,
            "primera_linea": primera_linea,
        })

    return rows_in, sorted(list(unknown), key=lambda x: x.lower())


def _apply_import_rows(rows_in: list, *, decisions: dict | None = None) -> tuple[int, int, list[str]]:
    ok_n = 0
    fail_n = 0
    reasons = []
    decisions = decisions or {}
    name_to_id = {}

    for name, d in decisions.items():
        if not d.get("create"):
            continue
        ok, msg, gid = upsert_group(name, d.get("limit", 0.0), actor=current_user(), allow_update=False)
        if not ok and gid is None:
            fail_n += 1
            reasons.append(f"Grupo '{name}': {msg}")
            continue
        if gid is not None:
            name_to_id[name] = int(gid)

    for r in rows_in:
        if r.get("_err"):
            fail_n += 1
            reasons.append(f"Fila {r.get('_row')}: {r.get('_err')}")
            continue

        gid = r.get("group", _KEEP)
        if isinstance(gid, dict) and gid.get("__unknown__"):
            name = gid.get("__unknown__")
            if decisions.get(name, {}).get("create"):
                gid = name_to_id.get(name, _KEEP)
            else:
                gid = None

        ok, msg = upsert_firmante(
            r["razon_social"], r["cuit"],
            r.get("lim3"), r.get("limp"), r.get("limf"),
            r.get("exp3"), r.get("expp"),
            current_user(),
            allow_update=True,
            reactivate=True,
            group_id=gid,
            primera_linea=bool(r.get("primera_linea")),
        )
        if ok:
            ok_n += 1
        else:
            fail_n += 1
            reasons.append(f"Fila {r.get('_row')}: {msg}")
    return ok_n, fail_n, reasons


@bp.route("", methods=["GET"])
@login_required
def list_fp():
    q = (request.args.get("q") or "").strip()
    show_inactive = request.args.get("show_inactive") == "1"
    view_scope = (request.args.get("view_scope") or "3ros").strip().lower()
    if view_scope not in ("3ros", "propio", "fce"):
        view_scope = "3ros"
    group_id_raw = (request.args.get("group_id") or "").strip()
    group_id_int = int(group_id_raw) if group_id_raw.isdigit() else None
    first_line = (request.args.get("first_line") or "all").strip().lower()
    if first_line not in ("all", "yes", "no"):
        first_line = "all"

    _, _, agg, cartera_status = load_cartera()
    _, _, facturas_agg, facturas_status = load_facturas()
    blocks_agg_data, blocks_by_firmante = blocks_agg_today()

    conn_g = get_db()
    g_rows = conn_g.execute("SELECT id, nombre FROM grupos_economicos WHERE is_active=1").fetchall()
    conn_g.close()
    groups_map = {int(r["id"]): (r["nombre"] or "").strip() for r in g_rows}
    group_avail_map = compute_group_available_map(agg=agg, facturas_agg=facturas_agg, blocks_agg=blocks_agg_data)

    rows = list_firmantes(q=q, active_only=not show_inactive, group_id=group_id_int, first_line=first_line)
    today = today_str()
    out = []

    for r in rows:
        cd = (r["cuit_digits"] or "").strip() or cuit_digits(r["cuit"] or "")
        used3 = float((agg or {}).get(cd, {}).get("3ros", 0))
        usedp = float((agg or {}).get(cd, {}).get("propio", 0))
        usedf = float((facturas_agg or {}).get(cd, 0))
        lim3 = float(r["credit_limit_3ros"] or 0)
        limp = float(r["credit_limit_propio"] or 0)
        limf = float(r["credit_limit_fce"] or 0)
        exp3 = (r["limit_expiry_3ros"] or "").strip()
        expp = (r["limit_expiry_propio"] or "").strip()
        line3_active = effective_line_active(r["line_active_3ros"], exp3, today=today)
        linep_active = effective_line_active(r["line_active_propio"], expp, today=today)
        blocked3 = float((blocks_agg_data or {}).get(cd, {}).get("3ros", 0))
        blockedp = float((blocks_agg_data or {}).get(cd, {}).get("propio", 0))
        blockedf = float((blocks_agg_data or {}).get(cd, {}).get("fce", 0))
        avail3 = (lim3 - used3 - blocked3) if line3_active else 0
        availp = (limp - usedp - blockedp) if linep_active else 0
        availf = limf - usedf - blockedf

        gid_int = None
        try:
            gid_int = int(r["grupo_id"]) if r["grupo_id"] is not None and str(r["grupo_id"]).strip() else None
        except (TypeError, ValueError):
            pass
        group_avail = None
        if gid_int is not None:
            gi = group_avail_map.get(gid_int) or {}
            if "avail" in gi:
                group_avail = float(gi["avail"])
                avail3 = min(avail3, group_avail)
                availp = min(availp, group_avail)
                availf = min(availf, group_avail)

        if view_scope == "3ros" and (lim3 <= 0 or (not show_inactive and not line3_active)):
            continue
        if view_scope == "propio" and (limp <= 0 or (not show_inactive and not linep_active)):
            continue
        if view_scope == "fce" and limf <= 0:
            continue

        metric = avail3 if view_scope == "3ros" else (availp if view_scope == "propio" else availf)
        blocks = []
        role = current_role()
        for b in (blocks_by_firmante.get(cd, []) if blocks_by_firmante else []):
            can_delete = not (role == ROLE_SALES and b["username"] != current_user())
            blocks.append({
                "id": b["id"],
                "username": b["username"],
                "scope": b["scope"],
                "amount": float(b["amount"] or 0),
                "can_delete": can_delete,
            })

        out.append({
            "id": r["id"],
            "razon_social": r["razon_social"],
            "cuit": r["cuit"],
            "cuit_digits": cd,
            "is_active": bool(r["is_active"]),
            "primera_linea": bool(r["primera_linea"]),
            "grupo_name": groups_map.get(gid_int, ""),
            "grupo_id": gid_int,
            "lim3": lim3, "limp": limp, "limf": limf,
            "exp3": exp3, "expp": expp,
            "used3": used3, "usedp": usedp, "usedf": usedf,
            "blocked3": blocked3, "blockedp": blockedp, "blockedf": blockedf,
            "avail3": avail3, "availp": availp, "availf": availf,
            "line3_active": line3_active, "linep_active": linep_active,
            "line3_status": line_status_label(r["line_active_3ros"], exp3, today=today),
            "linep_status": line_status_label(r["line_active_propio"], expp, today=today),
            "group_avail": group_avail,
            "metric": metric,
            "blocked_any": blocked3 > 0 or blockedp > 0 or blockedf > 0,
            "blocks": blocks,
        })

    out.sort(key=lambda x: (x["metric"] >= 0, x["metric"]))
    return jsonify({
        "firmantes": out,
        "count": len(out),
        "cartera_status": cartera_status if view_scope != "fce" else (facturas_status or cartera_status),
    })


@bp.route("/<string:cd>", methods=["GET"])
@login_required
def detail(cd: str):
    cd = cuit_digits(cd)
    f = get_firmante(cd)
    if not f:
        return jsonify({"error": "Firmante no encontrado."}), 404
    return jsonify(dict(f))


@bp.route("", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def create():
    data = _payload()
    ok_ap, msg_ap, approver = _approval_payload(data)
    if not ok_ap:
        return jsonify({"ok": False, "error": msg_ap}), 400

    if current_role() == ROLE_RISK and bool(data.get("require_approval")):
        payload = {
            "razon_social": (data.get("razon_social") or "").strip(),
            "cuit": (data.get("cuit") or "").strip(),
            "lim3": data.get("lim3"),
            "limp": data.get("limp"),
            "limf": data.get("limf"),
            "exp3": data.get("exp3") or "",
            "expp": data.get("expp") or "",
            "primera_linea": bool(data.get("primera_linea", False)),
            "observacion": (data.get("observacion") or "").strip(),
        }
        ok, msg, req_id = create_approval_request(
            entity_type="firmante",
            entity_key=payload["cuit"],
            action="create",
            payload=payload,
            requested_by=current_user(),
            requested_to=approver,
            current_snapshot={},
        )
        return jsonify({"ok": ok, "message": msg, "request_id": req_id}), 200 if ok else 400

    ok, msg = upsert_firmante(
        (data.get("razon_social") or "").strip(),
        (data.get("cuit") or "").strip(),
        parse_es_number(data.get("lim3")) or 0,
        parse_es_number(data.get("limp")) or 0,
        parse_es_number(data.get("limf")) or 0,
        data.get("exp3") or "",
        data.get("expp") or "",
        current_user(),
        allow_update=False,
        primera_linea=bool(data.get("primera_linea", False)),
        observacion=(data.get("observacion") or "").strip(),
    )
    return jsonify({"ok": ok, "message": msg}), 201 if ok else 400


@bp.route("/import", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def import_csv():
    file = request.files.get("file")
    if not file:
        return jsonify({"ok": False, "error": "Falta archivo CSV."}), 400
    try:
        rows_in, unknown_groups = _parse_firmantes_csv(file)
        if unknown_groups:
            batch_id = uuid.uuid4().hex
            payload = {"rows": rows_in, "unknown_groups": unknown_groups}
            conn = get_db()
            conn.execute(
                f"INSERT INTO {TABLE_IMPORT_BATCHES}(batch_id, username, payload_json, created_at) VALUES (?, ?, ?, ?)",
                (batch_id, current_user(), json.dumps(payload, ensure_ascii=False), now_str()),
            )
            conn.commit()
            conn.close()
            return jsonify({
                "ok": True,
                "needs_confirmation": True,
                "batch_id": batch_id,
                "unknown_groups": unknown_groups,
                "rows_count": len(rows_in),
            })

        ok_n, fail_n, reasons = _apply_import_rows(rows_in)
        return jsonify({
            "ok": ok_n > 0,
            "message": f"Importación finalizada. OK: {ok_n} | Fallidos: {fail_n}",
            "summary": {"ok": ok_n, "failed": fail_n, "reasons": reasons[:20]},
        }), 200 if ok_n > 0 else 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"Error importando CSV: {e}"}), 500


@bp.route("/import/confirm", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def import_confirm():
    data = _payload()
    batch_id = (data.get("batch_id") or "").strip()
    if not batch_id:
        return jsonify({"ok": False, "error": "Batch inválido."}), 400

    conn = get_db()
    row = conn.execute(
        f"SELECT payload_json FROM {TABLE_IMPORT_BATCHES} WHERE batch_id=? AND username=?",
        (batch_id, current_user()),
    ).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Batch no encontrado o expirado."}), 404
    payload = json.loads(row["payload_json"])
    rows_in = payload.get("rows") or []
    unknown_groups = payload.get("unknown_groups") or []

    decisions_in = data.get("decisions") or []
    decisions = {name: {"create": False, "limit": 0.0} for name in unknown_groups}
    for d in decisions_in:
        name = str(d.get("name") or "").strip()
        if not name or name not in decisions:
            continue
        decisions[name] = {
            "create": bool(d.get("create")),
            "limit": float(parse_es_number(d.get("limit")) or 0),
        }

    ok_n, fail_n, reasons = _apply_import_rows(rows_in, decisions=decisions)
    conn.execute(f"DELETE FROM {TABLE_IMPORT_BATCHES} WHERE batch_id=? AND username=?", (batch_id, current_user()))
    conn.commit()
    conn.close()

    return jsonify({
        "ok": ok_n > 0,
        "message": f"Importación finalizada. OK: {ok_n} | Fallidos: {fail_n}",
        "summary": {"ok": ok_n, "failed": fail_n, "reasons": reasons[:20]},
    }), 200 if ok_n > 0 else 400


@bp.route("/<string:cd>/limits", methods=["PUT"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def update_limits(cd: str):
    cd = cuit_digits(cd)
    f = get_firmante(cd)
    if not f:
        return jsonify({"error": "Firmante no encontrado."}), 404
    data = _payload()

    ok_ap, msg_ap, approver = _approval_payload(data)
    if not ok_ap:
        return jsonify({"ok": False, "error": msg_ap}), 400

    group_name = (data.get("group_name") or "").strip()
    group_id = _KEEP
    if "group_name" in data:
        if not group_name:
            group_id = None
        else:
            g = get_group_by_name(group_name)
            if not g:
                return jsonify({"ok": False, "error": "Grupo no encontrado."}), 404
            group_id = int(g["id"])
    primera_linea = data.get("primera_linea", _KEEP)

    if current_role() == ROLE_RISK and bool(data.get("require_approval")):
        payload = {
            "cuit": f["cuit"],
            "cuit_digits": cd,
            "lim3": data.get("lim3"),
            "limp": data.get("limp"),
            "limf": data.get("limf"),
            "exp3": data.get("exp3", ""),
            "expp": data.get("expp", ""),
            "group_name": data.get("group_name") if "group_name" in data else None,
            "primera_linea": data.get("primera_linea") if "primera_linea" in data else None,
            "observacion": (data.get("observacion") or "").strip(),
        }
        ok, msg, req_id = create_approval_request(
            entity_type="firmante",
            entity_key=cd,
            action="update",
            payload=payload,
            requested_by=current_user(),
            requested_to=approver,
            current_snapshot=_current_snapshot_dict(cd),
        )
        return jsonify({"ok": ok, "message": msg, "request_id": req_id}), 200 if ok else 400

    ok, msg = upsert_firmante(
        f["razon_social"], f["cuit"],
        parse_es_number(data.get("lim3")),
        parse_es_number(data.get("limp")),
        parse_es_number(data.get("limf")),
        data.get("exp3", ""),
        data.get("expp", ""),
        current_user(),
        allow_update=True,
        group_id=group_id,
        primera_linea=primera_linea if primera_linea is not None else _KEEP,
        observacion=(data.get("observacion") or "").strip(),
    )
    return jsonify({"ok": ok, "message": msg})


@bp.route("/<string:cd>/first_line", methods=["PUT"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def set_first_line(cd: str):
    cd = cuit_digits(cd)
    f = get_firmante(cd)
    if not f:
        return jsonify({"error": "Firmante no encontrado."}), 404
    data = _payload()
    ok, msg = upsert_firmante(
        f["razon_social"], f["cuit"],
        lim3=None, limp=None, limf=None, exp3=None, expp=None,
        actor=current_user(), allow_update=True,
        group_id=_KEEP, primera_linea=bool(data.get("primera_linea", False)),
    )
    return jsonify({"ok": ok, "message": "Clasificación actualizada." if ok else msg})


@bp.route("/<string:cd>/group", methods=["PUT"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def set_group(cd: str):
    cd = cuit_digits(cd)
    f = get_firmante(cd)
    if not f:
        return jsonify({"error": "Firmante no encontrado."}), 404
    data = _payload()
    group_name = (data.get("group_name") or "").strip()

    if not group_name or data.get("clear"):
        ok, msg = upsert_firmante(
            f["razon_social"], f["cuit"],
            lim3=None, limp=None, limf=None, exp3=None, expp=None,
            actor=current_user(), allow_update=True, group_id=None,
        )
        return jsonify({"ok": ok, "message": "Grupo quitado." if ok else msg})

    g = get_group_by_name(group_name)
    if not g and data.get("create_if_missing"):
        limit_val = parse_es_number(data.get("group_limit"))
        if limit_val is None:
            return jsonify({"ok": False, "error": "Límite grupal inválido."}), 400
        ok_g, msg_g, gid = upsert_group(group_name, limit_val, actor=current_user(), allow_update=False)
        if not ok_g and gid is None:
            return jsonify({"ok": False, "error": msg_g}), 400
        g = get_group_by_name(group_name)

    if not g:
        return jsonify({"ok": False, "error": "Grupo no encontrado.", "needs_create": True, "group_name": group_name}), 404

    ok, msg = upsert_firmante(
        f["razon_social"], f["cuit"],
        lim3=None, limp=None, limf=None, exp3=None, expp=None,
        actor=current_user(), allow_update=True, group_id=int(g["id"]),
    )
    return jsonify({"ok": ok, "message": "Grupo económico actualizado." if ok else msg})


@bp.route("/<string:cd>/deactivate", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def deactivate(cd: str):
    cd = cuit_digits(cd)
    data = _payload()
    ok_ap, msg_ap, approver = _approval_payload(data)
    if not ok_ap:
        return jsonify({"ok": False, "error": msg_ap}), 400
    if current_role() == ROLE_RISK and bool(data.get("require_approval")):
        ok, msg, req_id = create_approval_request(
            entity_type="firmante",
            entity_key=cd,
            action="deactivate",
            payload={"cuit_digits": cd, "observacion": (data.get("observacion") or "").strip()},
            requested_by=current_user(),
            requested_to=approver,
            current_snapshot=_current_snapshot_dict(cd),
        )
        return jsonify({"ok": ok, "message": msg, "request_id": req_id}), 200 if ok else 400
    ok, msg = set_firmante_active(cd, False, current_user(), observacion=(data.get("observacion") or "").strip())
    return jsonify({"ok": ok, "message": "Firmante desactivado." if ok else msg}), 200 if ok else 400


@bp.route("/<string:cd>/reactivate", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def reactivate(cd: str):
    cd = cuit_digits(cd)
    data = _payload()
    ok_ap, msg_ap, approver = _approval_payload(data)
    if not ok_ap:
        return jsonify({"ok": False, "error": msg_ap}), 400
    if current_role() == ROLE_RISK and bool(data.get("require_approval")):
        ok, msg, req_id = create_approval_request(
            entity_type="firmante",
            entity_key=cd,
            action="reactivate",
            payload={"cuit_digits": cd, "observacion": (data.get("observacion") or "").strip()},
            requested_by=current_user(),
            requested_to=approver,
            current_snapshot=_current_snapshot_dict(cd),
        )
        return jsonify({"ok": ok, "message": msg, "request_id": req_id}), 200 if ok else 400
    ok, msg = set_firmante_active(cd, True, current_user(), observacion=(data.get("observacion") or "").strip())
    return jsonify({"ok": ok, "message": "Firmante reactivado." if ok else msg}), 200 if ok else 400


@bp.route("/<string:cd>", methods=["DELETE"])
@roles_required(ROLE_ADMIN)
def delete(cd: str):
    data = _payload()
    ok, msg = delete_firmante_hard(cuit_digits(cd), current_user(), observacion=(data.get("observacion") or "").strip())
    return jsonify({"ok": ok, "message": msg}), 200 if ok else 400


@bp.route("/<string:cd>/blocks", methods=["POST"])
@login_required
def block_add_route(cd: str):
    cd = cuit_digits(cd)
    f = get_firmante(cd)
    if not f:
        return jsonify({"error": "Firmante no encontrado."}), 404
    data = _payload()
    scope = (data.get("scope") or "").strip().lower()
    if scope not in ("propio", "3ros", "fce"):
        return jsonify({"ok": False, "error": "Elegí Propio, 3ros o FCE."}), 400
    amount = parse_es_number(data.get("amount"))
    if amount is None or amount <= 0:
        return jsonify({"ok": False, "error": "Monto inválido."}), 400
    add_block(cd, scope, current_user(), float(amount))
    return jsonify({"ok": True, "message": "Bloqueo aplicado."})


@bp.route("/blocks/<int:block_id>", methods=["DELETE"])
@login_required
def block_delete_route(block_id: int):
    ok, msg = delete_block(block_id, current_user(), current_role())
    return jsonify({"ok": ok, "message": msg}), 200 if ok else 403
