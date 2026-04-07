"""
Meridiano CRM — Grupos API
"""

from flask import Blueprint, request, jsonify

from app.auth import login_required, roles_required, current_user
from app.config import ROLE_ADMIN, ROLE_RISK
from app.utils import parse_es_number, cuit_digits, decode_bytes_best_effort, read_csv_dictreader, normalize_header
from app.models.grupo import (
    list_groups, get_group, upsert_group, compute_group_available_map,
)
from app.models.block import blocks_agg_today
from app.services.cartera import load_cartera, load_facturas
from app.database import get_db

bp = Blueprint("grupos", __name__, url_prefix="/api/grupos")


def _payload() -> dict:
    return request.get_json(silent=True) or {}


@bp.route("", methods=["GET"])
@login_required
def list_all():
    _, _, agg, _ = load_cartera()
    _, _, facturas_agg, _ = load_facturas()
    blocks_agg_data, _ = blocks_agg_today()
    group_avail_map = compute_group_available_map(agg=agg, facturas_agg=facturas_agg, blocks_agg=blocks_agg_data)

    rows = list_groups()
    out = []
    for g in rows:
        gid = int(g["id"])
        gi = group_avail_map.get(gid) or {}
        out.append({
            "id": gid,
            "nombre": (g["nombre"] or "").strip(),
            "limite": float(gi.get("limit", g["credit_limit_grupal"] or 0)),
            "used": float(gi.get("used", 0)),
            "blocked": float(gi.get("blocked", 0)),
            "avail": float(gi.get("avail", float(g["credit_limit_grupal"] or 0))),
        })
    out.sort(key=lambda x: (0 if x["avail"] < 0 else 1, x["avail"]))
    return jsonify({"grupos": out, "count": len(out)})


@bp.route("", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def create():
    data = _payload()
    nombre = (data.get("nombre") or "").strip()
    lim = parse_es_number(data.get("limite"))
    ok, msg, gid = upsert_group(nombre, lim, actor=current_user(), allow_update=False)
    return jsonify({"ok": ok, "message": msg, "id": gid}), 201 if ok else 400


@bp.route("/import", methods=["POST"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def import_csv():
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No se seleccionó archivo."}), 400
    try:
        txt = decode_bytes_best_effort(f.read())
        reader = read_csv_dictreader(txt)
        if not reader.fieldnames:
            return jsonify({"ok": False, "error": "CSV inválido: sin encabezados."}), 400
        n_ok = 0
        n_err = 0
        reasons = []
        for idx, row in enumerate(reader, start=2):
            row_norm = {normalize_header(k): v for k, v in (row or {}).items()}
            nombre = (row_norm.get("grupo") or row_norm.get("nombre") or "").strip()
            lim_raw = row_norm.get("limite")
            lim = None if lim_raw is None or str(lim_raw).strip() == "" else parse_es_number(lim_raw)
            if not nombre:
                n_err += 1
                reasons.append(f"Fila {idx}: falta nombre de grupo")
                continue
            ok, msg, _ = upsert_group(nombre, lim, actor=current_user(), allow_update=True)
            if ok:
                n_ok += 1
            else:
                n_err += 1
                reasons.append(f"Fila {idx}: {msg}")
        return jsonify({
            "ok": n_ok > 0,
            "message": f"Importación finalizada. OK={n_ok} · Errores={n_err}.",
            "summary": {"ok": n_ok, "failed": n_err, "reasons": reasons[:20]},
        }), 200 if n_ok > 0 else 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"Error importando CSV: {e}"}), 500


@bp.route("/<int:group_id>/limit", methods=["PUT"])
@roles_required(ROLE_ADMIN, ROLE_RISK)
def update_limit(group_id: int):
    g = get_group(group_id)
    if not g:
        return jsonify({"error": "Grupo no encontrado."}), 404
    data = _payload()
    lim = parse_es_number(data.get("limite"))
    if lim is None:
        return jsonify({"error": "Límite inválido."}), 400
    ok, msg, _ = upsert_group(g["nombre"], lim, actor=current_user(), allow_update=True)
    return jsonify({"ok": ok, "message": "Límite grupal actualizado." if ok else msg})


@bp.route("/<int:group_id>/cartera", methods=["GET"])
@login_required
def group_cartera(group_id: int):
    g = get_group(group_id)
    if not g:
        return jsonify({"error": "Grupo no encontrado."}), 404

    headers, rows, _, status = load_cartera()
    conn = get_db()
    firm_rows = conn.execute("SELECT cuit_digits FROM firmantes WHERE grupo_id=?", (group_id,)).fetchall()
    conn.close()
    cuits = {r["cuit_digits"] for r in firm_rows if r["cuit_digits"]}

    idx_cuit = None
    for i, h in enumerate(headers):
        if (h or "").strip().lower() in ("cuit", "cuit_emisor", "cuitemisor"):
            idx_cuit = i
            break

    filtered = []
    if idx_cuit is not None:
        for r in rows:
            cd = cuit_digits(r[idx_cuit]) if idx_cuit < len(r) else ""
            if cd and cd in cuits:
                filtered.append([str(v) if v is not None else "" for v in r])

    return jsonify({
        "grupo": {"id": g["id"], "nombre": g["nombre"]},
        "headers": headers,
        "rows": filtered,
        "status": status,
        "firmantes_count": len(cuits),
    })
