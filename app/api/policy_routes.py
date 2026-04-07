"""
Meridiano CRM — Credit policy API
"""

from flask import Blueprint, request, jsonify
from app.auth import login_required, roles_required, current_user, is_admin
from app.config import ROLE_ADMIN
from app.utils import parse_es_number, cuit_digits
from app.models.credit_policy import (
    get_policy_config, update_policy_config,
    list_extraordinary_limits, create_extraordinary_limit,
    update_extraordinary_limit, deactivate_extraordinary_limit,
)

bp = Blueprint("policy", __name__, url_prefix="/api/policy")


@bp.route("", methods=["GET"])
@login_required
def get_config():
    policy = get_policy_config()
    limits = list_extraordinary_limits(active_only=True)
    return jsonify({
        "config": dict(policy) if policy else {},
        "extraordinary": limits,
    })


@bp.route("/config", methods=["PUT"])
@roles_required(ROLE_ADMIN)
def update_config():
    data = request.get_json(silent=True) or {}
    update_policy_config(
        nyps=float(parse_es_number(data.get("nyps")) or 0),
        pymes=float(parse_es_number(data.get("pymes")) or 0),
        t1_t2_ar=float(parse_es_number(data.get("t1_t2_ar")) or 0),
        t2_directo=float(parse_es_number(data.get("t2_directo")) or 0),
        garantizado=float(parse_es_number(data.get("garantizado")) or 0),
        actor=current_user(),
    )
    return jsonify({"ok": True, "message": "Política actualizada."})


@bp.route("/extraordinary", methods=["POST"])
@roles_required(ROLE_ADMIN)
def create_extra():
    data = request.get_json(silent=True) or {}
    ok, msg = create_extraordinary_limit(
        cuit_raw=(data.get("cuit") or "").strip(),
        razon=(data.get("razon_social") or "").strip(),
        segmento=(data.get("segmento") or "").strip(),
        limite=float(parse_es_number(data.get("limite")) or 0),
        actor=current_user(),
    )
    return jsonify({"ok": ok, "message": msg}), 201 if ok else 400


@bp.route("/extraordinary/<int:row_id>", methods=["PUT"])
@roles_required(ROLE_ADMIN)
def update_extra(row_id: int):
    data = request.get_json(silent=True) or {}
    ok, msg = update_extraordinary_limit(
        row_id=row_id,
        cuit_raw=(data.get("cuit") or "").strip(),
        razon=(data.get("razon_social") or "").strip(),
        segmento=(data.get("segmento") or "").strip(),
        limite=float(parse_es_number(data.get("limite")) or 0),
        actor=current_user(),
    )
    return jsonify({"ok": ok, "message": msg})


@bp.route("/extraordinary/<int:row_id>", methods=["DELETE"])
@roles_required(ROLE_ADMIN)
def delete_extra(row_id: int):
    deactivate_extraordinary_limit(row_id, current_user())
    return jsonify({"ok": True, "message": "Límite desactivado."})
