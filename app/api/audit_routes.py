"""
Meridiano CRM — Audit API
"""

from flask import Blueprint, request, jsonify
from app.auth import login_required, roles_required
from app.config import ROLE_ADMIN
from app.models.audit import list_audit, delete_audit_entry, delete_all_audit

bp = Blueprint("audit", __name__, url_prefix="/api/audit")


@bp.route("", methods=["GET"])
@login_required
def list_all():
    q = (request.args.get("q") or "").strip()
    rows = list_audit(q=q)
    return jsonify({"rows": rows, "count": len(rows)})


@bp.route("/<int:audit_id>", methods=["DELETE"])
@roles_required(ROLE_ADMIN)
def delete_one(audit_id: int):
    delete_audit_entry(audit_id)
    return jsonify({"ok": True, "message": "Registro eliminado."})


@bp.route("/all", methods=["DELETE"])
@roles_required(ROLE_ADMIN)
def delete_everything():
    delete_all_audit()
    return jsonify({"ok": True, "message": "Todos los registros eliminados."})
