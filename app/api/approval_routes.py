
"""
Meridiano CRM — Approval requests API
"""

from flask import Blueprint, jsonify, request
from app.auth import roles_required, current_user
from app.config import ROLE_ADMIN
from app.models.approval import list_requests, pending_count, approve_request, reject_request

bp = Blueprint("approvals", __name__, url_prefix="/api/approvals")


@bp.route("/pending-count", methods=["GET"])
@roles_required(ROLE_ADMIN)
def pending_count_route():
    return jsonify({"count": pending_count(current_user())})


@bp.route("", methods=["GET"])
@roles_required(ROLE_ADMIN)
def list_route():
    status = (request.args.get("status") or "").strip().lower()
    rows = list_requests(status=status or "", requested_to=current_user())
    return jsonify({"rows": rows, "count": len(rows)})


@bp.route("/<int:req_id>/approve", methods=["POST"])
@roles_required(ROLE_ADMIN)
def approve_route(req_id: int):
    data = request.get_json(silent=True) or {}
    ok, msg = approve_request(req_id, current_user(), note=(data.get("note") or "").strip())
    return jsonify({"ok": ok, "message": msg}), 200 if ok else 400


@bp.route("/<int:req_id>/reject", methods=["POST"])
@roles_required(ROLE_ADMIN)
def reject_route(req_id: int):
    data = request.get_json(silent=True) or {}
    ok, msg = reject_request(req_id, current_user(), note=(data.get("note") or "").strip())
    return jsonify({"ok": ok, "message": msg}), 200 if ok else 400
