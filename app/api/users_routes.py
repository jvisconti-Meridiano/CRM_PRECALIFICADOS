"""
Meridiano CRM — Users API
"""

from flask import Blueprint, request, jsonify
from app.auth import roles_required, current_user, login_required
from app.config import ROLE_ADMIN, ROLE_RISK, ROLE_SALES, ROLE_LABELS
from app.models.user import list_users, create_user, set_user_active, list_active_admins

bp = Blueprint("users", __name__, url_prefix="/api/users")


@bp.route("", methods=["GET"])
@roles_required(ROLE_ADMIN)
def list_all():
    rows = list_users()
    for r in rows:
        r["role_label"] = ROLE_LABELS.get(r["role"], r["role"])
    return jsonify({"users": rows})


@bp.route("", methods=["POST"])
@roles_required(ROLE_ADMIN)
def create():
    data = request.get_json(silent=True) or {}
    u = (data.get("username") or "").strip()
    p = (data.get("password") or "").strip()
    role = (data.get("role") or "").strip().lower()
    if not u or not p:
        return jsonify({"ok": False, "error": "Usuario y clave obligatorios."}), 400
    if role not in (ROLE_ADMIN, ROLE_RISK, ROLE_SALES):
        return jsonify({"ok": False, "error": "Rol inválido."}), 400
    ok, msg = create_user(u, p, role)
    return jsonify({"ok": ok, "message": msg}), 201 if ok else 409


@bp.route("/<string:username>/deactivate", methods=["POST"])
@roles_required(ROLE_ADMIN)
def deactivate(username: str):
    if username == current_user():
        return jsonify({"ok": False, "error": "No podés desactivarte a vos mismo."}), 400
    set_user_active(username, False)
    return jsonify({"ok": True, "message": "Usuario desactivado."})


@bp.route("/<string:username>/reactivate", methods=["POST"])
@roles_required(ROLE_ADMIN)
def reactivate(username: str):
    set_user_active(username, True)
    return jsonify({"ok": True, "message": "Usuario reactivado."})


@bp.route("/admins", methods=["GET"])
@login_required
def list_admins():
    rows = list_active_admins()
    for r in rows:
        r["role_label"] = ROLE_LABELS.get(r["role"], r["role"])
    return jsonify({"users": rows})
