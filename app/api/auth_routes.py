"""
Meridiano CRM — Auth API
"""

from flask import Blueprint, request, session, jsonify
from app.auth import current_user, current_role, pop_flash, set_flash, login_required
from app.config import ROLE_LABELS
from app.models.user import authenticate

bp = Blueprint("auth", __name__, url_prefix="/api/auth")


@bp.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    u = (data.get("username") or "").strip()
    p = (data.get("password") or "").strip()
    if not u or not p:
        return jsonify({"ok": False, "error": "Usuario y clave son obligatorios."}), 400

    row = authenticate(u, p)
    if not row:
        return jsonify({"ok": False, "error": "Credenciales inválidas o usuario inactivo."}), 401

    session["username"] = row["username"]
    session["role"] = (row["role"] or "").lower()
    return jsonify({
        "ok": True,
        "user": {"username": row["username"], "role": session["role"],
                 "role_label": ROLE_LABELS.get(session["role"], session["role"])},
    })


@bp.route("/logout", methods=["POST"])
@login_required
def logout():
    session.clear()
    return jsonify({"ok": True})


@bp.route("/me", methods=["GET"])
def me():
    u = current_user()
    if not u:
        return jsonify({"authenticated": False}), 200
    role = current_role()
    return jsonify({
        "authenticated": True,
        "username": u,
        "role": role,
        "role_label": ROLE_LABELS.get(role, role),
    })
