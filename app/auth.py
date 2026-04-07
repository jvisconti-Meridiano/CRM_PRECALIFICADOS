"""
Meridiano CRM — Auth & RBAC
Session helpers and route decorators.
"""

from functools import wraps
from flask import session, redirect, url_for, request, abort, jsonify


# ── Session helpers ────────────────────────────────────────

def current_user() -> str | None:
    return session.get("username")


def current_role() -> str:
    return (session.get("role") or "").lower()


def is_admin() -> bool:
    return current_role() == "admin"


def is_risk() -> bool:
    return current_role() == "risk"


def is_sales() -> bool:
    return current_role() == "sales"


# ── Flash (session-based, one message) ─────────────────────

def set_flash(msg: str, kind: str = "ok") -> None:
    session["_flash"] = {"msg": msg, "kind": kind}


def pop_flash() -> dict | None:
    return session.pop("_flash", None)


# ── Decorators ─────────────────────────────────────────────

def _is_api_request() -> bool:
    """Detect if the caller expects JSON (API) vs HTML redirect."""
    accept = request.headers.get("Accept", "")
    return "application/json" in accept or request.is_json


def login_required(fn):
    """Reject unauthenticated requests.  API callers get 401 JSON,
    browser callers get redirected to login."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user():
            if _is_api_request():
                return jsonify({"error": "No autenticado."}), 401
            return redirect(url_for("auth.login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper


def roles_required(*roles):
    """Restrict to specific roles.  Composes with login_required."""
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user():
                if _is_api_request():
                    return jsonify({"error": "No autenticado."}), 401
                return redirect(url_for("auth.login", next=request.path))
            if current_role() not in roles:
                if _is_api_request():
                    return jsonify({"error": "No autorizado."}), 403
                abort(403)
            return fn(*args, **kwargs)
        return wrapper
    return deco
