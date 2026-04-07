"""
Meridiano CRM — User model
"""

import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash
from app.database import get_db
from app.utils import now_str


def authenticate(username: str, password: str):
    """Return user row if credentials valid, else None."""
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    conn.close()
    if not row or not row["is_active"]:
        return None
    if not check_password_hash(row["pass_hash"], password):
        return None
    return row


def list_users() -> list:
    conn = get_db()
    rows = conn.execute("SELECT username, role, is_active FROM users ORDER BY username").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_user(username: str, password: str, role: str) -> tuple[bool, str]:
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users (username, pass_hash, role, is_active, created_at) VALUES (?, ?, ?, 1, ?)",
            (username, generate_password_hash(password), role, now_str()),
        )
        conn.commit()
        conn.close()
        return True, "Usuario creado."
    except sqlite3.IntegrityError:
        conn.close()
        return False, "Ese usuario ya existe."


def set_user_active(username: str, active: bool) -> None:
    conn = get_db()
    conn.execute("UPDATE users SET is_active=? WHERE username=?", (1 if active else 0, username))
    conn.commit()
    conn.close()


def get_active_admin(username: str):
    conn = get_db()
    row = conn.execute(
        "SELECT username, role, is_active FROM users WHERE username=? AND role='admin' AND is_active=1",
        ((username or "").strip(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def list_active_admins() -> list:
    conn = get_db()
    rows = conn.execute(
        "SELECT username, role, is_active FROM users WHERE role='admin' AND is_active=1 ORDER BY username"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
