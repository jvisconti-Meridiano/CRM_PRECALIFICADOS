"""
Meridiano CRM — Audit model
Immutable log for ALTA, BAJA, MODIFICACION, VENCIMIENTO, RENOVACION, ELIMINACION.
"""

from app.database import get_db
from app.config import TABLE_FIRMANTES
from app.utils import now_str


def audit_log(cd: str, action: str, username: str, details: str, conn=None) -> None:
    """Append an audit entry.  If *conn* is given, caller owns the transaction."""
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    try:
        conn.execute(
            "INSERT INTO audit_log (firmante_cuit_digits, action, username, details, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (cd, action, username, details, now_str()),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def append_observation(details: str, observacion: str | None) -> str:
    obs = (observacion or "").strip()
    base = (details or "").strip()
    if not obs:
        return base or "Sin cambios"
    if not base:
        return f"Observación: {obs}"
    return f"{base} | Observación: {obs}"


def list_audit(q: str = "", limit: int = 1500) -> list[dict]:
    """Query audit log with optional free-text search."""
    from app.utils import cuit_digits
    conn = get_db()
    qd = cuit_digits(q) if q else ""

    sql = f"""
        SELECT a.id, a.firmante_cuit_digits, a.action, a.username, a.details, a.created_at,
               f.razon_social, f.cuit
        FROM audit_log a
        LEFT JOIN {TABLE_FIRMANTES} f ON f.cuit_digits = a.firmante_cuit_digits
    """
    params = []
    if q:
        sql += " WHERE a.firmante_cuit_digits LIKE ? OR f.razon_social LIKE ? COLLATE NOCASE OR f.cuit LIKE ?"
        params = [f"%{qd or q}%", f"%{q}%", f"%{q}%"]
    sql += " ORDER BY a.id DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_audit_entry(audit_id: int) -> None:
    conn = get_db()
    conn.execute("DELETE FROM audit_log WHERE id=?", (audit_id,))
    conn.commit()
    conn.close()


def delete_all_audit() -> None:
    conn = get_db()
    conn.execute("DELETE FROM audit_log")
    conn.commit()
    conn.close()
