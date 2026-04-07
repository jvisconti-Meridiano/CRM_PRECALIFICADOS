"""
Meridiano CRM — Daily blocks model
"""

from app.database import get_db
from app.utils import today_str, now_str

_last_cleanup: str | None = None


def cleanup_daily_blocks() -> None:
    """Delete blocks from previous days.  Idempotent within the same day."""
    global _last_cleanup
    today = today_str()
    if _last_cleanup == today:
        return
    conn = get_db()
    conn.execute("DELETE FROM limit_blocks WHERE block_date <> ?", (today,))
    conn.commit()
    conn.close()
    _last_cleanup = today


def blocks_agg_today() -> tuple[dict, dict]:
    """Returns (agg, by_firmante).
    agg = {cuit_digits: {'3ros': sum, 'propio': sum, 'fce': sum}}
    by_firmante = {cuit_digits: [rows]}
    """
    today = today_str()
    conn = get_db()
    rows = conn.execute(
        "SELECT id, firmante_cuit_digits, scope, username, amount, created_at"
        " FROM limit_blocks WHERE block_date=? ORDER BY id DESC",
        (today,),
    ).fetchall()
    conn.close()

    agg: dict = {}
    by_firmante: dict = {}
    for r in rows:
        cd = r["firmante_cuit_digits"]
        by_firmante.setdefault(cd, []).append(r)
        agg.setdefault(cd, {"3ros": 0.0, "propio": 0.0, "fce": 0.0})
        agg[cd][r["scope"]] += float(r["amount"] or 0)
    return agg, by_firmante


def add_block(cd: str, scope: str, username: str, amount: float) -> None:
    today = today_str()
    conn = get_db()
    conn.execute(
        "INSERT INTO limit_blocks (firmante_cuit_digits, scope, username, amount, block_date, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (cd, scope, username, float(amount), today, now_str()),
    )
    conn.commit()
    conn.close()


def delete_block(block_id: int, username: str, role: str) -> tuple[bool, str]:
    conn = get_db()
    row = conn.execute("SELECT id, username FROM limit_blocks WHERE id=?", (block_id,)).fetchone()
    if not row:
        conn.close()
        return False, "Bloqueo no encontrado."
    if role == "sales" and row["username"] != username:
        conn.close()
        return False, "No autorizado."
    conn.execute("DELETE FROM limit_blocks WHERE id=?", (block_id,))
    conn.commit()
    conn.close()
    return True, "Bloqueo eliminado."
