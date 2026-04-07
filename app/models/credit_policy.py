"""
Meridiano CRM — Credit policy model
"""

from app.database import get_db
from app.config import TABLE_CREDIT_POLICY_CONFIG, TABLE_EXTRAORDINARY_LIMITS
from app.utils import cuit_digits, now_str


def get_policy_config():
    conn = get_db()
    row = conn.execute(f"SELECT * FROM {TABLE_CREDIT_POLICY_CONFIG} WHERE id=1").fetchone()
    conn.close()
    return row


def update_policy_config(nyps, pymes, t1_t2_ar, t2_directo, garantizado, actor: str) -> None:
    conn = get_db()
    conn.execute(
        f"UPDATE {TABLE_CREDIT_POLICY_CONFIG}"
        f" SET nyps=?, pymes=?, t1_t2_ar=?, t2_directo=?, garantizado=?, updated_at=?, updated_by=?"
        f" WHERE id=1",
        (float(nyps or 0), float(pymes or 0), float(t1_t2_ar or 0),
         float(t2_directo or 0), float(garantizado or 0), now_str(), actor),
    )
    conn.commit()
    conn.close()


def list_extraordinary_limits(*, active_only=True) -> list:
    conn = get_db()
    sql = f"SELECT * FROM {TABLE_EXTRAORDINARY_LIMITS}"
    if active_only:
        sql += " WHERE is_active=1"
    sql += " ORDER BY razon_social COLLATE NOCASE, id DESC"
    rows = conn.execute(sql).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_extraordinary_limit(cuit_raw: str, razon: str, segmento: str, limite: float, actor: str) -> tuple[bool, str]:
    cd = cuit_digits(cuit_raw)
    if not cd or not razon or not segmento:
        return False, "CUIT, Razón y Segmento son obligatorios."
    conn = get_db()
    conn.execute(
        f"INSERT INTO {TABLE_EXTRAORDINARY_LIMITS}"
        f" (cuit, cuit_digits, razon_social, segmento, limite, is_active, created_at, updated_at, updated_by)"
        f" VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)",
        (cuit_raw, cd, razon, segmento, float(limite), now_str(), now_str(), actor),
    )
    conn.commit()
    conn.close()
    return True, "Límite extraordinario creado."


def update_extraordinary_limit(row_id: int, cuit_raw: str, razon: str, segmento: str, limite: float, actor: str) -> tuple[bool, str]:
    cd = cuit_digits(cuit_raw)
    if not cd or not razon or not segmento:
        return False, "CUIT, Razón y Segmento son obligatorios."
    conn = get_db()
    conn.execute(
        f"UPDATE {TABLE_EXTRAORDINARY_LIMITS}"
        f" SET cuit=?, cuit_digits=?, razon_social=?, segmento=?, limite=?, updated_at=?, updated_by=?"
        f" WHERE id=?",
        (cuit_raw, cd, razon, segmento, float(limite), now_str(), actor, int(row_id)),
    )
    conn.commit()
    conn.close()
    return True, "Límite extraordinario actualizado."


def deactivate_extraordinary_limit(row_id: int, actor: str) -> None:
    conn = get_db()
    conn.execute(
        f"UPDATE {TABLE_EXTRAORDINARY_LIMITS} SET is_active=0, updated_at=?, updated_by=? WHERE id=?",
        (now_str(), actor, int(row_id)),
    )
    conn.commit()
    conn.close()
