"""
Meridiano CRM — Grupo económico model
"""

from app.database import get_db
from app.config import TABLE_GROUPS, TABLE_FIRMANTES
from app.utils import now_str


def get_group(group_id: int):
    conn = get_db()
    r = conn.execute(f"SELECT * FROM {TABLE_GROUPS} WHERE id=?", (int(group_id),)).fetchone()
    conn.close()
    return r


def get_group_by_name(nombre: str):
    if not nombre or not str(nombre).strip():
        return None
    conn = get_db()
    r = conn.execute(f"SELECT * FROM {TABLE_GROUPS} WHERE nombre=?", (str(nombre).strip(),)).fetchone()
    conn.close()
    return r


def list_groups():
    conn = get_db()
    rows = conn.execute(
        f"SELECT * FROM {TABLE_GROUPS} WHERE is_active=1 ORDER BY nombre COLLATE NOCASE"
    ).fetchall()
    conn.close()
    return rows


def upsert_group(nombre: str, limite_grupal, actor: str, *, allow_update=True) -> tuple[bool, str, int | None]:
    nombre = (nombre or "").strip()
    if not nombre:
        return False, "El nombre del grupo es obligatorio.", None

    lim = None if limite_grupal is None else float(limite_grupal or 0.0)
    conn = get_db()
    existing = conn.execute(f"SELECT * FROM {TABLE_GROUPS} WHERE nombre=?", (nombre,)).fetchone()

    if existing is None:
        conn.execute(f"""
            INSERT INTO {TABLE_GROUPS} (nombre, credit_limit_grupal, is_active, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
        """, (nombre, float(lim or 0), now_str(), now_str()))
        conn.commit()
        gid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        conn.close()
        return True, "Grupo creado.", int(gid)

    if not allow_update:
        gid = int(existing["id"])
        conn.close()
        return False, "El grupo ya existe.", gid

    old_lim = float(existing["credit_limit_grupal"] or 0)
    new_lim = old_lim if lim is None else float(lim)
    conn.execute(f"UPDATE {TABLE_GROUPS} SET credit_limit_grupal=?, updated_at=? WHERE id=?",
                 (new_lim, now_str(), int(existing["id"])))
    conn.commit()
    conn.close()
    return True, "Grupo actualizado.", int(existing["id"])


def compute_group_available_map(*, agg=None, facturas_agg=None, blocks_agg=None) -> dict:
    """
    Returns {group_id: {limit, used, blocked, avail}} for all active groups.
    `agg`, `facturas_agg`, `blocks_agg` are dicts keyed by cuit_digits.
    """
    agg = agg or {}
    facturas_agg = facturas_agg or {}
    blocks_agg = blocks_agg or {}

    conn = get_db()
    groups_rows = conn.execute(
        f"SELECT id, credit_limit_grupal FROM {TABLE_GROUPS} WHERE is_active=1"
    ).fetchall()
    firm_rows = conn.execute(
        f"SELECT cuit_digits, grupo_id FROM {TABLE_FIRMANTES} WHERE grupo_id IS NOT NULL"
    ).fetchall()
    conn.close()

    g_to_cuits: dict[int, set[str]] = {}
    for fr in firm_rows:
        try:
            gid = int(fr["grupo_id"])
        except (TypeError, ValueError):
            continue
        cd = (fr["cuit_digits"] or "").strip()
        if cd:
            g_to_cuits.setdefault(gid, set()).add(cd)

    out = {}
    for g in groups_rows:
        gid = int(g["id"])
        cuits = g_to_cuits.get(gid, set())
        used = blocked = 0.0
        for cd in cuits:
            a = agg.get(cd) or {"3ros": 0, "propio": 0}
            used += float(a.get("3ros", 0)) + float(a.get("propio", 0)) + float(facturas_agg.get(cd, 0))
            b = blocks_agg.get(cd) or {"3ros": 0, "propio": 0, "fce": 0}
            blocked += float(b.get("3ros", 0)) + float(b.get("propio", 0)) + float(b.get("fce", 0))
        lim = float(g["credit_limit_grupal"] or 0)
        out[gid] = {"limit": lim, "used": used, "blocked": blocked, "avail": lim - used - blocked}
    return out
