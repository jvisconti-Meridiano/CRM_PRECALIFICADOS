"""
Meridiano CRM — Firmante model
CRUD + upsert + CSV import logic.  All mutations are audited.
"""

from app.database import get_db
from app.config import TABLE_FIRMANTES, TABLE_GROUPS
from app.utils import (
    cuit_digits, now_str, today_str, parse_date_any,
    line_is_expired, effective_line_active, line_status_label, fmt_ars,
)
from app.models.audit import audit_log, append_observation

_KEEP = object()  # sentinel: "don't touch this field"


# ── Reads ──────────────────────────────────────────────────

def get_firmante(cd: str):
    conn = get_db()
    r = conn.execute(f"SELECT * FROM {TABLE_FIRMANTES} WHERE cuit_digits=?", (cd,)).fetchone()
    conn.close()
    return r


def get_firmante_rs(cd: str) -> str | None:
    """Return razón social for a CUIT digits string (or None)."""
    r = get_firmante((cd or "").strip())
    if not r:
        return None
    return (r["razon_social"] or "").strip() or None


def list_firmantes(*, q: str = "", active_only: bool = True, group_id: int | None = None,
                   first_line: str = "all") -> list:
    conn = get_db()
    where, params = [], []
    if active_only:
        where.append("is_active=1")
    if q:
        like = f"%{q}%"
        qd = cuit_digits(q)
        where.append("(razon_social LIKE ? COLLATE NOCASE OR cuit LIKE ? COLLATE NOCASE OR cuit_digits LIKE ?)")
        params.extend([like, like, f"%{qd}%" if qd else like])
    if group_id is not None:
        where.append("grupo_id=?")
        params.append(group_id)
    if first_line == "yes":
        where.append("COALESCE(primera_linea, 0)=1")
    elif first_line == "no":
        where.append("COALESCE(primera_linea, 0)=0")

    sql = f"SELECT * FROM {TABLE_FIRMANTES}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY razon_social COLLATE NOCASE"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def all_cuit_digits_active() -> list[str]:
    """All active firmante CUIT digits (for monitor universe)."""
    conn = get_db()
    rows = conn.execute(
        f"SELECT cuit_digits FROM {TABLE_FIRMANTES} WHERE is_active=1 AND cuit_digits <> ''"
    ).fetchall()
    conn.close()
    return [r["cuit_digits"] for r in rows]


def firmantes_rs_map(cuits: list[str]) -> dict[str, str]:
    """Bulk lookup: {cuit_digits → razon_social}."""
    if not cuits:
        return {}
    conn = get_db()
    qmarks = ",".join(["?"] * len(cuits))
    rows = conn.execute(
        f"SELECT cuit_digits, razon_social FROM {TABLE_FIRMANTES} WHERE cuit_digits IN ({qmarks})",
        tuple(cuits),
    ).fetchall()
    conn.close()
    return {(r["cuit_digits"] or "").strip(): (r["razon_social"] or "").strip() for r in rows}


# ── Writes ─────────────────────────────────────────────────

def upsert_firmante(
    razon_social, cuit, lim3, limp, limf, exp3, expp, actor,
    *, allow_update=True, reactivate=False,
    group_id=_KEEP, primera_linea=_KEEP, observacion: str = "",
) -> tuple[bool, str]:
    cd = cuit_digits(cuit)
    if not cd or not razon_social:
        return False, "Razón social y CUIT son obligatorios."

    lim3_val = None if lim3 is None else float(lim3 or 0.0)
    limp_val = None if limp is None else float(limp or 0.0)
    limf_val = None if limf is None else float(limf or 0.0)
    exp3_val = None if exp3 is None else parse_date_any(exp3)
    expp_val = None if expp is None else parse_date_any(expp)

    conn = get_db()
    try:
        existing = conn.execute(f"SELECT * FROM {TABLE_FIRMANTES} WHERE cuit_digits=?", (cd,)).fetchone()

        # ── INSERT (new firmante) ──
        if existing is None:
            e3 = (exp3_val or "") if exp3_val is not None else ""
            ep = (expp_val or "") if expp_val is not None else ""
            gid = None if group_id is _KEEP else (group_id if group_id is not None else None)
            pl = 1 if (False if primera_linea is _KEEP else bool(primera_linea)) else 0

            conn.execute(f"""
                INSERT INTO {TABLE_FIRMANTES}
                (razon_social, cuit, cuit_digits,
                 credit_limit_3ros, credit_limit_propio, credit_limit_fce,
                 limit_expiry_3ros, limit_expiry_propio,
                 line_active_3ros, line_active_propio,
                 grupo_id, primera_linea,
                 is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """, (
                razon_social.strip(), str(cuit).strip(), cd,
                float(lim3_val or 0), float(limp_val or 0), float(limf_val or 0),
                e3, ep,
                0 if line_is_expired(e3) else 1,
                0 if line_is_expired(ep) else 1,
                gid, pl, now_str(), now_str(),
            ))
            audit_log(
                cd, "ALTA", actor,
                append_observation(
                    f"Límites: 3ros={fmt_ars(float(lim3_val or 0))} (vto {exp3_val or '—'}); "
                    f"propio={fmt_ars(float(limp_val or 0))} (vto {expp_val or '—'}); "
                    f"FCE={fmt_ars(float(limf_val or 0))}",
                    observacion,
                ),
                conn=conn,
            )
            conn.commit()
            return True, "Firmante creado."

        # ── UPDATE ──
        if not allow_update:
            return False, "El firmante ya existe."

        old3 = float(existing["credit_limit_3ros"] or 0)
        oldp = float(existing["credit_limit_propio"] or 0)
        oldf = float(existing["credit_limit_fce"] or 0)
        olde3 = (existing["limit_expiry_3ros"] or "").strip()
        oldep = (existing["limit_expiry_propio"] or "").strip()
        old_active = int(existing["is_active"] or 0)
        old_l3 = int(existing["line_active_3ros"] or 0)
        old_lp = int(existing["line_active_propio"] or 0)
        old_gid = existing["grupo_id"]
        old_primera = int(existing["primera_linea"] or 0)

        new_active = 1 if (reactivate and old_active == 0) else old_active
        new3 = old3 if lim3_val is None else float(lim3_val)
        newp = oldp if limp_val is None else float(limp_val)
        newf = oldf if limf_val is None else float(limf_val)
        newe3 = olde3 if exp3_val is None else (exp3_val or "")
        newep = oldep if expp_val is None else (expp_val or "")

        # Recalculate line active status
        new_l3 = old_l3
        if exp3_val is not None:
            new_l3 = 0 if line_is_expired(newe3) else 1
        elif lim3_val is not None and line_is_expired(newe3):
            new_l3 = 0

        new_lp = old_lp
        if expp_val is not None:
            new_lp = 0 if line_is_expired(newep) else 1
        elif limp_val is not None and line_is_expired(newep):
            new_lp = 0

        new_gid = old_gid if group_id is _KEEP else group_id
        new_primera = old_primera if primera_linea is _KEEP else (1 if bool(primera_linea) else 0)

        conn.execute(f"""
            UPDATE {TABLE_FIRMANTES}
            SET razon_social=?, cuit=?,
                credit_limit_3ros=?, credit_limit_propio=?, credit_limit_fce=?,
                limit_expiry_3ros=?, limit_expiry_propio=?,
                line_active_3ros=?, line_active_propio=?,
                grupo_id=?, primera_linea=?, is_active=?, updated_at=?
            WHERE cuit_digits=?
        """, (
            razon_social.strip(), str(cuit).strip(),
            new3, newp, newf, newe3, newep, new_l3, new_lp,
            new_gid, new_primera, new_active, now_str(), cd,
        ))

        # Build change description
        changes = []
        if old_active == 0 and new_active == 1:
            changes.append("Reactivado")
        if old3 != new3:
            changes.append(f"3ros: {fmt_ars(old3)} → {fmt_ars(new3)}")
        if oldp != newp:
            changes.append(f"propio: {fmt_ars(oldp)} → {fmt_ars(newp)}")
        if oldf != newf:
            changes.append(f"FCE: {fmt_ars(oldf)} → {fmt_ars(newf)}")
        if olde3 != newe3:
            changes.append(f"vto 3ros: {olde3 or '—'} → {newe3 or '—'}")
        if oldep != newep:
            changes.append(f"vto propio: {oldep or '—'} → {newep or '—'}")
        if old_l3 != new_l3:
            changes.append(f"estado 3ros: {line_status_label(old_l3, olde3)} → {line_status_label(new_l3, newe3)}")
        if old_lp != new_lp:
            changes.append(f"estado propio: {line_status_label(old_lp, oldep)} → {line_status_label(new_lp, newep)}")
        if old_gid != new_gid:
            changes.append(f"grupo: {_group_name(old_gid)} → {_group_name(new_gid)}")
        if old_primera != new_primera:
            changes.append(f"primera línea: {'Sí' if old_primera else 'No'} → {'Sí' if new_primera else 'No'}")

        action = "ALTA" if (old_active == 0 and new_active == 1) else "MODIFICACION"
        audit_log(cd, action, actor, append_observation(" | ".join(changes) or "Sin cambios", observacion), conn=conn)

        # Renewal audit
        if old_l3 == 0 and new_l3 == 1 and line_is_expired(olde3):
            audit_log(cd, "RENOVACION", actor,
                      append_observation(f"3ros renovado: vto {olde3 or '—'} → {newe3 or '—'} | límite {fmt_ars(new3)}", observacion), conn=conn)
        if old_lp == 0 and new_lp == 1 and line_is_expired(oldep):
            audit_log(cd, "RENOVACION", actor,
                      append_observation(f"propio renovado: vto {oldep or '—'} → {newep or '—'} | límite {fmt_ars(newp)}", observacion), conn=conn)

        conn.commit()
        return True, "Firmante actualizado."
    finally:
        conn.close()


def set_firmante_active(cd: str, active: bool, actor: str, observacion: str = "") -> tuple[bool, str]:
    conn = get_db()
    r = conn.execute(f"SELECT * FROM {TABLE_FIRMANTES} WHERE cuit_digits=?", (cd,)).fetchone()
    if not r:
        conn.close()
        return False, "Firmante no encontrado."
    conn.execute(f"UPDATE {TABLE_FIRMANTES} SET is_active=?, updated_at=? WHERE cuit_digits=?",
                 (1 if active else 0, now_str(), cd))
    conn.commit()
    conn.close()
    audit_log(cd, "BAJA" if not active else "ALTA", actor,
              append_observation("Reactivación" if active else "Desactivación", observacion))
    return True, "OK"


def delete_firmante_hard(cd: str, actor: str, observacion: str = "") -> tuple[bool, str]:
    cd = cuit_digits(cd)
    if not cd:
        return False, "Firmante inválido."
    conn = get_db()
    r = conn.execute(f"SELECT * FROM {TABLE_FIRMANTES} WHERE cuit_digits=?", (cd,)).fetchone()
    if not r:
        conn.close()
        return False, "Firmante no encontrado."
    rs = (r["razon_social"] or "").strip()
    try:
        conn.execute("DELETE FROM limit_blocks WHERE firmante_cuit_digits=?", (cd,))
    except Exception:
        pass
    conn.execute(f"DELETE FROM {TABLE_FIRMANTES} WHERE cuit_digits=?", (cd,))
    audit_log(cd, "ELIMINACION", actor,
              append_observation(f"Eliminación definitiva: {rs} ({cd})", observacion), conn=conn)
    conn.commit()
    conn.close()
    return True, "Firmante eliminado definitivamente."


# ── Expiry daily job ───────────────────────────────────────

def apply_expired_limits() -> None:
    """Mark expired lines as inactive.  Called once per day."""
    today = today_str()
    conn = get_db()
    rows = conn.execute(f"""
        SELECT cuit_digits, razon_social, credit_limit_3ros, credit_limit_propio,
               limit_expiry_3ros, limit_expiry_propio, line_active_3ros, line_active_propio
        FROM {TABLE_FIRMANTES}
        WHERE is_active=1 AND (
            (limit_expiry_3ros <> '' AND limit_expiry_3ros <= ? AND COALESCE(line_active_3ros,1)=1)
         OR (limit_expiry_propio <> '' AND limit_expiry_propio <= ? AND COALESCE(line_active_propio,1)=1)
        )
    """, (today, today)).fetchall()

    for r in rows:
        cd = r["cuit_digits"]
        l3 = int(r["line_active_3ros"] or 0)
        lp = int(r["line_active_propio"] or 0)
        parts = []
        if (r["limit_expiry_3ros"] or "").strip() and r["limit_expiry_3ros"] <= today and l3 == 1:
            l3 = 0
            parts.append(f"3ros vencido ({r['limit_expiry_3ros']}): límite se mantiene en {fmt_ars(r['credit_limit_3ros'])} y pasa a inactiva")
        if (r["limit_expiry_propio"] or "").strip() and r["limit_expiry_propio"] <= today and lp == 1:
            lp = 0
            parts.append(f"propio vencido ({r['limit_expiry_propio']}): límite se mantiene en {fmt_ars(r['credit_limit_propio'])} y pasa a inactiva")
        if parts:
            conn.execute(f"UPDATE {TABLE_FIRMANTES} SET line_active_3ros=?, line_active_propio=?, updated_at=? WHERE cuit_digits=?",
                         (l3, lp, now_str(), cd))
            audit_log(cd, "VENCIMIENTO", "system", " | ".join(parts), conn=conn)
    conn.commit()
    conn.close()


# ── Internal helpers ───────────────────────────────────────

def _group_name(gid) -> str:
    if gid is None:
        return "—"
    try:
        conn = get_db()
        g = conn.execute(f"SELECT nombre FROM {TABLE_GROUPS} WHERE id=?", (int(gid),)).fetchone()
        conn.close()
        return g["nombre"] if g else "—"
    except Exception:
        return "—"
