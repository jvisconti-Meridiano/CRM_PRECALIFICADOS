"""
Meridiano CRM — Cartera API
"""

from flask import Blueprint, request, jsonify
from app.auth import login_required
from app.utils import cuit_digits, normalize_header
from app.services.cartera import load_cartera, load_facturas
from app.models.firmante import get_firmante

bp = Blueprint("cartera", __name__, url_prefix="/api/cartera")


def _serialize_rows(headers: list, rows: list) -> list[dict]:
    """Convert list-of-lists to list-of-dicts for JSON."""
    return [
        {h: (str(v) if v is not None else "") for h, v in zip(headers, r)}
        for r in rows
    ]


@bp.route("/cheques", methods=["GET"])
@login_required
def cheques():
    headers, rows, _, status = load_cartera()
    return jsonify({
        "headers": headers,
        "rows": _serialize_rows(headers, rows),
        "count": len(rows),
        "status": status,
    })


@bp.route("/facturas", methods=["GET"])
@login_required
def facturas():
    headers, rows, _, status = load_facturas()
    return jsonify({
        "headers": headers,
        "rows": _serialize_rows(headers, rows),
        "count": len(rows),
        "status": status,
    })


@bp.route("/firmante/<string:cd>", methods=["GET"])
@login_required
def firmante_cartera(cd: str):
    """Cartera rows filtered for a single firmante."""
    cd = cuit_digits(cd)
    f = get_firmante(cd)
    if not f:
        return jsonify({"error": "Firmante no encontrado."}), 404

    headers, all_rows, _, status = load_cartera()
    fact_headers, fact_all_rows, _, fact_status = load_facturas()

    # Filter cheques by CUIT
    cheq_rows = []
    if headers and all_rows:
        hn = [normalize_header(h) for h in headers]
        idx = next((i for i, h in enumerate(hn) if h == "cuit"), None)
        if idx is not None:
            cheq_rows = [r for r in all_rows if cuit_digits(r[idx]) == cd]

    # Filter facturas by Firmante
    fact_rows = []
    if fact_headers and fact_all_rows:
        hn = [normalize_header(h) for h in fact_headers]
        idx = next((i for i, h in enumerate(hn) if h == "firmante"), None)
        if idx is not None:
            fact_rows = [r for r in fact_all_rows if cuit_digits(r[idx]) == cd]

    return jsonify({
        "firmante": {"razon_social": f["razon_social"], "cuit": f["cuit"], "cuit_digits": cd},
        "cheques": {"headers": headers, "rows": _serialize_rows(headers, cheq_rows), "count": len(cheq_rows), "status": status},
        "facturas": {"headers": fact_headers, "rows": _serialize_rows(fact_headers, fact_rows), "count": len(fact_rows), "status": fact_status},
    })
