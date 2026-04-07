"""
Meridiano CRM — BCRA API client
Fetches cheques rechazados data with retry logic, plus a shared rate limiter
adapted to the public BCRA endpoint behaviour.
"""

import hashlib
import json
import random
import socket
import ssl
import threading
import time
import urllib.error
import urllib.request
from collections import deque

from app.config import (
    BCRA_CHEQ_ENDPOINT,
    BCRA_RATE_LIMIT_REQUESTS,
    BCRA_RATE_LIMIT_WINDOW_SECONDS,
    BCRA_RATE_LIMIT_COOLDOWN_SECONDS,
)

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

_rate_lock = threading.Lock()
_rate_timestamps = deque()
_cooldown_until = 0.0
_MIN_SPACING_SECONDS = max(0.2, BCRA_RATE_LIMIT_WINDOW_SECONDS / max(BCRA_RATE_LIMIT_REQUESTS, 1))
_last_request_at = 0.0


def _respect_rate_limit():
    global _cooldown_until, _last_request_at
    while True:
        wait = 0.0
        now = time.monotonic()
        with _rate_lock:
            # purge expired timestamps
            while _rate_timestamps and (now - _rate_timestamps[0]) >= BCRA_RATE_LIMIT_WINDOW_SECONDS:
                _rate_timestamps.popleft()

            if now < _cooldown_until:
                wait = max(wait, _cooldown_until - now)

            delta = now - _last_request_at
            if delta < _MIN_SPACING_SECONDS:
                wait = max(wait, _MIN_SPACING_SECONDS - delta)

            if not wait and len(_rate_timestamps) >= BCRA_RATE_LIMIT_REQUESTS:
                oldest = _rate_timestamps[0]
                wait = max(wait, BCRA_RATE_LIMIT_WINDOW_SECONDS - (now - oldest) + 0.05)

            if not wait:
                stamp = time.monotonic()
                _rate_timestamps.append(stamp)
                _last_request_at = stamp
                return
        time.sleep(min(wait or 0.05, 1.0))


def _register_429(retry_after: str | None = None):
    global _cooldown_until
    extra = BCRA_RATE_LIMIT_COOLDOWN_SECONDS
    try:
        if retry_after:
            extra = max(extra, int(float(retry_after)))
    except Exception:
        pass
    with _rate_lock:
        _cooldown_until = max(_cooldown_until, time.monotonic() + extra)


def _do_request(url: str, *, timeout: int = 20, user_agent: str = "MeridianoCRM/2.2", context=None) -> dict:
    _respect_rate_limit()
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": user_agent},
    )
    with urllib.request.urlopen(req, timeout=timeout, context=context or _ssl_ctx) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
        return json.loads(raw)


def _sleep_backoff(attempt: int, extra: float = 0.0):
    time.sleep(min(12.0, 0.9 * (attempt + 1) + random.uniform(0.0, 0.4) + extra))


def _fetch_with_retry(url: str, max_retries: int = 3) -> tuple[dict | None, str]:
    """Returns (json_data, error_string). error is '' on success."""
    last_err = ""
    for attempt in range(max_retries + 1):
        try:
            return _do_request(url), ""
        except urllib.error.HTTPError as e:
            code = getattr(e, "code", 0)
            last_err = f"HTTP {code}"
            if code == 429:
                _register_429(e.headers.get("Retry-After") if getattr(e, "headers", None) else None)
            if code in (429, 500, 502, 503, 504) and attempt < max_retries:
                _sleep_backoff(attempt, extra=1.5 if code == 429 else 0.0)
                continue
            break
        except json.JSONDecodeError:
            last_err = "Respuesta BCRA inválida"
            if attempt < max_retries:
                _sleep_backoff(attempt)
                continue
            break
        except (TimeoutError, socket.timeout) as e:
            last_err = f"Timeout: {e}"
            if attempt < max_retries:
                _sleep_backoff(attempt)
                continue
            break
        except Exception as e:
            last_err = str(e)
            if attempt < max_retries:
                _sleep_backoff(attempt)
                continue
            break

    # fallback final, más corto y con UA alternativo
    try:
        return _do_request(url, timeout=10, user_agent="Mozilla/5.0 MeridianoCRM", context=_ssl_ctx), ""
    except urllib.error.HTTPError as e:
        if getattr(e, "code", 0) == 429:
            _register_429(e.headers.get("Retry-After") if getattr(e, "headers", None) else None)
        if not last_err:
            last_err = f"HTTP {getattr(e, 'code', 0)}"
    except Exception as e:
        if not last_err:
            last_err = str(e)
    return None, last_err or "Error desconocido BCRA"


def fetch_cheques_all(cuit_digits_str: str) -> dict:
    """Fetch ALL causales from BCRA.
    Returns: {'denominacion': str, 'events': [...], 'error': str}
    Each event: {'causal': str, 'entidad': int|None, 'detalle_raw': dict}
    """
    out = {"denominacion": "", "events": [], "error": ""}
    url = f"{BCRA_CHEQ_ENDPOINT}/{cuit_digits_str}"

    j, err = _fetch_with_retry(url)
    if err:
        out["error"] = err
        return out

    results = (j or {}).get("results") or {}
    if isinstance(results, dict):
        out["denominacion"] = str(results.get("denominacion") or "").strip()

    for c in (results.get("causales") or []):
        if not isinstance(c, dict):
            continue
        causal = str(c.get("causal") or "").strip()
        for ent in (c.get("entidades") or []):
            if not isinstance(ent, dict):
                continue
            entidad = ent.get("entidad")
            for d in (ent.get("detalle") or []):
                if isinstance(d, dict):
                    out["events"].append({"causal": causal, "entidad": entidad, "detalle_raw": d})

    out["events"].sort(
        key=lambda ev: (str((ev.get("detalle_raw") or {}).get("fechaRechazo") or "")[:10],
                        str((ev.get("detalle_raw") or {}).get("nroCheque") or "")),
        reverse=True,
    )
    return out


def extract_sin_fondos(bcra_all: dict) -> list[dict]:
    """Filter + normalise SIN FONDOS events from a fetch_cheques_all result."""
    out = []
    for ev in (bcra_all or {}).get("events") or []:
        causal = str((ev or {}).get("causal") or "").strip().upper()
        if not causal.startswith("SIN FONDOS"):
            continue
        d = (ev or {}).get("detalle_raw") or {}
        try:
            monto = float(d.get("monto") or 0)
        except (TypeError, ValueError):
            monto = 0.0
        out.append({
            "entidad": (ev or {}).get("entidad"),
            "nro_cheque": str(d.get("nroCheque") or ""),
            "fecha_rechazo": str(d.get("fechaRechazo") or "").strip()[:10],
            "monto": monto,
            "pagado": bool(d.get("fechaPago")),
            "causal": causal,
            "payload_json": d,
        })
    out.sort(key=lambda e: (e.get("fecha_rechazo") or "", e.get("nro_cheque") or ""), reverse=True)
    return out


def cheq_event_id(cd: str, e: dict) -> str:
    payload = (
        f"{(cd or '').strip()}|{str(e.get('entidad') or '')}|{str(e.get('nro_cheque') or '')}|"
        f"{str(e.get('fecha_rechazo') or '')[:10]}|{float(e.get('monto') or 0):.2f}"
    )
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


def cheq_raw_event_id(cd: str, causal: str, entidad, detalle_raw: dict) -> str:
    d = detalle_raw or {}
    try:
        monto = float(d.get("monto") or 0)
    except (TypeError, ValueError):
        monto = 0.0
    flags = "|".join([
        "1" if d.get("ctaPersonal") else "0",
        "1" if d.get("enRevision") else "0",
        "1" if d.get("procesoJud") else "0",
    ])
    payload = (
        f"{(cd or '').strip()}|{(causal or '').strip().upper()}|"
        f"{'' if entidad is None else str(entidad).strip()}|"
        f"{str(d.get('nroCheque') or '')}|{str(d.get('fechaRechazo') or '')[:10]}|{monto:.2f}|"
        f"{str(d.get('fechaPago') or '')[:10]}|{str(d.get('fechaPagoMulta') or '')[:10]}|"
        f"{str(d.get('estadoMulta') or '')}|{flags}"
    )
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()
