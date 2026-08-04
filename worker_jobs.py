import os
import re
import traceback
import requests
import base64
import json
import hashlib
import time

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from redis import Redis
from app.verifiable_flow import (
    load_pending,
    claim_provider_result,
    release_provider_result_claim,
    finish_pending,
)

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
)

from app.db import (
    Base,
    engine,
    SessionLocal,
)

class VerifiableProviderStat(Base):
    __tablename__ = "verifiable_provider_stats"

    id = Column(
        Integer,
        primary_key=True,
        index=True,
    )

    count = Column(
        Integer,
        nullable=False,
        default=1,
        server_default="1",
    )

    provider_db_name = Column(
        String(120),
        nullable=False,
        index=True,
    )

    provider_name = Column(
        String(200),
        nullable=False,
        default="",
    )

    provider_group_jid = Column(
        String(120),
        nullable=False,
        default="",
    )

    status = Column(
        String(30),
        nullable=False,
        default="DONE",
        index=True,
    )

    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        index=True,
    )

    request_key = Column(
        String(180),
        nullable=False,
        unique=True,
        index=True,
    )


try:
    VerifiableProviderStat.__table__.create(
        bind=engine,
        checkfirst=True,
    )
except Exception as provider_stat_table_exc:
    print(
        "VERIFIABLE_PROVIDER_STATS_TABLE_ERROR =",
        repr(provider_stat_table_exc),
        flush=True,
    )

EVOLUTION_BASE_URL = os.getenv("EVOLUTION_BASE_URL", "").rstrip("/")
EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY", "").strip()
EVOLUTION_INSTANCE = os.getenv("EVOLUTION_INSTANCE", "").strip()

BOT_INTERNAL_URL = os.getenv("BOT_INTERNAL_URL", "").strip().rstrip("/")
BOT_INTERNAL_TOKEN = os.getenv("BOT_INTERNAL_TOKEN", "").strip()

if BOT_INTERNAL_URL.endswith("/internal/generate-pdf"):
    BOT_INTERNAL_URL = BOT_INTERNAL_URL[:-len("/internal/generate-pdf")]

if BOT_INTERNAL_URL.endswith("/internal/generate-pdf-from-media"):
    BOT_INTERNAL_URL = BOT_INTERNAL_URL[:-len("/internal/generate-pdf-from-media")]

print("BOT_INTERNAL_URL NORMALIZED =", repr(BOT_INTERNAL_URL), flush=True)

# =========================
# PANEL STATS
# =========================
REDIS_URL = os.getenv("REDIS_URL", "").strip()
PANEL_TZ = os.getenv("PANEL_TZ", "America/Monterrey").strip()
redis_stats = Redis.from_url(REDIS_URL, decode_responses=True)

DELIVERY_LOCK_TTL_SEC = int(
    os.getenv(
        "DELIVERY_LOCK_TTL_SEC",
        "1800",
    ) or "1800"
)

DELIVERY_DONE_TTL_SEC = int(
    os.getenv(
        "DELIVERY_DONE_TTL_SEC",
        "300",
    ) or "300"
)

CURP_RE = re.compile(r"\b[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d\b", re.I)
RFC_RE = re.compile(r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b", re.I)
IDCIF_RE = re.compile(r"\b\d{11}\b", re.I)

def _panel_now():
    return datetime.now(ZoneInfo(PANEL_TZ))

def _panel_day_str():
    return _panel_now().strftime("%Y-%m-%d")

def _format_total_time(
    seconds: float,
) -> str:
    """
    Ejemplos:
      6.44  -> 6.44 segundos
      66.44 -> 1 min 6.44 segundos
      3726  -> 1 h 2 min 6.00 segundos
    """
    try:
        total = max(
            0.0,
            float(seconds or 0),
        )
    except Exception:
        total = 0.0

    hours = int(total // 3600)

    remaining = (
        total
        - (hours * 3600)
    )

    minutes = int(
        remaining // 60
    )

    seconds_part = (
        remaining
        - (minutes * 60)
    )

    if hours > 0:
        return (
            f"{hours} h "
            f"{minutes} min "
            f"{seconds_part:.2f} segundos"
        )

    if minutes > 0:
        return (
            f"{minutes} min "
            f"{seconds_part:.2f} segundos"
        )

    return (
        f"{seconds_part:.2f} segundos"
    )

def _panel_stats_key(group_jid: str) -> str:
    return f"panel_stats:{_panel_day_str()}:group:{group_jid}"

def _classify_success_kind(query: str, original_text: str, msg_type: str) -> str:
    """
    Regresa uno de:
      QR
      RFC_IDCIF
      CURP
      RFC_ONLY
      UNKNOWN
    """
    if (msg_type or "").lower() in ("image", "document"):
        return "QR"

    src = f"{query or ''}\n{original_text or ''}".upper()

    has_curp = bool(CURP_RE.search(src))
    has_rfc = bool(RFC_RE.search(src))
    has_idcif = bool(IDCIF_RE.search(src))

    if has_rfc and has_idcif:
        return "RFC_IDCIF"
    if has_curp:
        return "CURP"
    if has_rfc:
        return "RFC_ONLY"
    return "UNKNOWN"

def _family_from_kind(kind: str) -> str:
    if kind in ("QR", "RFC_IDCIF"):
        return "RFC_IDCIF_QR"
    if kind in ("CURP", "RFC_ONLY"):
        return "RFC_CLON"
    if kind == "RFC_VERIFICABLE":
        return "RFC_VERIFICABLE"
    return "UNKNOWN"

def panel_record_success(group_jid: str, group_name: str, kind: str, count: int = 1):
    """
    Cuenta éxitos solo cuando ya se entregó el PDF/ZIP final.
    Reinicio diario automático por fecha.
    """
    if not group_jid or count <= 0:
        return

    kind = (kind or "UNKNOWN").strip().upper()
    family = _family_from_kind(kind)
    day = _panel_day_str()
    now_iso = _panel_now().isoformat(timespec="seconds")
    key = _panel_stats_key(group_jid)

    pipe = redis_stats.pipeline()
    pipe.hset(key, mapping={
        "group_jid": group_jid,
        "group_name": group_name or group_jid,
        "day": day,
        "updated_at": now_iso,
    })
    pipe.hincrby(key, "total", count)

    if kind == "QR":
        pipe.hincrby(key, "ok_qr", count)
    elif kind == "RFC_IDCIF":
        pipe.hincrby(key, "ok_rfc_idcif", count)
    elif kind == "CURP":
        pipe.hincrby(key, "ok_curp", count)
    elif kind == "RFC_ONLY":
        pipe.hincrby(key, "ok_rfc_only", count)
    elif kind == "RFC_VERIFICABLE":
        pipe.hincrby(
            key,
            "ok_rfc_verificable",
            count,
        )
    else:
        pipe.hincrby(key, "ok_unknown", count)

    if family == "RFC_IDCIF_QR":
        pipe.hincrby(key, "ok_rfc_idcif_qr", count)
    elif family == "RFC_CLON":
        pipe.hincrby(key, "ok_rfc_clon", count)

    # historial permanente para panel mensual / auditorías
    pipe.persist(key)
    pipe.execute()

# =========================
# CUT STATS (HISTORIAL DE CORTES)
# CLON = CURP + RFC_ONLY
# IDCIF = RFC_IDCIF + QR
# =========================

def _cut_stats_key(group_jid: str) -> str:
    return f"cut_stats:{_panel_day_str()}:group:{group_jid}"

def cut_record_success(group_jid: str, group_name: str, kind: str, count: int = 1):
    """
    Guarda historial diario por grupo:
      - count_clon   = CURP + RFC_ONLY
      - count_idcif  = RFC_IDCIF + QR
    TTL de 8 días para poder ver una semana + 1 día de margen.
    """
    if not group_jid or count <= 0:
        return

    kind = (kind or "").strip().upper()
    add_clon = 0
    add_idcif = 0
    add_verificable = 0
    
    if kind in ("CURP", "RFC_ONLY"):
        add_clon = count
    
    elif kind in ("RFC_IDCIF", "QR"):
        add_idcif = count
    
    elif kind == "RFC_VERIFICABLE":
        add_verificable = count
    
    else:
        return

    day = _panel_day_str()
    now_iso = _panel_now().isoformat(timespec="seconds")
    key = _cut_stats_key(group_jid)

    pipe = redis_stats.pipeline()
    pipe.hset(key, mapping={
        "group_jid": group_jid,
        "group_name": group_name or group_jid,
        "date": day,
        "updated_at": now_iso,
    })

    if add_clon:
        pipe.hincrby(key, "count_clon", add_clon)

    if add_idcif:
        pipe.hincrby(key, "count_idcif", add_idcif)

    if add_verificable:
        pipe.hincrby(
            key,
            "count_verificable",
            add_verificable,
        )

    # historial permanente para cortes / cobros / auditorías
    pipe.persist(key)
    pipe.execute()

def _stats_counted_key(
    job_data: dict,
    kind: str,
    item_key: str = "",
) -> str:
    instance = (
        job_data.get("evolution_instance")
        or EVOLUTION_INSTANCE
        or ""
    ).strip()

    group = (
        job_data.get("group_jid")
        or ""
    ).strip()

    requester = (
        job_data.get("requester_number")
        or ""
    ).strip()

    execution_key = (
        job_data.get("execution_key")
        or job_data.get("msg_id")
        or job_data.get("request_key")
        or ""
    ).strip()

    day = _panel_day_str()

    if not item_key:
        item_key = (
            job_data.get("query")
            or job_data.get("original_text")
            or job_data.get("media_id")
            or ""
        )

    normalized_item = re.sub(
        r"\s+",
        " ",
        str(item_key or "").strip().upper(),
    )

    if execution_key:
        base = (
            f"{day}|{instance}|{group}|{requester}|"
            f"{execution_key}|{kind}|{normalized_item}"
        )
    else:
        base = (
            f"{day}|{instance}|{group}|{requester}|"
            f"{kind}|{normalized_item}"
        )

    digest = hashlib.sha256(
        base.encode("utf-8")
    ).hexdigest()

    return (
        f"stats_counted:{day}:{digest}"
    )


def _commercial_accounting_key(
    job_data: dict,
    kind: str,
    item_key: str = "",
) -> str:
    instance = (
        job_data.get("evolution_instance")
        or job_data.get("instance_name")
        or EVOLUTION_INSTANCE
        or ""
    ).strip()

    group = (
        job_data.get("group_jid")
        or ""
    ).strip()

    requester = (
        job_data.get("requester_number")
        or ""
    ).strip()

    execution_key = (
        job_data.get("execution_key")
        or job_data.get("msg_id")
        or job_data.get("request_key")
        or ""
    ).strip()

    normalized_item = re.sub(
        r"\s+",
        " ",
        str(item_key or "").strip().upper(),
    )

    if not execution_key:
        raise RuntimeError(
            "RFC_ACCOUNTING_IDENTITY_EMPTY"
        )

    base = (
        f"{instance}|"
        f"{group}|"
        f"{requester}|"
        f"{execution_key}|"
        f"{kind}|"
        f"{normalized_item}"
    )

    digest = hashlib.sha256(
        base.encode("utf-8")
    ).hexdigest()

    return (
        f"rfc_accounting:{digest}"
    )


def record_success_once(job_data: dict, group_jid: str, group_name: str, kind: str, count: int = 1, item_key: str = "") -> bool:
    """
    Cuenta una solicitud exitosa una sola vez.
    Evita inflar panel_stats/cut_stats por reintentos, jobs repetidos o webhooks duplicados.
    """
    if not group_jid or count <= 0:
        return False

    key = _stats_counted_key(job_data, kind, item_key=item_key)

    accounting_key = (
        _commercial_accounting_key(
            job_data,
            kind,
            item_key=item_key,
        )
    )

    ok = redis_stats.set(
        key,
        "1",
        nx=True,
        ex=60 * 60 * 24 * 35,
    )
    
    if not ok:
        print(
            "[STATS DUPLICATE IGNORED]",
            key,
            flush=True,
        )
        return False
    
    try:
        _save_rfc_panel_request_log(
            {
                **job_data,
                "item_key": item_key,
                "query_type": kind,
            },
            {
                "filename": item_key,
            },
            status="DONE",
        )

        instance_name = (
            job_data.get(
                "evolution_instance"
            )
            or job_data.get(
                "instance_name"
            )
            or EVOLUTION_INSTANCE
            or "grupo02"
        ).strip()

        commercial_recorded = (
            _rfc_commercial_after_success(
                job_data=job_data,
                group_jid=group_jid,
                group_name=group_name,
                instance_name=instance_name,
                kind=kind,
                count=count,
                accounting_key=accounting_key,
                item_key=item_key,
            )
        )
        
        if commercial_recorded is False:
            print(
                "[RFC_SUCCESS_DUPLICATE_FULLY_IGNORED]",
                {
                    "accounting_key": key,
                    "group_jid": group_jid,
                    "kind": kind,
                    "item_key": item_key,
                },
                flush=True,
            )
        
            return False
        
        panel_record_success(
            group_jid=group_jid,
            group_name=group_name,
            kind=kind,
            count=count,
        )
    
        cut_record_success(
            group_jid=group_jid,
            group_name=group_name,
            kind=kind,
            count=count,
        )
    
        print(
            "[COUNT_SUCCESS]",
            {
                "group_jid": group_jid,
                "group_name": group_name,
                "kind": kind,
                "count": count,
                "item_key": item_key,
            },
            flush=True,
        )
    
        return True
    
    except Exception:
        try:
            redis_stats.delete(key)
        except Exception:
            pass
    
        raise

def evolution_headers():
    return {
        "apikey": EVOLUTION_API_KEY,
        "Content-Type": "application/json",
    }

def evolution_send_text_to_group(group_jid: str, text: str, instance_name=None):
    instance_name = (instance_name or EVOLUTION_INSTANCE).strip()

    url = f"{EVOLUTION_BASE_URL}/message/sendText/{instance_name}"
    payload = {
        "number": group_jid,
        "text": text
    }

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=60)
    print("worker sendText instance:", instance_name, flush=True)
    print("worker sendText:", r.status_code, r.text, flush=True)
    r.raise_for_status()
    return r.json()

def notify_verifiable_provider_sat_rejection(
    *,
    job_data: dict,
    error_code: str,
) -> bool:
    if not bool(
        job_data.get("is_verifiable")
    ):
        return False

    provider_group = (
        job_data.get(
            "verifiable_provider_group"
        )
        or ""
    ).strip()

    provider_instance = (
        job_data.get(
            "verifiable_provider_instance"
        )
        or EVOLUTION_INSTANCE
        or "grupo02"
    ).strip()

    provider_name = (
        job_data.get(
            "verifiable_provider_name"
        )
        or "Proveedor verificable"
    ).strip()

    original_identifier = (
        job_data.get(
            "verifiable_original_identifier"
        )
        or ""
    ).strip().upper()

    provider_rfc = (
        job_data.get("provider_rfc")
        or ""
    ).strip().upper()

    provider_idcif = (
        job_data.get("provider_idcif")
        or ""
    ).strip()

    if not provider_group:
        print(
            "VERIFIABLE_PROVIDER_ALERT_SKIPPED =",
            {
                "reason": "provider_group_empty",
                "error_code": error_code,
                "provider_rfc": provider_rfc,
            },
            flush=True,
        )
        return False

    reason_map = {
        "SIN_DATOS_SAT": (
            "el IDCIF fue leído, pero la página "
            "oficial del SAT no devolvió información"
        ),
        "SAT_CIF_NOT_ISSUED": (
            "la página oficial del SAT indica que "
            "a este RFC no se le ha emitido una "
            "Cédula de Identificación Fiscal; el "
            "IDCIF entregado no pudo validarse "
            "para ese RFC"
        ),
        "SAT_NO_ACTIVE_REGIME": (
            "el RFC aparece sin régimen fiscal "
            "vigente en la página oficial del SAT"
        ),
        "SAT_STATUS_SUSPENDED": (
            "el RFC aparece suspendido o no activo "
            "en la página oficial del SAT"
        ),
        "CLIENT_RFC_CANCELLED": (
            "CheckID indica que el RFC aparece "
            "cancelado o dado de baja"
        ),
        "CLIENT_RFC_SUSPENDED": (
            "CheckID indica que el RFC aparece "
            "suspendido"
        ),
        "CLIENT_RFC_INACTIVE": (
            "CheckID indica que el RFC aparece "
            "como no activo o no vigente"
        ),
    }

    reason_text = (
        reason_map.get(error_code)
        or "el resultado no pudo validarse en SAT"
    )

    message = (
        "⚠️ Resultado verificable no procesable\n\n"
        f"Solicitud original: "
        f"{original_identifier or 'N/D'}\n"
        f"RFC entregado: "
        f"{provider_rfc or 'N/D'}\n"
        f"IDCIF: "
        f"{provider_idcif or 'N/D'}\n\n"
        f"Motivo: {reason_text}.\n\n"
        "No se generó constancia y el resultado "
        "no fue contabilizado."
    )

    try:
        evolution_send_text_to_group(
            provider_group,
            message,
            instance_name=provider_instance,
        )

        print(
            "VERIFIABLE_PROVIDER_SAT_ALERT_SENT =",
            {
                "provider_name": provider_name,
                "provider_group": provider_group,
                "provider_instance": (
                    provider_instance
                ),
                "error_code": error_code,
                "provider_rfc": provider_rfc,
                "provider_idcif": provider_idcif,
            },
            flush=True,
        )

        return True

    except Exception as alert_exc:
        print(
            "VERIFIABLE_PROVIDER_SAT_ALERT_ERROR =",
            {
                "provider_group": provider_group,
                "provider_instance": (
                    provider_instance
                ),
                "error_code": error_code,
                "error": repr(alert_exc),
            },
            flush=True,
        )

        return False

def evolution_send_media_to_group(
    group_jid: str,
    media_url: str,
    file_name: str,
    instance_name=None,
    caption: str = "",
):
    instance_name = (
        instance_name
        or EVOLUTION_INSTANCE
    ).strip()

    url = (
        f"{EVOLUTION_BASE_URL}"
        f"/message/sendMedia/"
        f"{instance_name}"
    )

    payload = {
        "number": group_jid,
        "mediatype": "document",
        "media": media_url,
        "fileName": file_name,
        "caption": (
            caption or ""
        ).strip(),
    }

    r = requests.post(
        url,
        json=payload,
        headers=evolution_headers(),
        timeout=240,
    )

    print(
        "worker sendMedia instance:",
        instance_name,
        flush=True,
    )

    print(
        "worker sendMedia payload:",
        payload,
        flush=True,
    )

    print(
        "worker sendMedia resp:",
        r.status_code,
        r.text,
        flush=True,
    )

    r.raise_for_status()
    return r.json()

def call_bot_internal_text(
    requester_number: str,
    requester_name: str,
    group_jid: str,
    original_text: str,
    query: str,
    instance_name=None,
    *,
    is_verifiable: bool = False,
    provider_rfc: str = "",
    provider_idcif: str = "",
):
    headers = {
        "Authorization": f"Bearer {BOT_INTERNAL_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "requester_number": requester_number,
        "requester_name": requester_name,
        "group_jid": group_jid,
        "original_text": original_text,
        "query": query,
        "evolution_instance": instance_name,

        # RFC verificable
        "is_verifiable": bool(is_verifiable),
        "provider_rfc": (
            provider_rfc or ""
        ).strip().upper(),
        "provider_idcif": (
            provider_idcif or ""
        ).strip(),
    }

    url = (
        f"{BOT_INTERNAL_URL.rstrip('/')}"
        "/internal/generate-pdf"
    )

    r = requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=420,
    )

    print(
        "worker call_bot_internal_text instance:",
        instance_name,
        flush=True,
    )

    print(
        "worker call_bot_internal_text status:",
        r.status_code,
        flush=True,
    )

    print(
        "worker call_bot_internal_text resp:",
        r.text,
        flush=True,
    )

    r.raise_for_status()
    return r.json()
    
def call_bot_internal_media(
    requester_number: str,
    requester_name: str,
    group_jid: str,
    original_text: str,
    mime_type: str,
    media_bytes: bytes,
    instance_name=None,
):
    headers = {
        "Authorization": f"Bearer {BOT_INTERNAL_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "requester_number": requester_number,
        "requester_name": requester_name,
        "group_jid": group_jid,
        "original_text": original_text,
        "mime_type": mime_type,
        "media_b64": base64.b64encode(media_bytes).decode("utf-8"),
        "evolution_instance": instance_name,
    }
    url = f"{BOT_INTERNAL_URL.rstrip('/')}/internal/generate-pdf-from-media"
    r = requests.post(url, json=payload, headers=headers, timeout=420)
    print("worker call_bot_internal_media instance:", instance_name, flush=True)
    print("worker call_bot_internal_media status:", r.status_code, flush=True)
    print("worker call_bot_internal_media resp:", r.text, flush=True)
    r.raise_for_status()
    return r.json()

def _extraer_lugar_emision_desde_texto(raw: str) -> str:
    """
    Detecta MUNICIPIO, ENTIDAD en cualquier parte del texto,
    incluso si viene en la misma línea que RFC/CURP/IDCIF.
    """
    raw = (raw or "").strip().upper()
    if not raw:
        return ""

    m = re.search(r'([A-ZÁÉÍÓÚÜÑ\s]+)\s*,\s*([A-ZÁÉÍÓÚÜÑ\s]+)', raw)
    if m:
        mun = m.group(1).strip()
        ent = m.group(2).strip()
        if mun and ent:
            return f"{mun}, {ent}"

    lineas = [ln.strip() for ln in raw.replace("\r", "\n").split("\n") if ln.strip()]
    for ln in lineas:
        partes = [p.strip() for p in ln.split(",") if p.strip()]
        if len(partes) >= 2:
            return f"{partes[0].upper()}, {partes[-1].upper()}"

    return ""

def _normalize_delivery_value(value: str) -> str:
    value = (value or "").strip().upper()
    return re.sub(r"\s+", " ", value)


def _delivery_fingerprint(
    job_data: dict,
    item_key: str = "",
) -> str:
    """
    Identifica una entrega lógica sin depender del msg_id.

    Cada reenvío manual de WhatsApp tiene otro msg_id,
    pero debe considerarse la misma entrega.
    """
    instance = (
        job_data.get("evolution_instance")
        or EVOLUTION_INSTANCE
        or ""
    ).strip()

    group_jid = (
        job_data.get("group_jid")
        or ""
    ).strip()

    requester = (
        job_data.get("requester_number")
        or ""
    ).strip()

    request_key = (
        job_data.get("request_key")
        or ""
    ).strip()

    normalized_item = _normalize_delivery_value(
        item_key
    )

    if request_key:
        base = (
            f"{instance}|"
            f"{group_jid}|"
            f"{requester}|"
            f"{request_key}|"
            f"{normalized_item}"
        )
    else:
        query = _normalize_delivery_value(
            job_data.get("query")
            or job_data.get("original_text")
            or normalized_item
            or ""
        )

        base = (
            f"{instance}|"
            f"{group_jid}|"
            f"{requester}|"
            f"{query}|"
            f"{normalized_item}"
        )

    return hashlib.sha256(
        base.encode("utf-8")
    ).hexdigest()


def claim_delivery_once(
    job_data: dict,
    item_key: str = "",
) -> tuple[bool, str, str]:
    fingerprint = _delivery_fingerprint(
        job_data,
        item_key,
    )

    lock_key = (
        f"rfc:delivery:lock:{fingerprint}"
    )

    done_key = (
        f"rfc:delivery:done:{fingerprint}"
    )

    # Ya fue entregado recientemente.
    if redis_stats.exists(done_key):
        print(
            "[RFC DELIVERY ALREADY DONE]",
            done_key,
            flush=True,
        )
        return False, lock_key, done_key

    # Solo un worker puede obtener SET NX.
    claimed = redis_stats.set(
        lock_key,
        "1",
        nx=True,
        ex=DELIVERY_LOCK_TTL_SEC,
    )

    if not claimed:
        print(
            "[RFC DELIVERY ALREADY CLAIMED]",
            lock_key,
            flush=True,
        )
        return False, lock_key, done_key

    # Segunda revisión por una carrera mínima.
    if redis_stats.exists(done_key):
        redis_stats.delete(lock_key)
        return False, lock_key, done_key

    return True, lock_key, done_key


def mark_delivery_done(
    lock_key: str,
    done_key: str,
):
    pipe = redis_stats.pipeline()

    pipe.set(
        done_key,
        _panel_now().isoformat(),
        ex=DELIVERY_DONE_TTL_SEC,
    )

    pipe.delete(lock_key)
    pipe.execute()


def release_delivery_claim(lock_key: str):
    if lock_key:
        redis_stats.delete(lock_key)


def release_request_inflight(
    inflight_key: str,
):
    if not inflight_key:
        return

    try:
        redis_stats.delete(inflight_key)

        print(
            "[RFC REQUEST INFLIGHT RELEASED]",
            inflight_key,
            flush=True,
        )

    except Exception as release_exc:
        print(
            "[RFC REQUEST INFLIGHT RELEASE ERROR]",
            repr(release_exc),
            flush=True,
        )

def process_verifiable_timeout_job(
    request_key: str,
):
    """
    Se ejecuta 1 hora después de enviar una
    solicitud RFC verificable al proveedor.

    Si la solicitud todavía está pendiente:
    - avisa al cliente;
    - libera el inflight;
    - elimina el pendiente;
    - evita que una respuesta tardía genere PDF.
    """

    request_key = (
        request_key or ""
    ).strip()

    if not request_key:
        print(
            "[RFC VERIFIABLE TIMEOUT SKIP] "
            "EMPTY_REQUEST_KEY",
            flush=True,
        )
        return {
            "ok": True,
            "ignored": "empty_request_key",
        }

    pending = load_pending(
        request_key
    )

    # El proveedor ya respondió, llegó "no id"
    # o la solicitud ya fue finalizada.
    if not pending:
        print(
            "[RFC VERIFIABLE TIMEOUT SKIP]",
            {
                "request_key": request_key,
                "reason": "pending_not_found",
            },
            flush=True,
        )

        return {
            "ok": True,
            "ignored": "already_finished",
        }

    # Competencia segura:
    # - si el proveedor ya tomó el resultado, no avisar timeout;
    # - si este job toma el resultado, una respuesta posterior
    #   del proveedor ya no debe generar el documento.
    if not claim_provider_result(
        request_key
    ):
        print(
            "[RFC VERIFIABLE TIMEOUT SKIP]",
            {
                "request_key": request_key,
                "reason": (
                    "provider_result_already_claimed"
                ),
            },
            flush=True,
        )

        return {
            "ok": True,
            "ignored": "result_already_claimed",
        }

    client_group_jid = (
        pending.get("client_group_jid")
        or ""
    ).strip()

    client_instance = (
        pending.get("client_instance")
        or EVOLUTION_INSTANCE
        or "grupo02"
    ).strip()

    requester_label = (
        pending.get("requester_label")
        or pending.get("requester_name")
        or "Usuario"
    ).strip()

    original_type = (
        pending.get("original_query_type")
        or ""
    ).strip().upper()

    original_identifier = (
        pending.get("original_identifier")
        or ""
    ).strip().upper()

    inflight_key = (
        pending.get("inflight_key")
        or ""
    ).strip()

    provider_message_id = (
        pending.get("provider_message_id")
        or ""
    ).strip()

    if original_type == "CURP":
        identifier_text = (
            f"la CURP {original_identifier}"
        )

    elif original_type == "RFC_ONLY":
        identifier_text = (
            f"el RFC {original_identifier}"
        )

    else:
        identifier_text = (
            original_identifier
            or "el dato solicitado"
        )

    try:
        if client_group_jid:
            evolution_send_text_to_group(
                client_group_jid,
                (
                    f"⚠️ {requester_label}, "
                    "solicitud sin éxito después "
                    "de 15min-1h para "
                    f"{identifier_text}."
                ),
                instance_name=client_instance,
            )

        print(
            "[RFC VERIFIABLE TIMEOUT SENT]",
            {
                "request_key": request_key,
                "client_group": (
                    client_group_jid
                ),
                "client_instance": (
                    client_instance
                ),
                "original_type": original_type,
                "original_identifier": (
                    original_identifier
                ),
            },
            flush=True,
        )

    except Exception as send_exc:
        print(
            "[RFC VERIFIABLE TIMEOUT "
            "SEND ERROR]",
            {
                "request_key": request_key,
                "client_group": (
                    client_group_jid
                ),
                "error": repr(send_exc),
            },
            flush=True,
        )

    finally:
        # Permite que el cliente vuelva a enviar
        # la misma CURP o RFC.
        if inflight_key:
            try:
                redis_stats.delete(
                    inflight_key
                )

                print(
                    "[RFC VERIFIABLE TIMEOUT "
                    "INFLIGHT RELEASED]",
                    inflight_key,
                    flush=True,
                )

            except Exception as inflight_exc:
                print(
                    "[RFC VERIFIABLE TIMEOUT "
                    "INFLIGHT RELEASE ERROR]",
                    repr(inflight_exc),
                    flush=True,
                )

        # Elimina pendiente y asociación con
        # el mensaje enviado al proveedor.
        finish_pending(
            request_key,
            provider_message_id=(
                provider_message_id
            ),
        )

    return {
        "ok": True,
        "timeout": True,
        "request_key": request_key,
        "identifier": original_identifier,
    }

def record_verifiable_provider_success(
    job_data: dict,
    count: int = 1,
):
    if not bool(
        job_data.get("is_verifiable")
    ):
        return

    provider_db_name = (
        job_data.get(
            "verifiable_provider_db_name"
        )
        or ""
    ).strip().upper()

    provider_name = (
        job_data.get(
            "verifiable_provider_name"
        )
        or provider_db_name
        or "RFC VERIFICABLE"
    ).strip()

    provider_group_jid = (
        job_data.get(
            "verifiable_provider_group"
        )
        or ""
    ).strip()

    request_key = (
        job_data.get(
            "verifiable_request_key"
        )
        or job_data.get("request_key")
        or ""
    ).strip()

    if not provider_db_name:
        print(
            "VERIFIABLE_PROVIDER_STAT_SKIPPED =",
            {
                "reason": "provider_db_name_empty",
                "request_key": request_key,
            },
            flush=True,
        )
        return

    if not request_key:
        print(
            "VERIFIABLE_PROVIDER_STAT_SKIPPED =",
            {
                "reason": "request_key_empty",
                "provider": provider_db_name,
            },
            flush=True,
        )
        return

    db = SessionLocal()

    try:
        existing = (
            db.query(
                VerifiableProviderStat
            )
            .filter(
                VerifiableProviderStat.request_key
                == request_key
            )
            .first()
        )

        if existing:
            return

        row = VerifiableProviderStat(
            provider_db_name=provider_db_name,
            provider_name=provider_name,
            provider_group_jid=(
                provider_group_jid
            ),
            status="DONE",
            request_key=request_key,
            count=max(
                int(count or 1),
                1,
            ),
            created_at=datetime.utcnow(),
        )

        db.add(row)
        db.commit()

        print(
            "VERIFIABLE_PROVIDER_SUCCESS_RECORDED =",
            {
                "provider_db_name": (
                    provider_db_name
                ),
                "provider_name": provider_name,
                "request_key": request_key,
                "count": max(
                    int(count or 1),
                    1,
                ),
            },
            flush=True,
        )

    except Exception as stat_exc:
        db.rollback()

        print(
            "VERIFIABLE_PROVIDER_STAT_ERROR =",
            {
                "provider_db_name": (
                    provider_db_name
                ),
                "request_key": request_key,
                "error": repr(stat_exc),
            },
            flush=True,
        )

        raise

    finally:
        db.close()

def process_group_request_job(job_data: dict):
    requester_number = job_data["requester_number"]
    requester_name = job_data["requester_name"]
    requester_label = job_data["requester_label"]
    group_jid = job_data["group_jid"]
    group_name = job_data.get("group_name") or group_jid
    original_text = job_data["original_text"]
    query = job_data.get("query")
    msg_type = job_data.get("msg_type") or ""
    media_id = job_data.get("media_id") or ""
    mime_type = job_data.get("mime_type") or ""
    inflight_key = (
        job_data.get("inflight_key")
        or ""
    ).strip()

    try:
        request_started_at_epoch = float(
            job_data.get(
                "request_started_at_epoch"
            )
            or time.time()
        )
    except Exception:
        request_started_at_epoch = (
            time.time()
        )

    is_verifiable = bool(
        job_data.get("is_verifiable")
    )

    verifiable_request_key = (
        job_data.get(
            "verifiable_request_key"
        )
        or ""
    ).strip()

    instance_name = (job_data.get("evolution_instance") or EVOLUTION_INSTANCE).strip()
    print("[WORKER EVOLUTION INSTANCE]", repr(instance_name), flush=True)

    print("[WORKER GROUP NAME]", repr(group_name), flush=True)

    if is_verifiable:
        print(
            "[RFC VERIFIABLE WORKER START]",
            {
                "request_key": (
                    verifiable_request_key
                ),
                "original_type": (
                    job_data.get(
                        "verifiable_original_type"
                    )
                ),
                "original_identifier": (
                    job_data.get(
                        "verifiable_original_identifier"
                    )
                ),
                "provider_rfc": (
                    job_data.get("provider_rfc")
                ),
                "provider_idcif": (
                    job_data.get(
                        "provider_idcif"
                    )
                ),
                "client_instance": (
                    instance_name
                ),
                "client_group": group_jid,
            },
            flush=True,
        )

    try:
        requested_kind = _classify_success_kind(
            query=query or "",
            original_text=original_text or "",
            msg_type=msg_type or ""
        )

        forced_success_kind = (
            job_data.get("forced_success_kind")
            or ""
        ).strip().upper()
        
        # Todo resultado nacido del flujo verificable
        # debe contar y descontarse como RFC_VERIFICABLE,
        # aunque internamente se genere como RFC_ONLY.
        if is_verifiable:
            forced_success_kind = "RFC_VERIFICABLE"
        
        if forced_success_kind:
            requested_kind = forced_success_kind

        if not _rfc_commercial_check_or_notify(
            job_data=job_data,
            group_jid=group_jid,
            group_name=group_name,
            instance_name=instance_name,
            kind=requested_kind
        ):
            return

        if query:
            try:
                lugar_line = _extraer_lugar_emision_desde_texto(original_text)

                q_lines = [ln.strip() for ln in (query or "").splitlines() if ln.strip()]
                trae_lugar = any("," in ln for ln in q_lines)

                # ✅ si original_text trae lugar y query no lo trae, anexarlo SIEMPRE
                if lugar_line and (not trae_lugar):
                    query = f"{query.rstrip()}\n{lugar_line}"

            except Exception as e:
                print("worker merge lugar fail:", repr(e), flush=True)

            print("[WORKER ORIGINAL_TEXT RAW]", repr(original_text), flush=True)
            print("[WORKER ORIGINAL_TEXT LINES]", (original_text or "").splitlines(), flush=True)
            print("[WORKER QUERY RAW]", repr(query), flush=True)
            print("[WORKER QUERY LINES]", (query or "").splitlines(), flush=True)

            result = call_bot_internal_text(
                requester_number=requester_number,
                requester_name=requester_name,
                group_jid=group_jid,
                original_text=original_text,
                query=query,
                instance_name=instance_name,
            
                is_verifiable=is_verifiable,
            
                provider_rfc=(
                    job_data.get("provider_rfc")
                    or ""
                ),
            
                provider_idcif=(
                    job_data.get("provider_idcif")
                    or ""
                ),
            )
            
        elif msg_type in ("image", "document") and media_id:
            media_bytes = evolution_get_media_base64(media_id, instance_name=instance_name)

            result = call_bot_internal_media(
                requester_number=requester_number,
                requester_name=requester_name,
                group_jid=group_jid,
                original_text=original_text,
                mime_type=mime_type,
                media_bytes=media_bytes,
                instance_name=instance_name,
            )
        else:
            raise RuntimeError("NO_TEXT_OR_MEDIA")

        verifiable_warning_code = (
            result.get(
                "verifiable_provider_warning_code"
            )
            or ""
        ).strip().upper()
        
        verifiable_fallback_used = bool(
            result.get(
                "verifiable_fallback_used"
            )
        )

        if not result.get("ok"):
            err = result.get("error") or "No fue posible generar el documento."
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} {err}",
                instance_name=instance_name
            )
            return

        kind = _classify_success_kind(query=query or "", original_text=original_text or "", msg_type=msg_type)
        if forced_success_kind:
            kind = forced_success_kind
        mode = (result.get("mode") or "single").strip().lower()

        if mode == "batch_zip":
            zip_url = (result.get("zip_url") or "").strip()
            file_name = (result.get("filename") or "constancias_lote.zip").strip()
            ok_count = int(result.get("ok_count") or 0)

            if not zip_url:
                evolution_send_text_to_group(
                    group_jid,
                    f"❌ {requester_label} no se obtuvo enlace del lote.",
                    instance_name=instance_name
                )
                return

            delivery_item_key = (
                f"BATCH_ZIP:"
                f"{query or original_text or file_name}"
            )
            
            (
                claimed,
                delivery_lock_key,
                delivery_done_key,
            ) = claim_delivery_once(
                job_data=job_data,
                item_key=delivery_item_key,
            )
            
            if not claimed:
                print(
                    "[RFC BATCH ZIP DUPLICATE SUPPRESSED]",
                    delivery_item_key,
                    flush=True,
                )
                return
            
            try:
                evolution_send_media_to_group(
                    group_jid=group_jid,
                    media_url=zip_url,
                    file_name=file_name,
                    instance_name=instance_name,
                )
            
            except requests.Timeout as media_err:
                print(
                    "[RFC BATCH ZIP TIMEOUT - CLAIM RETAINED]",
                    repr(media_err),
                    delivery_lock_key,
                    flush=True,
                )
                return
            
            except Exception as media_err:
                print(
                    "[RFC BATCH ZIP SEND ERROR]",
                    repr(media_err),
                    flush=True,
                )
            
                release_delivery_claim(
                    delivery_lock_key
                )
            
                evolution_send_text_to_group(
                    group_jid,
                    (
                        f"⚠️ {requester_label} "
                        "el lote se generó, "
                        "pero no pude adjuntarlo.\n"
                        f"{zip_url}"
                    ),
                    instance_name=instance_name,
                )
            
                return

            try:
                if ok_count > 0:
                    success_recorded = (
                        record_success_once(
                            job_data=job_data,
                            group_jid=group_jid,
                            group_name=group_name,
                            kind=kind,
                            count=ok_count,
                            item_key=
                                delivery_item_key,
                        )
                    )
            
                    if (
                        success_recorded
                        and bool(
                            job_data.get(
                                "verifiable_count_provider_success",
                                True,
                            )
                        )
                    ):
                        record_verifiable_provider_success(
                            job_data,
                            count=1,
                        )
            
                mark_delivery_done(
                    delivery_lock_key,
                    delivery_done_key,
                )

                if (
                    is_verifiable
                    and verifiable_request_key
                ):
                    finish_pending(
                        verifiable_request_key,
                        provider_message_id=(
                            job_data.get(
                                "provider_response_msg_id"
                            )
                            or ""
                        ),
                    )
                
                    print(
                        "[RFC VERIFIABLE PENDING FINISHED]",
                        {
                            "request_key":
                                verifiable_request_key,
                            "mode": "batch_zip",
                        },
                        flush=True,
                    )
            
            except Exception as accounting_exc:
                print(
                    "[RFC BATCH ZIP ACCOUNTING ERROR]",
                    repr(accounting_exc),
                    {
                        "group_jid": group_jid,
                        "kind": kind,
                        "count": ok_count,
                        "item_key":
                            delivery_item_key,
                    },
                    flush=True,
                )
            
                raise
            
            return

        if mode == "batch_multi":
            items = result.get("items") or []
            provider_success_count = 0

            for item in items:
                pdf_url = (item.get("pdf_url") or "").strip()
                file_name = (item.get("filename") or "documento.pdf").strip()
                err = (item.get("error") or "").strip()
                rfc = (item.get("rfc") or "").strip()
                idcif = (item.get("idcif") or "").strip()

                if pdf_url and not err:
                    item_key = (
                        rfc
                        or idcif
                        or file_name
                        or pdf_url
                    )
                
                    (
                        claimed,
                        delivery_lock_key,
                        delivery_done_key,
                    ) = claim_delivery_once(
                        job_data=job_data,
                        item_key=item_key,
                    )
                
                    if not claimed:
                        print(
                            "[RFC BATCH ITEM DUPLICATE SUPPRESSED]",
                            item_key,
                            flush=True,
                        )
                        continue
                
                    try:
                        evolution_send_media_to_group(
                            group_jid=group_jid,
                            media_url=pdf_url,
                            file_name=file_name,
                            instance_name=instance_name,
                        )
                    
                    except requests.Timeout as media_err:
                        print(
                            "[RFC BATCH ITEM TIMEOUT - CLAIM RETAINED]",
                            repr(media_err),
                            delivery_lock_key,
                            flush=True,
                        )
                        continue
                    
                    except Exception as media_err:
                        print(
                            "[RFC BATCH ITEM SEND ERROR]",
                            repr(media_err),
                            flush=True,
                        )
                    
                        release_delivery_claim(
                            delivery_lock_key
                        )
                    
                        evolution_send_text_to_group(
                            group_jid,
                            (
                                f"⚠️ {requester_label} "
                                f"no pude adjuntar "
                                f"{file_name}.\n{pdf_url}"
                            ),
                            instance_name=instance_name,
                        )
                    
                        continue

                    try:
                        success_recorded = (
                            record_success_once(
                                job_data=job_data,
                                group_jid=group_jid,
                                group_name=group_name,
                                kind=kind,
                                count=1,
                                item_key=item_key,
                            )
                        )
                    
                        if success_recorded:
                            provider_success_count += 1
                    
                        mark_delivery_done(
                            delivery_lock_key,
                            delivery_done_key,
                        )
                    
                    except Exception as accounting_exc:
                        print(
                            "[RFC BATCH ITEM ACCOUNTING ERROR]",
                            repr(accounting_exc),
                            {
                                "group_jid": group_jid,
                                "kind": kind,
                                "item_key": item_key,
                            },
                            flush=True,
                        )
                    
                        raise
                
                else:
                    evolution_send_text_to_group(
                        group_jid,
                        f"❌ {requester_label} fallo {rfc} {idcif}: {err or 'error desconocido'}",
                        instance_name=instance_name
                    )

            if provider_success_count > 0:
                record_verifiable_provider_success(
                    job_data,
                    count=provider_success_count,
                )
            
                if (
                    is_verifiable
                    and verifiable_request_key
                ):
                    finish_pending(
                        verifiable_request_key,
                        provider_message_id=(
                            job_data.get(
                                "provider_response_msg_id"
                            )
                            or ""
                        ),
                    )
            
                    print(
                        "[RFC VERIFIABLE PENDING FINISHED]",
                        {
                            "request_key":
                                verifiable_request_key,
                            "mode": "batch_multi",
                        },
                        flush=True,
                    )
                
            return

        pdf_url = (result.get("pdf_url") or "").strip()
        file_name = (result.get("filename") or "documento.pdf").strip()

        if not pdf_url:
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} no se obtuvo enlace del PDF.",
                instance_name=instance_name
            )
            return

        delivery_item_key = (
            query
            or original_text
            or file_name
            or pdf_url
        )
        
        (
            claimed,
            delivery_lock_key,
            delivery_done_key,
        ) = claim_delivery_once(
            job_data=job_data,
            item_key=delivery_item_key,
        )
        
        if not claimed:
            print(
                "[RFC PDF DUPLICATE SUPPRESSED]",
                delivery_item_key,
                flush=True,
            )
            return

        time_caption = ""

        elapsed_seconds = max(
            0.0,
            time.time()
            - request_started_at_epoch,
        )
        
        time_caption = (
            "⏱️ Tiempo total: "
            + _format_total_time(
                elapsed_seconds
            )
        )
        
        if is_verifiable:
            time_caption += (
                "\nRFC verificable entregado"
            )
        
        try:
            evolution_send_media_to_group(
                group_jid=group_jid,
                media_url=pdf_url,
                file_name=file_name,
                instance_name=instance_name,
                caption=time_caption,
            )
        
        except requests.Timeout as media_err:
            print(
                "[RFC PDF TIMEOUT - CLAIM RETAINED]",
                repr(media_err),
                delivery_lock_key,
                flush=True,
            )
            return
        
        except Exception as media_err:
            print(
                "[RFC PDF SEND ERROR]",
                repr(media_err),
                flush=True,
            )
        
            release_delivery_claim(
                delivery_lock_key
            )
        
            evolution_send_text_to_group(
                group_jid,
                (
                    f"⚠️ {requester_label} "
                    "el documento se generó, "
                    "pero no pude adjuntarlo.\n"
                    f"{pdf_url}"
                ),
                instance_name=instance_name,
            )
        
            return

        if bool(
            job_data.get(
                "verifiable_identifier_corrected"
            )
        ):
            original_identifier = (
                job_data.get(
                    "verifiable_original_identifier"
                )
                or ""
            ).strip().upper()
        
            corrected_rfc = (
                job_data.get("provider_rfc")
                or ""
            ).strip().upper()
        
            match_method = (
                job_data.get(
                    "provider_match_method"
                )
                or ""
            ).strip()
        
            try:
                evolution_send_text_to_group(
                    group_jid,
                    (
                        "ℹ️ Se detectó una corrección "
                        "en el dato solicitado.\n\n"
                        f"Dato enviado: "
                        f"{original_identifier}\n"
                        f"RFC localizado: "
                        f"{corrected_rfc}\n\n"
                        "La constancia fue generada con "
                        "el RFC localizado."
                    ),
                    instance_name=instance_name,
                )
        
                print(
                    "RFC_VERIFIABLE_IDENTIFIER_"
                    "CORRECTION_NOTIFIED =",
                    {
                        "request_key": (
                            job_data.get(
                                "verifiable_request_key"
                            )
                        ),
                        "original_identifier": (
                            original_identifier
                        ),
                        "provider_rfc": corrected_rfc,
                        "match_method": match_method,
                    },
                    flush=True,
                )
        
            except Exception as correction_exc:
                print(
                    "RFC_VERIFIABLE_IDENTIFIER_"
                    "CORRECTION_NOTICE_ERROR =",
                    {
                        "request_key": (
                            job_data.get(
                                "verifiable_request_key"
                            )
                        ),
                        "error": repr(
                            correction_exc
                        ),
                    },
                    flush=True,
                )

        try:
            success_recorded = record_success_once(
                job_data=job_data,
                group_jid=group_jid,
                group_name=group_name,
                kind=kind,
                count=1,
                item_key=delivery_item_key,
            )
        
            if (
                success_recorded
                and bool(
                    job_data.get(
                        "verifiable_count_provider_success",
                        True,
                    )
                )
            ):
                record_verifiable_provider_success(
                    job_data,
                    count=1,
                )
        
            if (
                success_recorded
                and is_verifiable
                and verifiable_fallback_used
                and verifiable_warning_code
            ):
                notify_verifiable_provider_sat_rejection(
                    job_data=job_data,
                    error_code=verifiable_warning_code,
                )
        
            mark_delivery_done(
                delivery_lock_key,
                delivery_done_key,
            )
        
            # Una vez entregado y contabilizado correctamente,
            # elimina definitivamente el pendiente verificable.
            if (
                is_verifiable
                and verifiable_request_key
            ):
                finish_pending(
                    verifiable_request_key,
                    provider_message_id=(
                        job_data.get(
                            "provider_response_msg_id"
                        )
                        or ""
                    ),
                )
        
                print(
                    "[RFC VERIFIABLE PENDING FINISHED]",
                    {
                        "request_key": (
                            verifiable_request_key
                        ),
                        "provider_response_msg_id": (
                            job_data.get(
                                "provider_response_msg_id"
                            )
                            or ""
                        ),
                    },
                    flush=True,
                )
        
        except Exception as accounting_exc:
            print(
                "[RFC DELIVERY ACCOUNTING ERROR]",
                repr(accounting_exc),
                {
                    "group_jid": group_jid,
                    "kind": kind,
                    "item_key":
                        delivery_item_key,
                },
                flush=True,
            )
        
            # No enviar al cliente un mensaje falso
            # diciendo que el archivo no se adjuntó.
            raise

    except requests.HTTPError as e:
        print("process_group_request_job HTTPError:", repr(e), flush=True)
        traceback.print_exc()
    
        resp_text = ""
        err_code = ""
        try:
            resp_text = e.response.text or ""
        except Exception:
            pass
    
        print("process_group_request_job HTTP response body:", resp_text, flush=True)
    
        try:
            try:
                obj = json.loads(resp_text) if resp_text else {}
                err_code = str(
                    obj.get("error")
                    or obj.get("detail")
                    or ""
                ).strip().upper()
            except Exception:
                err_code = ""
    
            if "QR_NOT_SAT_DOMAIN" in resp_text or err_code == "QR_NOT_SAT_DOMAIN":
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el QR no corresponde a un enlace oficial del SAT.",
                    instance_name=instance_name
                )
            elif "QR_NOT_READABLE" in resp_text or err_code == "QR_NOT_READABLE":
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} no pude leer el QR. Envíalo más cerca, más nítido y con buena luz.",
                    instance_name=instance_name
                )
            elif "MIME_NOT_SUPPORTED" in resp_text or err_code == "MIME_NOT_SUPPORTED":
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} ese tipo de archivo aún no es compatible. Envíalo como imagen.",
                    instance_name=instance_name
                )
            elif err_code in {
                "SIN_DATOS_SAT",
                "SAT_CIF_NOT_ISSUED",
                "SAT_NO_ACTIVE_REGIME",
                "SAT_STATUS_SUSPENDED",
                "CLIENT_RFC_CANCELLED",
                "CLIENT_RFC_SUSPENDED",
                "CLIENT_RFC_INACTIVE",
                "CLIENT_RFC_CP_EMPTY",
                "CLIENT_RFC_REGIME_EMPTY",
                "CLIENT_RFC_CP_AND_REGIME_EMPTY",
            }:
                client_reason_map = {
                    "SIN_DATOS_SAT": (
                        "el IDCIF/QR se leyó, pero la página "
                        "oficial del SAT no arrojó información"
                    ),
                    "SAT_CIF_NOT_ISSUED": (
                        "el SAT indica que a este RFC no se le "
                        "ha emitido una Cédula de Identificación "
                        "Fiscal. El IDCIF entregado no pudo "
                        "validarse para ese RFC"
                    ),
                    "SAT_NO_ACTIVE_REGIME": (
                        "el RFC aparece sin régimen fiscal "
                        "vigente en la página oficial del SAT"
                    ),
                    "SAT_STATUS_SUSPENDED": (
                        "el RFC aparece suspendido o no activo "
                        "en la página oficial del SAT"
                    ),
                    "CLIENT_RFC_CANCELLED": (
                        "el RFC aparece cancelado "
                        "en la consulta oficial"
                    ),
                    "CLIENT_RFC_SUSPENDED": (
                        "el RFC aparece suspendido "
                        "en la consulta oficial"
                    ),
                    "CLIENT_RFC_INACTIVE": (
                        "el RFC aparece como no activo "
                        "en la consulta oficial"
                    ),
                    "CLIENT_RFC_CP_EMPTY": (
                        "se localizó el RFC, pero el sistema "
                        "no devolvió un código postal válido"
                    ),
                    "CLIENT_RFC_REGIME_EMPTY": (
                        "se localizó el RFC, pero el sistema "
                        "no devolvió un régimen fiscal vigente"
                    ),
                    "CLIENT_RFC_CP_AND_REGIME_EMPTY": (
                        "se localizó el RFC, pero el sistema "
                        "no devolvió código postal ni régimen "
                        "fiscal vigente"
                    ),
                }
            
                client_reason = (
                    client_reason_map[err_code]
                )
            
                evolution_send_text_to_group(
                    group_jid,
                    (
                        f"⚠️ {requester_label}, "
                        f"{client_reason}. "
                        "No se generó la constancia."
                    ),
                    instance_name=instance_name,
                )
            
                notify_verifiable_provider_sat_rejection(
                    job_data=job_data,
                    error_code=err_code,
                )
            
                print(
                    "RFC_VERIFIABLE_SAT_REJECTED =",
                    {
                        "request_key": (
                            job_data.get(
                                "verifiable_request_key"
                            )
                        ),
                        "error_code": err_code,
                        "client_group": group_jid,
                        "provider_group": (
                            job_data.get(
                                "verifiable_provider_group"
                            )
                        ),
                        "provider_rfc": (
                            job_data.get("provider_rfc")
                        ),
                        "provider_idcif": (
                            job_data.get("provider_idcif")
                        ),
                    },
                    flush=True,
                )
            elif "CLIENT_CURP_NOT_FOUND_OR_WRONG" in resp_text or err_code == "CLIENT_CURP_NOT_FOUND_OR_WRONG":
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} la CURP no fue encontrada.\n\n"
                    "Verifica que esté escrita correctamente y vuelve a enviarla.",
                    instance_name=instance_name
                )
            elif "CLIENT_RFC_NOT_FOUND_OR_WRONG" in resp_text or err_code == "CLIENT_RFC_NOT_FOUND_OR_WRONG":
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el RFC no fue encontrado.\n\n"
                    "Verifica que esté escrito correctamente y vuelve a enviarlo.",
                    instance_name=instance_name
                )
            elif (
                "CLIENT_RFC_CANCELLED"
                in resp_text
                or err_code
                == "CLIENT_RFC_CANCELLED"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    (
                        f"⚠️ {requester_label} "
                        "el RFC aparece como cancelado "
                        "en la consulta oficial.\n\n"
                        "No se generó la constancia."
                    ),
                    instance_name=instance_name,
                )

            elif (
                "CLIENT_RFC_SUSPENDED"
                in resp_text
                or err_code
                == "CLIENT_RFC_SUSPENDED"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    (
                        f"⚠️ {requester_label} "
                        "el RFC aparece como suspendido "
                        "en la consulta oficial.\n\n"
                        "No se generó la constancia."
                    ),
                    instance_name=instance_name,
                )

            elif (
                "CLIENT_RFC_INACTIVE"
                in resp_text
                or err_code
                == "CLIENT_RFC_INACTIVE"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    (
                        f"⚠️ {requester_label} "
                        "el RFC aparece como no activo "
                        "en la consulta oficial.\n\n"
                        "No se generó la constancia."
                    ),
                    instance_name=instance_name,
                )
            elif (
                "CLIENT_CHECKID_INCOMPLETE_DATA_CLON_REQUIRED" in resp_text
                or err_code.startswith("CLIENT_CHECKID_INCOMPLETE_DATA_CLON_REQUIRED")
            ):
                curp_req = ""
            
                try:
                    obj = json.loads(resp_text) if resp_text else {}
                    raw_error = str(
                        obj.get("error")
                        or obj.get("detail")
                        or ""
                    ).strip()
            
                    if ":" in raw_error:
                        curp_req = raw_error.split(":", 1)[1].strip().upper()
                except Exception:
                    curp_req = ""
            
                if not curp_req:
                    m = re.search(
                        r"CLIENT_CHECKID_INCOMPLETE_DATA_CLON_REQUIRED:([A-Z0-9]{18})",
                        resp_text,
                        flags=re.I
                    )
                    if m:
                        curp_req = m.group(1).strip().upper()
            
                if not curp_req:
                    curp_req = "LA MISMA CURP"
            
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} se encontró información, pero está incompleta.\n\n"
                    "No se generó el documento para evitar datos incorrectos.",
                    instance_name=instance_name
                )
            
            elif "CLIENT_CHECKID_INCOMPLETE_DATA" in resp_text or err_code == "CLIENT_CHECKID_INCOMPLETE_DATA":
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} se encontró información, pero está incompleta.\n\n"
                    "No se generó el documento para evitar datos incorrectos. Verifica la CURP/RFC o intenta más tarde.",
                    instance_name=instance_name
                )

            elif (
                "FALTA APELLIDO PATERNO" in err_code
                or "FALTA APELLIDO PATERNO" in resp_text.upper()
                or "FALTA APELLIDO MATERNO" in err_code
                or "FALTA APELLIDO MATERNO" in resp_text.upper()
                or "FALTA NOMBRE" in err_code
                or "FALTA NOMBRE" in resp_text.upper()
            ):
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} no se encontró información para esta CURP.\n"
                    "Verifica que esté escrita correctamente o que se encuentre certificada.",
                    instance_name=instance_name
                )

            elif (
                "GOB_CURP_FAIL:TIMEOUTEXCEPTION" in err_code
                or "GOB_CURP_FAIL:TIMEOUTEXCEPTION" in resp_text.upper()
            ):
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label}, el servicio de consulta de CURP "
                    "no respondió a tiempo.\n"
                    "La CURP no fue marcada como inexistente. "
                    "Intenta nuevamente en unos momentos.",
                    instance_name=instance_name
                )

            else:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} ocurrió una interrupción procesando la solicitud. Intenta de nuevo en 2-3 minutos",
                    instance_name=instance_name
                )
        except Exception:
            pass

    except Exception as e:
        print("process_group_request_job error:", repr(e), flush=True)
        traceback.print_exc()
        try:
            evolution_send_text_to_group(
                group_jid,
                f"⚠️ {requester_label} ocurrió una interrupción procesando la solicitud. Intenta de nuevo en 2-3 minutos",
                instance_name=instance_name
            )
        except Exception:
            pass

    finally:
        release_request_inflight(
            inflight_key
        )
    
        if is_verifiable and verifiable_request_key:
            try:
                release_provider_result_claim(
                    verifiable_request_key
                )
        
                print(
                    "[RFC VERIFIABLE RESULT CLAIM RELEASED]",
                    verifiable_request_key,
                    flush=True,
                )
        
            except Exception as claim_release_exc:
                print(
                    "[RFC VERIFIABLE RESULT CLAIM RELEASE ERROR]",
                    repr(claim_release_exc),
                    flush=True,
                )

def evolution_get_media_base64(message_id: str, instance_name=None):
    instance_name = (instance_name or EVOLUTION_INSTANCE).strip()

    url = f"{EVOLUTION_BASE_URL}/chat/getBase64FromMediaMessage/{instance_name}"
    payload = {"message": {"key": {"id": message_id}}}

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=120)
    print("worker getBase64 instance:", instance_name, flush=True)
    print("worker getBase64 payload:", payload, flush=True)
    print("worker getBase64 resp:", r.status_code, r.text[:1000], flush=True)
    r.raise_for_status()

    data = r.json() or {}
    b64 = (data.get("base64") or data.get("data") or "").strip()
    if not b64:
        raise RuntimeError("MEDIA_BASE64_EMPTY")

    return base64.b64decode(b64)


def _save_rfc_panel_request_log(job_data: dict, result: dict | None = None, status: str = "DONE", error_message: str = ""):
    """
    Guarda solicitudes RFC en request_logs usando SQL directo.
    Así el panel tipo actas puede mostrar solicitudes recientes, resumen por grupo,
    resumen por tipo y estado de solicitudes.
    """
    try:
        import os
        import hashlib
        from datetime import datetime, timedelta

        from dotenv import load_dotenv
        from sqlalchemy import create_engine, text

        load_dotenv("/opt/rfc-grupo02-bot/.env")

        result = result or {}

        db_url = os.getenv("DATABASE_URL", "").strip()
        if not db_url:
            print("[RFC_REQUEST_LOG_SAVE_SKIP] DATABASE_URL vacío", flush=True)
            return

        instance_name = (
            job_data.get("evolution_instance")
            or job_data.get("instance_name")
            or "grupo02"
        )

        group_jid = (
            job_data.get("group_jid")
            or job_data.get("source_group_id")
            or ""
        )

        msg_id = (
            job_data.get("msg_id")
            or job_data.get("message_id")
            or ""
        )

        requester_number = (
            job_data.get("requester_number")
            or job_data.get("requester_wa_id")
            or ""
        )

        requester_name = (
            job_data.get("requester_name")
            or job_data.get("requester_label")
            or ""
        )

        original_text = job_data.get("original_text") or ""
        detected_query = result.get("detected_query") or ""
        query = job_data.get("query") or detected_query or original_text or ""

        query_type = (
            job_data.get("query_type")
            or job_data.get("msg_type")
            or "RFC"
        )

        query_type = str(query_type or "RFC").upper()

        if query_type in ("IMAGE", "DOCUMENT"):
            act_type = "QR"
        elif query_type == "RFC_ONLY":
            act_type = "RFC"
        elif query_type == "RFC_IDCIF":
            act_type = "RFC_IDCIF"
        elif query_type == "RFC_VERIFICABLE":
            act_type = "RFC_VERIFICABLE"
        elif query_type == "CURP":
            act_type = "CURP"
        elif query_type == "QR_TEXT":
            act_type = "QR"
        else:
            act_type = query_type[:30]

        base_key = f"{instance_name}|{group_jid}|{requester_number}|{msg_id}|{query}|{act_type}"
        request_key = "rfc:" + hashlib.sha1(base_key.encode("utf-8")).hexdigest()

        now = datetime.utcnow()
        exp = now + timedelta(days=1)

        pdf_url = result.get("pdf_url") or job_data.get("pdf_url") or None
        pdf_filename = (
            result.get("filename")
            or result.get("pdf_filename")
            or job_data.get("item_key")
            or job_data.get("pdf_filename")
            or None
        )

        curp_field = str(query or original_text or result.get("detected_query") or pdf_filename or "")[:40]

        engine = create_engine(db_url, pool_pre_ping=True)

        sql = text("""
            INSERT INTO request_logs (
                request_key,
                curp,
                act_type,
                requester_wa_id,
                requester_name,
                source_chat_id,
                source_group_id,
                instance_name,
                evolution_message_id,
                provider_name,
                provider_group_id,
                provider_message,
                pdf_url,
                pdf_filename,
                pdf_saved_at,
                pdf_expires_at,
                api_charged,
                api_count_in_panel,
                resent_from_history,
                status,
                error_message,
                created_at,
                updated_at,
                expires_at
            )
            VALUES (
                :request_key,
                :curp,
                :act_type,
                :requester_wa_id,
                :requester_name,
                :source_chat_id,
                :source_group_id,
                :instance_name,
                :evolution_message_id,
                :provider_name,
                :provider_group_id,
                :provider_message,
                :pdf_url,
                :pdf_filename,
                :pdf_saved_at,
                :pdf_expires_at,
                :api_charged,
                :api_count_in_panel,
                :resent_from_history,
                :status,
                :error_message,
                :created_at,
                :updated_at,
                :expires_at
            )
            ON CONFLICT (request_key)
            DO UPDATE SET
                curp = EXCLUDED.curp,
                act_type = EXCLUDED.act_type,
                requester_wa_id = EXCLUDED.requester_wa_id,
                requester_name = EXCLUDED.requester_name,
                source_chat_id = EXCLUDED.source_chat_id,
                source_group_id = EXCLUDED.source_group_id,
                instance_name = EXCLUDED.instance_name,
                evolution_message_id = EXCLUDED.evolution_message_id,
                provider_name = EXCLUDED.provider_name,
                provider_group_id = EXCLUDED.provider_group_id,
                provider_message = EXCLUDED.provider_message,
                pdf_url = EXCLUDED.pdf_url,
                pdf_filename = EXCLUDED.pdf_filename,
                pdf_saved_at = EXCLUDED.pdf_saved_at,
                pdf_expires_at = EXCLUDED.pdf_expires_at,
                api_charged = EXCLUDED.api_charged,
                api_count_in_panel = EXCLUDED.api_count_in_panel,
                resent_from_history = EXCLUDED.resent_from_history,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at,
                expires_at = EXCLUDED.expires_at
        """)

        with engine.begin() as conn:
            conn.execute(sql, {
                "request_key": request_key,
                "curp": curp_field,
                "act_type": act_type,
                "requester_wa_id": str(requester_number or "")[:50],
                "requester_name": str(requester_name or "")[:150],
                "source_chat_id": group_jid,
                "source_group_id": group_jid,
                "instance_name": str(instance_name or "")[:50],
                "evolution_message_id": str(msg_id or "")[:120],
                "provider_name": (
                    "RFC_VERIFICABLE"
                    if act_type == "RFC_VERIFICABLE"
                    else "RFC"
                ),
                "provider_group_id": "RFC",
                "provider_message": original_text,
                "pdf_url": pdf_url,
                "pdf_filename": pdf_filename,
                "pdf_saved_at": now if pdf_url else None,
                "pdf_expires_at": exp if pdf_url else None,
                "api_charged": False,
                "api_count_in_panel": True,
                "resent_from_history": False,
                "status": status,
                "error_message": error_message or None,
                "created_at": now,
                "updated_at": now,
                "expires_at": exp,
            })

        print("[RFC_REQUEST_LOG_SAVED]", {
            "request_key": request_key,
            "group_jid": group_jid,
            "act_type": act_type,
            "status": status,
            "pdf_filename": pdf_filename,
        }, flush=True)

    except Exception as e:
        print("[RFC_REQUEST_LOG_SAVE_ERROR]", repr(e), {
            "job_data": job_data,
            "result": result,
            "status": status,
        }, flush=True)


def _rfc_plan_engine():
    import os
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    load_dotenv("/opt/rfc-grupo02-bot/.env")
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL_EMPTY")
    return create_engine(db_url, pool_pre_ping=True)


def _rfc_plan_get(group_jid: str, instance_name: str):
    from sqlalchemy import text

    engine = _rfc_plan_engine()

    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT
                group_jid,
                instance_name,
                group_name,
                clon_enabled,
                clon_balance,
                clon_used,
                clon_price,
                idcif_enabled,
                idcif_weekly_price,
                idcif_starts_at,
                idcif_expires_at,
                idcif_used
            FROM rfc_group_plans
            WHERE group_jid = :group_jid
              AND instance_name = :instance_name
            LIMIT 1
        """), {
            "group_jid": group_jid,
            "instance_name": instance_name,
        }).mappings().first()

    return dict(row) if row else None


# =========================================================
# RFC MODELO COMERCIAL FINAL
# CLON = CURP + RFC_ONLY, controlado por bot_control.
# IDCIF = QR + RFC_IDCIF, validado por plan semanal grupo02.
# =========================================================

def _rfc_owner_instance() -> str:
    import os
    return (os.getenv("RFC_OWNER_INSTANCE") or "grupo02").strip()


def _rfc_commercial_engine():
    import os
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    load_dotenv("/opt/rfc-grupo02-bot/.env")
    db_url = (os.getenv("DATABASE_URL") or "").strip()

    if not db_url:
        raise RuntimeError("DATABASE_URL_EMPTY")

    return create_engine(db_url, pool_pre_ping=True)


def _rfc_kind_family(kind: str) -> str:
    kind = (kind or "").strip().upper()

    if kind in ("CURP", "RFC_ONLY"):
        return "CLON"

    if kind in ("QR", "RFC_IDCIF"):
        return "IDCIF"

    return "UNKNOWN"


def _rfc_ensure_bot_control(conn, instance_name: str):
    from sqlalchemy import text

    instance_name = (instance_name or "grupo02").strip()

    row = conn.execute(text("""
        SELECT
            instance_name,
            "limit",
            used,
            is_blocked,
            is_active
        FROM bot_control
        WHERE instance_name = :instance_name
        LIMIT 1
    """), {
        "instance_name": instance_name,
    }).mappings().first()

    if row:
        return dict(row)

    conn.execute(text("""
        INSERT INTO bot_control (
            instance_name,
            label,
            panel_token,
            "limit",
            used,
            recharges,
            is_blocked,
            is_active,
            created_at,
            updated_at
        )
        VALUES (
            :instance_name,
            :label,
            NULL,
            0,
            0,
            0,
            FALSE,
            TRUE,
            now(),
            now()
        )
    """), {
        "instance_name": instance_name,
        "label": instance_name,
    })

    return {
        "instance_name": instance_name,
        "limit": 0,
        "used": 0,
        "is_blocked": False,
        "is_active": True,
    }


# =========================================================
# OVERRIDE FINAL RFC GRUPO02
# Modelo correcto:
# - RFC CLON global de grupo02: CURP/RFC_ONLY descuentan clon_balance.
# - IDCIF semanal global de grupo02: QR/RFC_IDCIF validan vigencia y suman idcif_used.
# - NO usar rfc_group_plans.
# =========================================================

def _rfc_plan_family(
    kind: str,
) -> str:
    kind = (
        kind or ""
    ).upper().strip()

    if kind == "RFC_VERIFICABLE":
        return "VERIFICABLE"

    if kind in (
        "CURP",
        "RFC_ONLY",
    ):
        return "CLON"

    if kind in (
        "QR",
        "RFC_IDCIF",
    ):
        return "IDCIF"

    return "UNKNOWN"


def _rfc_global_wallet_owner() -> str:
    import os
    return (os.getenv("RFC_OWNER_INSTANCE") or "grupo02").strip()


def _rfc_global_wallet_engine():
    import os
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    load_dotenv("/opt/rfc-grupo02-bot/.env")

    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL_EMPTY")

    return create_engine(db_url, pool_pre_ping=True)


def _rfc_global_wallet_ensure(conn, owner: str):
    from sqlalchemy import text

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rfc_owner_wallets (
            id SERIAL PRIMARY KEY,
            owner_instance TEXT NOT NULL UNIQUE,
            owner_name TEXT,
            clon_balance INTEGER NOT NULL DEFAULT 0,
            clon_used INTEGER NOT NULL DEFAULT 0,
            clon_price NUMERIC(10,2) NOT NULL DEFAULT 1.50,
            idcif_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            idcif_weekly_price NUMERIC(10,2) NOT NULL DEFAULT 500.00,
            idcif_starts_at TIMESTAMPTZ,
            idcif_expires_at TIMESTAMPTZ,
            idcif_used INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """))

    conn.execute(text("""
        INSERT INTO rfc_owner_wallets (
            owner_instance,
            owner_name,
            clon_balance,
            clon_used,
            clon_price,
            idcif_enabled,
            idcif_weekly_price
        )
        VALUES (
            :owner,
            :owner_name,
            0,
            0,
            1.50,
            FALSE,
            500.00
        )
        ON CONFLICT (owner_instance)
        DO NOTHING
    """), {
        "owner": owner,
        "owner_name": owner.upper(),
    })


# =========================================================
# OVERRIDE DEFINITIVO RFC GRUPO02
# CLON = saldo global rfc_owner_wallets.clon_balance
# IDCIF = plan semanal global rfc_owner_wallets.idcif_expires_at
# NO usar rfc_group_plans ni saldo por bot.
# =========================================================


def _rfc_final_ensure_wallet(conn, owner: str):
    from sqlalchemy import text

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rfc_owner_wallets (
            id SERIAL PRIMARY KEY,
            owner_instance TEXT NOT NULL UNIQUE,
            owner_name TEXT,
            clon_balance INTEGER NOT NULL DEFAULT 0,
            clon_used INTEGER NOT NULL DEFAULT 0,
            clon_price NUMERIC(10,2) NOT NULL DEFAULT 1.50,
            idcif_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            idcif_weekly_price NUMERIC(10,2) NOT NULL DEFAULT 500.00,
            idcif_starts_at TIMESTAMPTZ,
            idcif_expires_at TIMESTAMPTZ,
            idcif_used INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """))

    conn.execute(text("""
        INSERT INTO rfc_owner_wallets (
            owner_instance,
            owner_name,
            clon_balance,
            clon_used,
            clon_price,
            idcif_enabled,
            idcif_weekly_price
        )
        VALUES (
            :owner,
            :owner_name,
            0,
            0,
            1.50,
            FALSE,
            500.00
        )
        ON CONFLICT (owner_instance)
        DO NOTHING
    """), {
        "owner": owner,
        "owner_name": owner.upper(),
    })


# Alias para pisar todas las funciones viejas que el código ya llama.
def _rfc_clear_panel_cache_after_success():
    try:
        import os
        import redis
        from dotenv import load_dotenv

        load_dotenv("/opt/rfc-grupo02-bot/.env")
        redis_url = os.getenv("REDIS_URL", "").strip()

        if not redis_url:
            return

        r = redis.from_url(redis_url, decode_responses=True)

        deleted = 0
        for k in r.scan_iter("panel:*"):
            r.delete(k)
            deleted += 1

        print("[RFC_PANEL_CACHE_CLEARED]", {"deleted": deleted}, flush=True)

    except Exception as e:
        print("[RFC_PANEL_CACHE_CLEAR_ERROR]", repr(e), flush=True)


def _rfc_clear_panel_cache_v2():
    try:
        import os
        import redis
        from dotenv import load_dotenv

        load_dotenv("/opt/rfc-grupo02-bot/.env")
        redis_url = (os.getenv("REDIS_URL") or "").strip()

        if not redis_url:
            print("[RFC_PANEL_CACHE_V2_SKIP] REDIS_URL_EMPTY", flush=True)
            return

        r = redis.from_url(redis_url, decode_responses=True)

        deleted = 0
        for k in r.scan_iter("panel:*"):
            r.delete(k)
            deleted += 1

        print("[RFC_PANEL_CACHE_V2_CLEARED]", {"deleted": deleted}, flush=True)

    except Exception as e:
        print("[RFC_PANEL_CACHE_V2_ERROR]", repr(e), flush=True)


# =========================================================
# OVERRIDE FINAL CON LÍMITES POR BOT:
# - Control por bot separado: CLON e IDCIF.
# - CLON además descuenta saldo global grupo02.
# - IDCIF además valida plan semanal global grupo02.
# =========================================================

def _rfc_final_family(kind: str) -> str:
    k = (kind or "").strip().upper()

    if k in (
        "CURP",
        "RFC",
        "RFC_ONLY",
    ):
        return "CLON"

    if k in (
        "QR",
        "IDCIF",
        "RFC_IDCIF",
    ):
        return "IDCIF"

    if k == "RFC_VERIFICABLE":
        return "VERIFICABLE"

    return "UNKNOWN"


def _rfc_final_owner_instance() -> str:
    import os
    return (os.getenv("RFC_OWNER_INSTANCE") or "grupo02").strip()


def _rfc_final_engine():
    import os
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    load_dotenv("/opt/rfc-grupo02-bot/.env")
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL_EMPTY")

    return create_engine(db_url, pool_pre_ping=True)


def _rfc_final_ensure_wallet_and_bot(conn, owner: str, instance_name: str):
    from sqlalchemy import text

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rfc_owner_wallets (
            id SERIAL PRIMARY KEY,
            owner_instance TEXT NOT NULL UNIQUE,
            owner_name TEXT,
            clon_balance INTEGER NOT NULL DEFAULT 0,
            clon_used INTEGER NOT NULL DEFAULT 0,
            clon_price NUMERIC(10,2) NOT NULL DEFAULT 1.50,
            idcif_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            idcif_weekly_price NUMERIC(10,2) NOT NULL DEFAULT 500.00,
            idcif_starts_at TIMESTAMPTZ,
            idcif_expires_at TIMESTAMPTZ,
            idcif_used INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """))

    conn.execute(text("""
        INSERT INTO rfc_owner_wallets (
            owner_instance, owner_name, clon_balance, clon_used, clon_price,
            idcif_enabled, idcif_weekly_price
        )
        VALUES (:owner, :owner_name, 0, 0, 1.50, FALSE, 500.00)
        ON CONFLICT (owner_instance)
        DO NOTHING
    """), {
        "owner": owner,
        "owner_name": owner.upper(),
    })

    # MIGRACION DESACTIVADA: no correr ALTER TABLE en runtime; bloqueaba bot_control y congelaba panel.
    # conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS clon_limit INTEGER NOT NULL DEFAULT 0'))
    # MIGRACION DESACTIVADA: no correr ALTER TABLE en runtime; bloqueaba bot_control y congelaba panel.
    # conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS clon_used INTEGER NOT NULL DEFAULT 0'))
    # MIGRACION DESACTIVADA: no correr ALTER TABLE en runtime; bloqueaba bot_control y congelaba panel.
    # conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS clon_recharges INTEGER NOT NULL DEFAULT 0'))
    # MIGRACION DESACTIVADA: no correr ALTER TABLE en runtime; bloqueaba bot_control y congelaba panel.
    # conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS idcif_limit INTEGER NOT NULL DEFAULT 0'))
    # MIGRACION DESACTIVADA: no correr ALTER TABLE en runtime; bloqueaba bot_control y congelaba panel.
    # conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS idcif_used INTEGER NOT NULL DEFAULT 0'))
    # MIGRACION DESACTIVADA: no correr ALTER TABLE en runtime; bloqueaba bot_control y congelaba panel.
    # conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS idcif_recharges INTEGER NOT NULL DEFAULT 0'))

    conn.execute(text("""
        INSERT INTO bot_control (
            instance_name, label, panel_token, "limit", used, recharges,
            clon_limit, clon_used, clon_recharges,
            idcif_limit, idcif_used, idcif_recharges,
            is_blocked, is_active, created_at, updated_at
        )
        VALUES (
            :instance_name, :instance_name, NULL, 0, 0, 0,
            0, 0, 0,
            0, 0, 0,
            FALSE, TRUE, now(), now()
        )
        ON CONFLICT (instance_name)
        DO NOTHING
    """), {
        "instance_name": instance_name,
    })


def _rfc_final_check_global(
    job_data: dict,
    group_jid: str,
    group_name: str,
    instance_name: str,
    kind: str,
    count: int = 1,
) -> bool:
    try:
        from datetime import datetime, timezone
        from sqlalchemy import text

        family = _rfc_final_family(kind)
        if family == "UNKNOWN":
            return True

        owner = _rfc_final_owner_instance()
        engine = _rfc_final_engine()
        count = int(count or 1)

        requester_label = job_data.get("requester_label") or job_data.get("requester_name") or ""

        with engine.begin() as conn:
            _rfc_final_ensure_wallet_and_bot(conn, owner, instance_name)

            wallet = conn.execute(text("""
                SELECT clon_balance, idcif_enabled, idcif_expires_at
                FROM rfc_owner_wallets
                WHERE owner_instance = :owner
                LIMIT 1
            """), {"owner": owner}).mappings().first()

            bot = conn.execute(text("""
                SELECT
                    instance_name,
                    is_blocked,
                    is_active,
            
                    COALESCE(clon_limit, 0)
                        AS clon_limit,
            
                    COALESCE(clon_used, 0)
                        AS clon_used,
            
                    COALESCE(idcif_limit, 0)
                        AS idcif_limit,
            
                    COALESCE(idcif_used, 0)
                        AS idcif_used,
            
                    COALESCE(verifiable_enabled, FALSE)
                        AS verifiable_enabled,

                    COALESCE(verifiable_limit, 0)
                        AS verifiable_limit,
            
                    COALESCE(verifiable_used, 0)
                        AS verifiable_used,
            
                    COALESCE(sale_price_verifiable, 0)
                        AS sale_price_verifiable
            
                FROM bot_control
                WHERE instance_name = :instance_name
                LIMIT 1
            """), {
                "instance_name": instance_name,
            }).mappings().first()

            if not wallet or not bot:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} no hay configuración RFC para este bot.",
                    instance_name=instance_name
                )
                return False

            if not bool(bot.get("is_active")):
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} este bot interno no está activo.",
                    instance_name=instance_name
                )
                return False

            if bool(bot.get("is_blocked")):
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} este bot interno está bloqueado.",
                    instance_name=instance_name
                )
                return False

            if family == "CLON":
                global_balance = int(wallet.get("clon_balance") or 0)
                clon_limit = int(bot.get("clon_limit") or 0)
                clon_used = int(bot.get("clon_used") or 0)

                group_promo = conn.execute(text("""
                    SELECT clon_total, clon_used, idcif_total, idcif_used, is_active
                    FROM group_promotions
                    WHERE group_jid = :group_jid
                      AND is_active = TRUE
                    ORDER BY updated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                """), {"group_jid": group_jid}).mappings().first()

                # Bolsa por grupo OPCIONAL.
                # Si NO hay promo activa para el grupo, NO se bloquea:
                # consume del límite general del bot interno.
                # Si SÍ hay promo activa, se respeta el límite CLON de ese grupo.
                if group_promo:
                    group_clon_total = int(group_promo.get("clon_total") or 0)
                    group_clon_used = int(group_promo.get("clon_used") or 0)

                    if group_clon_total > 0 and group_clon_used >= group_clon_total:
                        evolution_send_text_to_group(
                            group_jid,
                            f"⚠️ {requester_label} este grupo ya no tiene RFC CLON disponibles.",
                            instance_name=instance_name
                        )
                        return False
                else:
                    print("[RFC_CLON_GROUP_WITHOUT_PROMO_USE_BOT_LIMIT]", {
                        "group_jid": group_jid,
                        "instance_name": instance_name,
                        "kind": kind,
                    }, flush=True)

                if global_balance <= 0:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} el panel ya no tiene RFC CLON disponibles.",
                        instance_name=instance_name
                    )
                    return False

                # clon_limit = 0 significa ilimitado para ese bot, pero siempre sujeto al saldo global.
                if clon_limit > 0 and clon_used >= clon_limit:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} este bot ya no tiene RFC CLON disponibles.",
                        instance_name=instance_name
                    )
                    return False

                print("[RFC_CLON_BOT_AND_GLOBAL_CHECK_OK]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "global_balance": global_balance,
                    "clon_limit": clon_limit,
                    "clon_used": clon_used,
                    "kind": kind,
                }, flush=True)

                return True

            if family == "IDCIF":
                idcif_limit = int(bot.get("idcif_limit") or 0)
                idcif_used = int(bot.get("idcif_used") or 0)

                group_promo = conn.execute(text("""
                    SELECT clon_total, clon_used, idcif_total, idcif_used, is_active
                    FROM group_promotions
                    WHERE group_jid = :group_jid
                      AND is_active = TRUE
                    ORDER BY updated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                """), {"group_jid": group_jid}).mappings().first()

                # Bolsa por grupo OPCIONAL.
                # Si NO hay promo activa para el grupo, NO se bloquea:
                # consume del límite general del bot interno.
                # Si SÍ hay promo activa, se respeta el límite IDCIF de ese grupo.
                if group_promo:
                    group_idcif_total = int(group_promo.get("idcif_total") or 0)
                    group_idcif_used = int(group_promo.get("idcif_used") or 0)

                    if group_idcif_total > 0 and group_idcif_used >= group_idcif_total:
                        evolution_send_text_to_group(
                            group_jid,
                            f"⚠️ {requester_label} este grupo ya no tiene RFC IDCIF disponibles.",
                            instance_name=instance_name
                        )
                        return False
                else:
                    print("[RFC_IDCIF_GROUP_WITHOUT_PROMO_USE_BOT_LIMIT]", {
                        "group_jid": group_jid,
                        "instance_name": instance_name,
                        "kind": kind,
                    }, flush=True)

                if not bool(wallet.get("idcif_enabled")):
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} el plan semanal RFC IDCIF no está activo.",
                        instance_name=instance_name
                    )
                    return False

                expires_at = wallet.get("idcif_expires_at")
                if not expires_at:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} el plan semanal RFC IDCIF no tiene vigencia activa.",
                        instance_name=instance_name
                    )
                    return False

                now = datetime.now(timezone.utc)
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)

                if expires_at <= now:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} el plan semanal RFC IDCIF venció.",
                        instance_name=instance_name
                    )
                    return False

                # idcif_limit = 0 significa ilimitado para ese bot, pero siempre sujeto al plan semanal global.
                if idcif_limit > 0 and idcif_used >= idcif_limit:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} este bot ya no tiene RFC IDCIF disponibles.",
                        instance_name=instance_name
                    )
                    return False

                print("[RFC_IDCIF_BOT_AND_GLOBAL_CHECK_OK]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "idcif_limit": idcif_limit,
                    "idcif_used": idcif_used,
                    "expires_at": str(expires_at),
                    "kind": kind,
                }, flush=True)

                return True

            if family == "VERIFICABLE":
                if not bool(
                    bot.get("verifiable_enabled")
                ):
                    evolution_send_text_to_group(
                        group_jid,
                        (
                            f"⚠️ {requester_label} "
                            "RFC verificable fue desactivado "
                            "para este bot."
                        ),
                        instance_name=instance_name,
                    )
                    return False

                verifiable_limit = int(
                    bot.get("verifiable_limit")
                    or 0
                )
                
                verifiable_used = int(
                    bot.get("verifiable_used")
                    or 0
                )
                
                if (
                    verifiable_limit > 0
                    and verifiable_used
                    >= verifiable_limit
                ):
                    evolution_send_text_to_group(
                        group_jid,
                        (
                            f"⚠️ {requester_label} "
                            "este bot ya no tiene RFC "
                            "verificables disponibles."
                        ),
                        instance_name=instance_name,
                    )
                
                    return False

                group_promo = conn.execute(
                    text("""
                        SELECT
                            id,
                            COALESCE(
                                verifiable_total,
                                0
                            ) AS verifiable_total,
                
                            COALESCE(
                                verifiable_used,
                                0
                            ) AS verifiable_used,
                
                            COALESCE(
                                shared_group_limit_verifiable,
                                0
                            ) AS shared_limit_verifiable,
                
                            COALESCE(
                                shared_group_used_verifiable,
                                0
                            ) AS shared_used_verifiable,
                
                            COALESCE(
                                shared_key,
                                ''
                            ) AS shared_key,
                
                            is_active
                
                        FROM group_promotions
                
                        WHERE group_jid = :group_jid
                          AND is_active = TRUE
                
                        ORDER BY
                            updated_at DESC NULLS LAST,
                            id DESC
                
                        LIMIT 1
                    """),
                    {
                        "group_jid": group_jid,
                    },
                ).mappings().first()

                if group_promo:
                    group_total = int(
                        group_promo.get(
                            "verifiable_total"
                        )
                        or 0
                    )
                
                    group_used = int(
                        group_promo.get(
                            "verifiable_used"
                        )
                        or 0
                    )

                    if group_total <= 0:
                        evolution_send_text_to_group(
                            group_jid,
                            (
                                f"⚠️ {requester_label} "
                                "este grupo no tiene una bolsa "
                                "de RFC verificables asignada."
                            ),
                            instance_name=instance_name,
                        )
                    
                        print(
                            "[RFC_VERIFICABLE_GROUP_NOT_ASSIGNED]",
                            {
                                "group_jid": group_jid,
                                "verifiable_total":
                                    group_total,
                            },
                            flush=True,
                        )
                    
                        return False
                
                    if group_used >= group_total:
                        evolution_send_text_to_group(
                            group_jid,
                            (
                                f"⚠️ {requester_label} "
                                "este grupo ya no tiene "
                                "RFC verificables disponibles."
                            ),
                            instance_name=instance_name,
                        )
                
                        print(
                            "[RFC_VERIFICABLE_GROUP_LIMIT_REACHED]",
                            {
                                "group_jid": group_jid,
                                "verifiable_total":
                                    group_total,
                                "verifiable_used":
                                    group_used,
                            },
                            flush=True,
                        )
                
                        return False
                
                    shared_limit = int(
                        group_promo.get(
                            "shared_limit_verifiable"
                        )
                        or 0
                    )
                
                    shared_used = int(
                        group_promo.get(
                            "shared_used_verifiable"
                        )
                        or 0
                    )
                
                    if (
                        shared_limit > 0
                        and shared_used >= shared_limit
                    ):
                        evolution_send_text_to_group(
                            group_jid,
                            (
                                f"⚠️ {requester_label} "
                                "este grupo alcanzó su "
                                "límite de RFC verificables "
                                "dentro de la bolsa compartida."
                            ),
                            instance_name=instance_name,
                        )
                
                        print(
                            "[RFC_VERIFICABLE_SHARED_GROUP_LIMIT_REACHED]",
                            {
                                "group_jid": group_jid,
                                "shared_limit":
                                    shared_limit,
                                "shared_used":
                                    shared_used,
                            },
                            flush=True,
                        )
                
                        return False

                print(
                    "[RFC_VERIFICABLE_BOT_AND_GROUP_CHECK_OK]",
                    {
                        "instance_name": instance_name,
                        "group_jid": group_jid,
                        "kind": kind,
                        "verifiable_limit": (
                            verifiable_limit
                        ),
                        "verifiable_used": (
                            verifiable_used
                        ),
                        "verifiable_available": (
                            max(
                                verifiable_limit
                                - verifiable_used,
                                0,
                            )
                            if verifiable_limit > 0
                            else None
                        ),
                        "sale_price_verifiable": str(
                            bot.get(
                                "sale_price_verifiable"
                            )
                            or 0
                        ),
                        "group_promo_found": bool(
                            group_promo
                        ),
                        "group_verifiable_total": (
                            int(
                                group_promo.get(
                                    "verifiable_total"
                                )
                                or 0
                            )
                            if group_promo
                            else None
                        ),
                        "group_verifiable_used": (
                            int(
                                group_promo.get(
                                    "verifiable_used"
                                )
                                or 0
                            )
                            if group_promo
                            else None
                        ),
                    },
                    flush=True,
                )

                return True

        return True

    except Exception as e:
        print("[RFC_FINAL_BOT_LIMIT_CHECK_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
        }, flush=True)
        try:
            evolution_send_text_to_group(
                group_jid,
                "⚠️ Error validando saldo/límite RFC. Intenta de nuevo.",
                instance_name=instance_name
            )
        except Exception:
            pass
        return False


def _rfc_final_after_success_global(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1, accounting_key: str = "", item_key: str = ""):
    try:
        from sqlalchemy import text

        family = _rfc_final_family(kind)
        if family == "UNKNOWN":
            return

        owner = _rfc_final_owner_instance()
        engine = _rfc_final_engine()
        count = int(count or 1)

        with engine.begin() as conn:
            _rfc_final_ensure_wallet_and_bot(conn, owner, instance_name)

            if not accounting_key:
                raise RuntimeError(
                    "RFC_ACCOUNTING_KEY_EMPTY"
                )
            
            accounting_result = conn.execute(
                text("""
                    INSERT INTO rfc_delivery_accounting (
                        accounting_key,
                        instance_name,
                        group_jid,
                        kind,
                        item_key,
                        count,
                        created_at
                    )
                    VALUES (
                        :accounting_key,
                        :instance_name,
                        :group_jid,
                        :kind,
                        :item_key,
                        :count,
                        now()
                    )
                    ON CONFLICT (
                        accounting_key
                    )
                    DO NOTHING
                """),
                {
                    "accounting_key":
                        accounting_key,
                    "instance_name":
                        instance_name,
                    "group_jid":
                        group_jid,
                    "kind":
                        kind,
                    "item_key":
                        item_key,
                    "count":
                        count,
                },
            )
            
            if accounting_result.rowcount == 0:
                print(
                    "[RFC_COMMERCIAL_DUPLICATE_IGNORED]",
                    {
                        "accounting_key":
                            accounting_key,
                        "group_jid":
                            group_jid,
                        "kind":
                            kind,
                        "item_key":
                            item_key,
                    },
                    flush=True,
                )
            
                return False

            if family == "CLON":
                conn.execute(text("""
                    UPDATE rfc_owner_wallets
                    SET
                        clon_balance = GREATEST(clon_balance - :count, 0),
                        clon_used = clon_used + :count,
                        updated_at = now()
                    WHERE owner_instance = :owner
                """), {"count": count, "owner": owner})

                conn.execute(text("""
                    UPDATE bot_control
                    SET
                        clon_used = clon_used + :count,
                        used = used + :count,
                        updated_at = now()
                    WHERE instance_name = :instance_name
                """), {"count": count, "instance_name": instance_name})

                # ✅ Descontar bolsa del grupo para que panel group detail y mini panel reflejen CLON usado
                conn.execute(text("""
                    UPDATE group_promotions
                    SET
                        clon_used = LEAST(COALESCE(clon_used, 0) + :count, COALESCE(clon_total, 0)),
                        used_actas = COALESCE(used_actas, 0) + :count,
                        updated_at = now()
                    WHERE group_jid = :group_jid
                      AND is_active = TRUE
                """), {"count": count, "group_jid": group_jid})

                print("[RFC_CLON_GROUP_PROMO_DEDUCTED]", {
                    "group_jid": group_jid,
                    "kind": kind,
                    "count": count,
                }, flush=True)

                print("[RFC_CLON_GLOBAL_AND_BOT_DEDUCTED]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "count": count,
                }, flush=True)

            elif family == "IDCIF":
                conn.execute(text("""
                    UPDATE rfc_owner_wallets
                    SET
                        idcif_used = idcif_used + :count,
                        updated_at = now()
                    WHERE owner_instance = :owner
                """), {"count": count, "owner": owner})

                conn.execute(text("""
                    UPDATE bot_control
                    SET
                        idcif_used = idcif_used + :count,
                        used = used + :count,
                        updated_at = now()
                    WHERE instance_name = :instance_name
                """), {"count": count, "instance_name": instance_name})

                # ✅ Descontar bolsa del grupo para que panel group detail y mini panel reflejen IDCIF usado
                conn.execute(text("""
                    UPDATE group_promotions
                    SET
                        idcif_used = LEAST(COALESCE(idcif_used, 0) + :count, COALESCE(idcif_total, 0)),
                        used_actas = COALESCE(used_actas, 0) + :count,
                        updated_at = now()
                    WHERE group_jid = :group_jid
                      AND is_active = TRUE
                """), {"count": count, "group_jid": group_jid})

                print("[RFC_IDCIF_GROUP_PROMO_USED_INC]", {
                    "group_jid": group_jid,
                    "kind": kind,
                    "count": count,
                }, flush=True)

                print("[RFC_IDCIF_GLOBAL_AND_BOT_USED_INC]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "count": count,
                }, flush=True)

            elif family == "VERIFICABLE":
                promo = conn.execute(
                    text("""
                        SELECT
                            id,
                            group_jid,
                            COALESCE(
                                shared_key,
                                ''
                            ) AS shared_key,
                
                            COALESCE(
                                verifiable_total,
                                0
                            ) AS verifiable_total,
                
                            COALESCE(
                                verifiable_used,
                                0
                            ) AS verifiable_used,
                
                            COALESCE(
                                shared_group_limit_verifiable,
                                0
                            ) AS shared_limit_verifiable,
                
                            COALESCE(
                                shared_group_used_verifiable,
                                0
                            ) AS shared_used_verifiable
                
                        FROM group_promotions
                
                        WHERE group_jid = :group_jid
                          AND is_active = TRUE
                
                        ORDER BY
                            updated_at DESC NULLS LAST,
                            id DESC
                
                        LIMIT 1
                
                        FOR UPDATE
                    """),
                    {
                        "group_jid": group_jid,
                    },
                ).mappings().first()

                promo_requires_consumption = bool(
                    promo
                    and int(
                        promo.get(
                            "verifiable_total"
                        )
                        or 0
                    ) > 0
                )

                shared_key = (
                    str(
                        promo.get("shared_key")
                        or ""
                    ).strip()
                    if promo
                    else ""
                )

                bot_result = conn.execute(
                    text("""
                        UPDATE bot_control
                
                        SET
                            verifiable_used =
                                COALESCE(
                                    verifiable_used,
                                    0
                                ) + :count,
                
                            used =
                                COALESCE(
                                    used,
                                    0
                                ) + :count,
                
                            updated_at = now()
                
                        WHERE instance_name =
                            :instance_name
                    """),
                    {
                        "count": count,
                        "instance_name":
                            instance_name,
                    },
                )
                
                if bot_result.rowcount != 1:
                    raise RuntimeError(
                        "RFC_VERIFICABLE_BOT_CONSUME_FAILED"
                    )

                if not shared_key:
                    promo_result = conn.execute(
                        text("""
                            UPDATE group_promotions
                
                            SET
                                verifiable_used =
                                    COALESCE(
                                        verifiable_used,
                                        0
                                    ) + :count,
                
                                used_actas =
                                    COALESCE(
                                        clon_used,
                                        0
                                    )
                                    + COALESCE(
                                        idcif_used,
                                        0
                                    )
                                    + COALESCE(
                                        verifiable_used,
                                        0
                                    )
                                    + :count,
                
                                updated_at = now()
                
                            WHERE group_jid = :group_jid
                              AND is_active = TRUE
                
                              AND COALESCE(
                                    verifiable_used,
                                    0
                                  ) + :count
                                  <= COALESCE(
                                    verifiable_total,
                                    0
                                  )
                        """),
                        {
                            "count": count,
                            "group_jid": group_jid,
                        },
                    )
                
                    if (
                        promo_requires_consumption
                        and promo_result.rowcount != 1
                    ):
                        raise RuntimeError(
                            "RFC_VERIFICABLE_GROUP_PROMO_CONSUME_FAILED"
                        )

                else:
                    shared_rows = conn.execute(
                        text("""
                            SELECT
                                id,
                                group_jid,
                                COALESCE(
                                    verifiable_total,
                                    0
                                ) AS verifiable_total,
                
                                COALESCE(
                                    verifiable_used,
                                    0
                                ) AS verifiable_used
                
                            FROM group_promotions
                
                            WHERE shared_key = :shared_key
                              AND is_active = TRUE
                
                            ORDER BY id
                
                            FOR UPDATE
                        """),
                        {
                            "shared_key": shared_key,
                        },
                    ).mappings().all()
                
                    if not shared_rows:
                        raise RuntimeError(
                            "RFC_VERIFICABLE_SHARED_PROMO_NOT_FOUND"
                        )

                    shared_totals = {
                        int(
                            row.get(
                                "verifiable_total"
                            )
                            or 0
                        )
                        for row in shared_rows
                    }
                    
                    shared_used_values = {
                        int(
                            row.get(
                                "verifiable_used"
                            )
                            or 0
                        )
                        for row in shared_rows
                    }
                    
                    if len(shared_totals) != 1:
                        raise RuntimeError(
                            "RFC_VERIFICABLE_SHARED_TOTAL_MISMATCH"
                        )
                    
                    if len(shared_used_values) != 1:
                        raise RuntimeError(
                            "RFC_VERIFICABLE_SHARED_USED_MISMATCH"
                        )
                
                    shared_total = int(
                        shared_rows[0].get(
                            "verifiable_total"
                        )
                        or 0
                    )
                
                    shared_used = int(
                        shared_rows[0].get(
                            "verifiable_used"
                        )
                        or 0
                    )
                
                    if shared_total <= 0:
                        raise RuntimeError(
                            "RFC_VERIFICABLE_SHARED_PROMO_NOT_ASSIGNED"
                        )
                
                    if (
                        shared_used + count
                        > shared_total
                    ):
                        raise RuntimeError(
                            "RFC_VERIFICABLE_SHARED_PROMO_EXHAUSTED"
                        )
                
                    shared_result = conn.execute(
                        text("""
                            UPDATE group_promotions
                
                            SET
                                verifiable_used =
                                    COALESCE(
                                        verifiable_used,
                                        0
                                    ) + :count,
                
                                used_actas =
                                    COALESCE(
                                        clon_used,
                                        0
                                    )
                                    + COALESCE(
                                        idcif_used,
                                        0
                                    )
                                    + COALESCE(
                                        verifiable_used,
                                        0
                                    )
                                    + :count,
                
                                updated_at = now()
                
                            WHERE shared_key = :shared_key
                              AND is_active = TRUE
                        """),
                        {
                            "count": count,
                            "shared_key": shared_key,
                        },
                    )
                
                    if (
                        shared_result.rowcount
                        != len(shared_rows)
                    ):
                        raise RuntimeError(
                            "RFC_VERIFICABLE_SHARED_SYNC_FAILED"
                        )
                
                    group_result = conn.execute(
                        text("""
                            UPDATE group_promotions
                
                            SET
                                shared_group_used_verifiable =
                                    COALESCE(
                                        shared_group_used_verifiable,
                                        0
                                    ) + :count,
                
                                updated_at = now()
                
                            WHERE group_jid = :group_jid
                              AND shared_key = :shared_key
                              AND is_active = TRUE
                
                              AND (
                                  COALESCE(
                                      shared_group_limit_verifiable,
                                      0
                                  ) = 0
                
                                  OR COALESCE(
                                      shared_group_used_verifiable,
                                      0
                                  ) + :count
                                  <= COALESCE(
                                      shared_group_limit_verifiable,
                                      0
                                  )
                              )
                        """),
                        {
                            "count": count,
                            "group_jid": group_jid,
                            "shared_key": shared_key,
                        },
                    )
                
                    if group_result.rowcount != 1:
                        raise RuntimeError(
                            "RFC_VERIFICABLE_SHARED_GROUP_CONSUME_FAILED"
                        )

                print(
                    "[RFC_VERIFICABLE_GROUP_PROMO_USED_INC]",
                    {
                        "group_jid": group_jid,
                        "shared_key": shared_key or None,
                        "kind": kind,
                        "count": count,
                    },
                    flush=True,
                )
            
                print(
                    "[RFC_VERIFICABLE_USED_INC]",
                    {
                        "instance_name": instance_name,
                        "group_jid": group_jid,
                        "kind": kind,
                        "count": count,
                        "price": float(
                            job_data.get(
                                "verifiable_price"
                            )
                            or 0
                        ),
                    },
                    flush=True,
                )

            try:
                _rfc_clear_panel_cache_v2()
            except Exception:
                pass

            return True

    except Exception as e:
        print(
            "[RFC_FINAL_BOT_LIMIT_AFTER_ERROR]",
            repr(e),
            {
                "kind": kind,
                "group_jid": group_jid,
                "instance_name":
                    instance_name,
                "count": count,
            },
            flush=True,
        )
    
        raise


# Aliases finales para pisar cualquier lógica anterior.
def _rfc_plan_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    return _rfc_final_check_global(job_data, group_jid, group_name, instance_name, kind)


def _rfc_commercial_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    return _rfc_final_check_global(job_data, group_jid, group_name, instance_name, kind)


def _rfc_plan_deduct_success(
    job_data: dict,
    group_jid: str,
    group_name: str,
    instance_name: str,
    kind: str,
    count: int = 1,
    accounting_key: str = "",
    item_key: str = "",
):
    return _rfc_final_after_success_global(
        job_data=job_data,
        group_jid=group_jid,
        group_name=group_name,
        instance_name=instance_name,
        kind=kind,
        count=count,
        accounting_key=accounting_key,
        item_key=item_key,
    )
    

def _rfc_commercial_after_success(
    job_data: dict,
    group_jid: str,
    group_name: str,
    instance_name: str,
    kind: str,
    count: int = 1,
    accounting_key: str = "",
    item_key: str = "",
):
    return _rfc_final_after_success_global(
        job_data=job_data,
        group_jid=group_jid,
        group_name=group_name,
        instance_name=instance_name,
        kind=kind,
        count=count,
        accounting_key=accounting_key,
        item_key=item_key,
    )
