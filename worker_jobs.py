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
from rq import get_current_job

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

from app.models import AuthorizedGroup, BotControl
from app.curp_validation import analyze_curp

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

# ============================================================
# MENSAJES CLIENTE RFC - WORKER
# ============================================================

def _job_batch_position(job_data: dict) -> str:
    return ""


def _job_request_identity(job_data: dict) -> dict:
    if bool(job_data.get("is_verifiable")):
        return {
            "query_type": "RFC_VERIFICABLE",
            "identifier": str(
                job_data.get("verifiable_original_identifier")
                or ""
            ).strip().upper(),
            "provider_rfc": str(
                job_data.get("provider_rfc")
                or ""
            ).strip().upper(),
            "provider_idcif": str(
                job_data.get("provider_idcif")
                or ""
            ).strip(),
        }

    query_type = str(
        job_data.get("query_type")
        or ""
    ).strip().upper()

    src = str(
        job_data.get("query")
        or job_data.get("original_text")
        or ""
    ).strip().upper()

    curp_match = CURP_RE.search(src)
    rfc_match = RFC_RE.search(src)
    idcif_match = IDCIF_RE.search(src)

    identifier = ""

    if query_type == "CURP" and curp_match:
        identifier = curp_match.group(0).upper()

    elif query_type == "RFC_ONLY" and rfc_match:
        identifier = rfc_match.group(0).upper()

    return {
        "query_type": query_type,
        "identifier": identifier,
        "rfc": rfc_match.group(0).upper() if rfc_match else "",
        "idcif": idcif_match.group(0) if idcif_match else "",
        "provider_rfc": "",
        "provider_idcif": "",
    }


def _normalize_job_bot_label(value: str) -> str:
    """
    Quita decoracion Unicode solamente de los extremos del nombre
    para evitar encabezados como "🚀 🚀 DOCU EXPRES ⚡".

    No modifica BotControl ni la base de datos.
    """
    import unicodedata

    text = re.sub(
        r"\s+",
        " ",
        str(value or "").strip(),
    )

    if not text:
        return "RFC"

    def decorative(char: str) -> bool:
        category = unicodedata.category(char)
        return (
            char.isspace()
            or category.startswith("S")
            or category in {"Mn", "Me", "Cf"}
        )

    start = 0
    end = len(text)

    while start < end and decorative(text[start]):
        start += 1

    while end > start and decorative(text[end - 1]):
        end -= 1

    cleaned = text[start:end].strip()

    return cleaned or "RFC"


def _job_bot_label(job_data: dict) -> str:
    explicit = str(
        job_data.get("bot_label") or ""
    ).strip()

    if explicit:
        return _normalize_job_bot_label(
            explicit
        )

    instance_name = str(
        job_data.get("evolution_instance")
        or job_data.get("instance_name")
        or EVOLUTION_INSTANCE
        or "RFC"
    ).strip()

    try:
        db = SessionLocal()

        try:
            row = (
                db.query(BotControl)
                .filter(
                    BotControl.instance_name
                    == instance_name
                )
                .first()
            )

            if row and str(row.label or "").strip():
                return _normalize_job_bot_label(
                    str(row.label)
                )

        finally:
            db.close()

    except Exception as exc:
        print(
            "RFC_WORKER_BOT_LABEL_FALLBACK =",
            {
                "instance": instance_name,
                "error": repr(exc),
            },
            flush=True,
        )

    return _normalize_job_bot_label(
        instance_name or "RFC"
    )


def _job_type_label(job_data: dict, type_override: str = "") -> str:
    override = str(type_override or "").strip()

    if override:
        return override

    if bool(job_data.get("is_verifiable")):
        return "RFC VERIFICABLE"

    query_type = str(
        job_data.get("query_type")
        or ""
    ).strip().upper()

    mapping = {
        "CURP": "CURP",
        "RFC_ONLY": "RFC",
        "RFC_IDCIF": "RFC IDCIF",
        "QR": "QR",
        "QR_TEXT": "QR",
        "IMAGE": "QR",
        "DOCUMENT": "QR",
        "RFC_VERIFICABLE": "RFC VERIFICABLE",
    }

    return mapping.get(query_type, "SOLICITUD")


def _job_data_value(job_data: dict, data_override: str = "") -> str:
    override = str(data_override or "").strip().upper()

    if override:
        return override

    identity = _job_request_identity(job_data)
    query_type = str(identity.get("query_type") or "").strip().upper()

    if query_type in {"RFC_VERIFICABLE", "CURP"}:
        return str(identity.get("identifier") or "").strip().upper() or "N/D"

    if query_type in {"RFC_ONLY", "RFC_IDCIF"}:
        return str(
            identity.get("rfc")
            or identity.get("identifier")
            or ""
        ).strip().upper() or "N/D"

    if query_type in {"QR", "QR_TEXT", "IMAGE", "DOCUMENT"}:
        return "QR SAT"

    return "N/D"


def _job_status_meta(title: str, body: str = "") -> tuple[str, str]:
    source = f"{title or ''}\n{body or ''}".upper()

    if "YA ENTREGADA" in source or "YA FUE ENTREGADA" in source:
        return "¡Estatus de solicitud!", "YA ENTREGADA"

    if "EN PROCESO" in source:
        return "¡Estatus de solicitud!", "EN PROCESO"

    if "SIN RESPUESTA" in source:
        return "¡Estatus de solicitud!", "SIN RESPUESTA"

    if "SIN ID" in source:
        return "¡Resultado de Busqueda!", "AVISO"

    if (
        "CONSTANCIA NO DISPONIBLE" in source
        or "PROBLEMA AL ADJUNTAR" in source
        or "PROBLEMA AL ENTREGAR" in source
        or "CORRECCIÓN" in source
        or "CORRECCION" in source
    ):
        return "¡Resultado de Busqueda!", "AVISO"

    service_markers = (
        "SERVICIO",
        "RFC CLON NO DISPONIBLE",
        "RFC IDCIF NO DISPONIBLE",
        "RFC VERIFICABLE NO DISPONIBLE",
        "SALDO/LÍMITE",
        "SALDO/LIMITE",
    )

    if any(marker in source for marker in service_markers):
        if "BLOQUE" in source:
            status = "BLOQUEADO"
        elif "DESACTIV" in source or "INACTIV" in source:
            status = "INACTIVO"
        elif "LÍMITE" in source or "LIMITE" in source:
            status = "LÍMITE ALCANZADO"
        elif "NO CONFIGUR" in source:
            status = "NO CONFIGURADO"
        elif "VIGENCIA" in source or "VIGENTE" in source:
            status = "NO VIGENTE"
        elif (
            "NO HAY" in source
            or "NO TIENE" in source
            or "NO DISPONIB" in source
        ):
            status = "NO DISPONIBLE"
        else:
            status = "ERROR"

        return "¡Estatus del servicio!", status

    return "¡Resultado de Busqueda!", "ERROR"


def _job_identity_lines(
    job_data: dict,
    *,
    result_mode: bool = False,
    result_rfc: str = "",
    result_idcif: str = "",
) -> str:
    return _job_data_value(job_data)


def _job_client_message(
    job_data: dict,
    *,
    title: str,
    requester_label: str = "",
    body: str = "",
    include_identity: bool = True,
    result_mode: bool = False,
    result_rfc: str = "",
    result_idcif: str = "",
    family: str = "",
    status: str = "",
    type_override: str = "",
    data_override: str = "",
) -> str:
    if family:
        heading = {
            "request": "¡Estatus de solicitud!",
            "result": "¡Resultado de Busqueda!",
            "service": "¡Estatus del servicio!",
        }.get(
            str(family or "").strip().lower(),
            "¡Resultado de Busqueda!",
        )
        final_status = str(status or "").strip().upper() or "AVISO"
    else:
        heading, inferred_status = _job_status_meta(title, body)
        final_status = str(status or "").strip().upper() or inferred_status

    lines = [
        f"🚀 {_job_bot_label(job_data)} ⚡",
        heading,
        f"_Tipo_: *{_job_type_label(job_data, type_override)}*",
        f"_Dato_: *{_job_data_value(job_data, data_override)}*",
        f"_Estatus_: *{final_status}*",
    ]

    if result_mode:
        result_lines = []
        result_rfc = str(result_rfc or "").strip().upper()
        result_idcif = str(result_idcif or "").strip()

        if result_rfc:
            result_lines.append(f"_RFC localizado_: *{result_rfc}*")

        if result_idcif:
            result_lines.append(f"_IDCIF_: *{result_idcif}*")

        if result_lines:
            lines += ["", "\n".join(result_lines)]

    if body:
        lines += ["", str(body).strip()]

    return "\n".join(lines)

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

def _group_service_enabled_for_job(
    *,
    group_jid: str,
    kind: str,
) -> bool:
    group_jid = (
        group_jid or ""
    ).strip()

    kind = (
        kind or ""
    ).strip().upper()

    if not group_jid:
        return False

    db = SessionLocal()

    try:
        row = (
            db.query(AuthorizedGroup)
            .filter(
                AuthorizedGroup.group_jid
                == group_jid
            )
            .first()
        )

        if not row:
            return False

        if kind in {
            "CURP",
            "RFC_ONLY",
        }:
            return bool(
                getattr(
                    row,
                    "clon_enabled",
                    True,
                )
            )

        if kind in {
            "QR",
            "RFC_IDCIF",
        }:
            return bool(
                getattr(
                    row,
                    "idcif_enabled",
                    True,
                )
            )

        if kind == "RFC_VERIFICABLE":
            return bool(
                getattr(
                    row,
                    "verifiable_enabled",
                    False,
                )
            )

        return False

    finally:
        db.close()

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

# RFC_TERMINAL_REJECTION_FINALIZE_V1
def _finalize_verifiable_terminal_rejection(
    job_data: dict,
) -> bool:
    """
    Cierra únicamente una solicitud VERIFICABLE
    que ya recibió una respuesta terminal y para
    la cual SAT rechazó la generación.

    No contabiliza.
    No marca completed_24h.
    No vuelve a enviar.
    No cambia proveedor.
    """

    if not bool(
        job_data.get("is_verifiable")
    ):
        return False

    request_key = str(
        job_data.get(
            "verifiable_request_key"
        )
        or ""
    ).strip()

    if not request_key:
        print(
            "[RFC VERIFIABLE TERMINAL "
            "REJECTION FINALIZE SKIP]",
            {
                "reason":
                    "request_key_empty",
            },
            flush=True,
        )

        return False

    inflight_key = str(
        job_data.get("inflight_key")
        or ""
    ).strip()

    processing_key = str(
        job_data.get(
            "verifiable_processing_key"
        )
        or ""
    ).strip()

    provider_message_id = str(
        job_data.get(
            "provider_request_msg_id"
        )
        or ""
    ).strip()

    cleanup_errors = []

    redis_keys = [
        key
        for key in (
            inflight_key,
            processing_key,
        )
        if key
    ]

    if redis_keys:
        try:
            redis_stats.delete(
                *redis_keys
            )
        except Exception as exc:
            cleanup_errors.append(
                "redis_keys:"
                + repr(exc)
            )

    pending_finished = False

    try:
        finish_pending(
            request_key,
            provider_message_id=(
                provider_message_id
            ),
        )

        pending_finished = True

    except Exception as exc:
        cleanup_errors.append(
            "finish_pending:"
            + repr(exc)
        )

    # Sólo liberar el result claim cuando
    # el pending sí pudo eliminarse.
    # Así no abrimos una carrera si Redis
    # falla en medio de la finalización.
    if pending_finished:
        try:
            release_provider_result_claim(
                request_key
            )
        except Exception as exc:
            cleanup_errors.append(
                "release_result_claim:"
                + repr(exc)
            )

    if cleanup_errors:
        print(
            "[RFC VERIFIABLE TERMINAL "
            "REJECTION FINALIZE WARN]",
            {
                "request_key":
                    request_key,
                "errors":
                    cleanup_errors,
            },
            flush=True,
        )

        return False

    print(
        "[RFC VERIFIABLE TERMINAL "
        "REJECTION FINALIZED]",
        {
            "request_key":
                request_key,
            "inflight_key":
                inflight_key,
            "processing_key":
                processing_key,
            "provider_message_id":
                provider_message_id,
        },
        flush=True,
    )

    return True

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
        f"Motivo: {reason_text}."
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

    if r.status_code >= 400:
        _body = r.text or ""
        _up = _body.upper()

        if (
            "ASPOSE_PDF_CONVERT_FAIL" in _up
            and (
                "TOKEN ERROR 429" in _up
                or "TOKEN ERROR 503" in _up
                or "503 SERVICE UNAVAILABLE" in _up
            )
        ):
            print(
                "[ASPOSE_VERIFICABLE_TEXT_TRIGGER]",
                {
                    "status": r.status_code,
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                },
                flush=True,
            )

            return {
                "ok": False,
                "error": "ASPOSE_TEMP_UNAVAILABLE",
                "aspose_text_fallback": True,
                "fallback_query": query,
            }

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
    *,
    is_verifiable: bool = False,
    provider_rfc: str = "",
    provider_idcif: str = "",
):
    headers = {
        "Authorization": (
            f"Bearer {BOT_INTERNAL_TOKEN}"
        ),
        "Content-Type": "application/json",
    }

    payload = {
        "requester_number": requester_number,
        "requester_name": requester_name,
        "group_jid": group_jid,
        "original_text": original_text,
        "mime_type": mime_type,
        "media_b64": base64.b64encode(
            media_bytes
        ).decode("utf-8"),
        "evolution_instance": instance_name,

        # RFC verificable
        "is_verifiable": bool(
            is_verifiable
        ),
        "provider_rfc": (
            provider_rfc or ""
        ).strip().upper(),
        "provider_idcif": (
            provider_idcif or ""
        ).strip(),
    }

    url = (
        f"{BOT_INTERNAL_URL.rstrip('/')}"
        "/internal/generate-pdf-from-media"
    )

    r = requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=420,
    )

    print(
        "worker call_bot_internal_media instance:",
        instance_name,
        flush=True,
    )

    print(
        "worker call_bot_internal_media status:",
        r.status_code,
        flush=True,
    )

    print(
        "worker call_bot_internal_media resp:",
        r.text,
        flush=True,
    )

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



def _delivery_accounting_payload(
    job_data: dict,
    group_jid: str,
    group_name: str,
    kind: str,
    count: int,
    item_key: str,
) -> dict:
    return {
        "status": "DELIVERED_PENDING_ACCOUNTING",
        "job_data": dict(job_data or {}),
        "group_jid": group_jid,
        "group_name": group_name,
        "kind": kind,
        "count": int(count or 1),
        "item_key": item_key or "",
        "delivered_at": _panel_now().isoformat(),
    }


def _reconcile_delivery_accounting(
    done_key: str,
    current_job_data: dict | None = None,
) -> bool:
    raw = redis_stats.get(done_key)
    if not raw:
        return False

    try:
        payload = json.loads(raw)
    except Exception:
        # Compatibilidad con done_key antiguos que guardaban sólo fecha.
        return False

    if not isinstance(payload, dict):
        return False

    if payload.get("status") == "ACCOUNTED":
        return True

    if payload.get("status") != "DELIVERED_PENDING_ACCOUNTING":
        return False

    stored_job_data = payload.get("job_data") or {}
    if not isinstance(stored_job_data, dict):
        stored_job_data = {}

    merged_job_data = {
        **stored_job_data,
        **(current_job_data or {}),
    }

    try:
        record_success_once(
            job_data=merged_job_data,
            group_jid=str(payload.get("group_jid") or merged_job_data.get("group_jid") or ""),
            group_name=str(payload.get("group_name") or payload.get("group_jid") or ""),
            kind=str(payload.get("kind") or ""),
            count=int(payload.get("count") or 1),
            item_key=str(payload.get("item_key") or ""),
        )

        payload["status"] = "ACCOUNTED"
        payload["accounted_at"] = _panel_now().isoformat()
        payload["job_data"] = merged_job_data
        redis_stats.set(
            done_key,
            json.dumps(payload, ensure_ascii=False, default=str),
            ex=DELIVERY_DONE_TTL_SEC,
        )

        print("[RFC DELIVERY ACCOUNTING RECONCILED]", {
            "done_key": done_key,
            "kind": payload.get("kind"),
            "item_key": payload.get("item_key"),
        }, flush=True)
        return True

    except Exception as exc:
        print("[RFC DELIVERY ACCOUNTING RECONCILE ERROR]", repr(exc), {
            "done_key": done_key,
        }, flush=True)
        raise


def claim_delivery_once(
    job_data: dict,
    item_key: str = "",
    *,
    repair_verifiable_after_done: bool = True,
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

    # Ya fue entregado recientemente. Antes de impedir el reenvío, repara
    # cualquier contabilización que haya quedado pendiente tras un crash/fallo DB.
    if redis_stats.exists(done_key):
        print(
            "[RFC DELIVERY ALREADY DONE]",
            done_key,
            flush=True,
        )

        _reconcile_delivery_accounting(
            done_key,
            current_job_data=job_data,
        )

        if repair_verifiable_after_done:
            _repair_verifiable_finalization_after_delivery(
                job_data
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
        redis_stats.delete(
            lock_key
        )

        _reconcile_delivery_accounting(
            done_key,
            current_job_data=job_data,
        )

        if repair_verifiable_after_done:
            _repair_verifiable_finalization_after_delivery(
                job_data
            )

        return False, lock_key, done_key

    return True, lock_key, done_key


def mark_delivery_done(
    lock_key: str,
    done_key: str,
    *,
    accounting_payload: dict | None = None,
):
    pipe = redis_stats.pipeline()

    value = (
        json.dumps(
            accounting_payload,
            ensure_ascii=False,
            default=str,
        )
        if accounting_payload
        else _panel_now().isoformat()
    )

    pipe.set(
        done_key,
        value,
        ex=DELIVERY_DONE_TTL_SEC,
    )

    pipe.delete(lock_key)
    pipe.execute()


def mark_delivery_pending_accounting(
    lock_key: str,
    done_key: str,
    *,
    job_data: dict,
    group_jid: str,
    group_name: str,
    kind: str,
    count: int,
    item_key: str,
):
    payload = _delivery_accounting_payload(
        job_data=job_data,
        group_jid=group_jid,
        group_name=group_name,
        kind=kind,
        count=count,
        item_key=item_key,
    )
    mark_delivery_done(
        lock_key,
        done_key,
        accounting_payload=payload,
    )


def mark_delivery_accounted(done_key: str):
    raw = redis_stats.get(done_key)
    if not raw:
        return

    try:
        payload = json.loads(raw)
    except Exception:
        return

    if not isinstance(payload, dict):
        return

    payload["status"] = "ACCOUNTED"
    payload["accounted_at"] = _panel_now().isoformat()
    redis_stats.set(
        done_key,
        json.dumps(payload, ensure_ascii=False, default=str),
        ex=DELIVERY_DONE_TTL_SEC,
    )


def release_delivery_claim(lock_key: str):
    if lock_key:
        redis_stats.delete(lock_key)


def mark_verifiable_completed_24h(
    job_data: dict,
) -> bool:
    if not bool(
        job_data.get("is_verifiable")
    ):
        return False

    processing_key = (
        job_data.get(
            "verifiable_processing_key"
        )
        or ""
    ).strip()

    completed_key = (
        job_data.get(
            "verifiable_completed_key"
        )
        or ""
    ).strip()

    if not completed_key:
        print(
            "[RFC VERIFIABLE COMPLETED KEY EMPTY]",
            {
                "request_key": (
                    job_data.get(
                        "verifiable_request_key"
                    )
                ),
            },
            flush=True,
        )

        return False

    pipe = redis_stats.pipeline()

    pipe.set(
        completed_key,
        _panel_now().isoformat(),
        ex=24 * 60 * 60,
    )

    if processing_key:
        pipe.delete(
            processing_key
        )

    pipe.execute()

    print(
        "[RFC VERIFIABLE COMPLETED 24H MARKED]",
        {
            "request_key": (
                job_data.get(
                    "verifiable_request_key"
                )
            ),
            "completed_key": completed_key,
            "processing_key": processing_key,
            "ttl_seconds": 86400,
        },
        flush=True,
    )

    return True


def _repair_verifiable_finalization_after_delivery(
    job_data: dict,
) -> None:
    """
    Si el documento ya fue entregado pero un intento
    anterior falló después de marcar delivery_done,
    repara el estado administrativo sin volver a
    enviar ni volver a contabilizar.
    """

    if not bool(
        job_data.get(
            "is_verifiable"
        )
    ):
        return

    request_key = str(
        job_data.get(
            "verifiable_request_key"
        )
        or ""
    ).strip()

    if not request_key:
        return

    completion_marked = (
        mark_verifiable_completed_24h(
            job_data
        )
    )

    if not completion_marked:
        raise RuntimeError(
            "RFC_VERIFIABLE_"
            "COMPLETED_24H_REPAIR_FAILED"
        )

    finish_pending(
        request_key,
        provider_message_id=(
            job_data.get(
                "provider_request_msg_id"
            )
            or ""
        ),
    )

    print(
        "[RFC VERIFIABLE FINALIZATION REPAIRED]",
        {
            "request_key":
                request_key,
        },
        flush=True,
    )


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


def process_verifiable_provider_reminder_job(
    request_key: str,
    reminder_minutes: int,
):
    """
    Recordatorio NO destructivo para proveedores RFC verificables.

    Actualmente habilitado únicamente para VERIF4.

    Reglas:
    - Si la solicitud ya terminó, no hace nada.
    - No toma result_claim.
    - No finaliza pending.
    - No libera inflight/processing.
    - No toca cuotas.
    - No contabiliza.
    - Agrupa las solicitudes vencidas del mismo proveedor.
    """

    request_key = (
        request_key or ""
    ).strip()

    reminder_minutes = int(
        reminder_minutes or 0
    )

    if not request_key:
        return {
            "ok": True,
            "ignored": "empty_request_key",
        }

    pending = load_pending(
        request_key
    )

    # La solicitud que originó este job
    # ya fue atendida/finalizada.
    if not pending:
        print(
            "[RFC VERIFIABLE REMINDER SKIP]",
            {
                "request_key": request_key,
                "minutes": reminder_minutes,
                "reason": "pending_not_found",
            },
            flush=True,
        )

        return {
            "ok": True,
            "ignored": "already_finished",
        }

    provider_code = (
        pending.get("provider_code")
        or ""
    ).strip().upper()

    provider_group_jid = (
        pending.get("provider_group_jid")
        or ""
    ).strip()

    provider_instance = (
        pending.get("provider_instance")
        or ""
    ).strip()

    provider_name = (
        pending.get("provider_name")
        or provider_code
        or "Proveedor"
    ).strip()

    # Por ahora únicamente Roberto / VERIF4.
    if provider_code != "VERIF4":
        print(
            "[RFC VERIFIABLE REMINDER SKIP]",
            {
                "request_key": request_key,
                "provider_code": provider_code,
                "reason": "provider_not_enabled",
            },
            flush=True,
        )

        return {
            "ok": True,
            "ignored": "provider_not_enabled",
        }

    if (
        not provider_group_jid
        or not provider_instance
    ):
        print(
            "[RFC VERIFIABLE REMINDER SKIP]",
            {
                "request_key": request_key,
                "reason": "provider_destination_missing",
            },
            flush=True,
        )

        return {
            "ok": True,
            "ignored": "provider_destination_missing",
        }

    # Evita que varios jobs que vencen casi al mismo
    # tiempo envíen el mismo bloque repetidamente.
    cooldown_key = (
        "rfc:verifiable:reminder:cooldown:"
        f"{provider_code}:"
        f"{provider_group_jid}:"
        f"{reminder_minutes}"
    )

    cooldown_claimed = redis_stats.set(
        cooldown_key,
        request_key,
        nx=True,
        ex=300,
    )

    if not cooldown_claimed:
        print(
            "[RFC VERIFIABLE REMINDER SKIP]",
            {
                "request_key": request_key,
                "provider_code": provider_code,
                "minutes": reminder_minutes,
                "reason": "provider_reminder_cooldown",
            },
            flush=True,
        )

        return {
            "ok": True,
            "ignored": "provider_reminder_cooldown",
        }

    now_epoch = time.time()
    minimum_age_seconds = (
        max(1, reminder_minutes) * 60
    )

    overdue = []

    try:
        for redis_key in redis_stats.scan_iter(
            match="rfc:verifiable:pending:*",
            count=300,
        ):
            try:
                raw = redis_stats.get(
                    redis_key
                )

                if not raw:
                    continue

                if isinstance(raw, bytes):
                    raw = raw.decode(
                        "utf-8",
                        errors="ignore",
                    )

                item = json.loads(raw)

                if not isinstance(item, dict):
                    continue

                item_provider_code = (
                    item.get("provider_code")
                    or ""
                ).strip().upper()

                item_provider_group = (
                    item.get("provider_group_jid")
                    or ""
                ).strip()

                item_provider_instance = (
                    item.get("provider_instance")
                    or ""
                ).strip()

                if (
                    item_provider_code
                    != provider_code
                ):
                    continue

                if (
                    item_provider_group
                    != provider_group_jid
                ):
                    continue

                if (
                    item_provider_instance
                    != provider_instance
                ):
                    continue

                started_at = float(
                    item.get(
                        "request_started_at_epoch"
                    )
                    or 0
                )

                if not started_at:
                    continue

                age_seconds = max(
                    0,
                    now_epoch - started_at,
                )

                if (
                    age_seconds
                    < minimum_age_seconds
                ):
                    continue

                identifier = (
                    item.get(
                        "provider_identifier"
                    )
                    or item.get(
                        "original_identifier"
                    )
                    or ""
                ).strip().upper()

                if not identifier:
                    continue

                age_minutes = int(
                    age_seconds // 60
                )

                # Request key real tomado de la llave Redis.
                # No dependemos de que exista dentro del JSON pending.
                item_request_key = str(redis_key)

                pending_prefix = (
                    "rfc:verifiable:pending:"
                )

                if item_request_key.startswith(
                    pending_prefix
                ):
                    item_request_key = (
                        item_request_key[
                            len(pending_prefix):
                        ]
                    )

                reminder_sent_key = (
                    "rfc:verifiable:reminder:sent:"
                    f"{item_request_key}:"
                    f"{reminder_minutes}"
                )

                # Esta solicitud ya apareció anteriormente
                # en ESTE nivel (90 o 120).
                if redis_stats.exists(
                    reminder_sent_key
                ):
                    continue

                overdue.append(
                    {
                        "identifier": identifier,
                        "age_minutes": age_minutes,
                        "started_at": started_at,
                        "request_key": item_request_key,
                        "reminder_sent_key": (
                            reminder_sent_key
                        ),
                    }
                )

            except Exception as scan_item_exc:
                print(
                    "[RFC VERIFIABLE REMINDER "
                    "SCAN ITEM ERROR]",
                    repr(scan_item_exc),
                    flush=True,
                )

    except Exception:
        # Si falla el scan, permitir un nuevo intento.
        redis_stats.delete(
            cooldown_key
        )
        raise

    if not overdue:
        redis_stats.delete(
            cooldown_key
        )

        print(
            "[RFC VERIFIABLE REMINDER SKIP]",
            {
                "request_key": request_key,
                "provider_code": provider_code,
                "minutes": reminder_minutes,
                "reason": "no_overdue_pending",
            },
            flush=True,
        )

        return {
            "ok": True,
            "ignored": "no_overdue_pending",
        }

    # Más antiguas primero.
    overdue.sort(
        key=lambda x: x["started_at"]
    )

    def _format_elapsed(total_minutes: int) -> str:
        total_minutes = max(
            0,
            int(total_minutes),
        )

        hours, minutes = divmod(
            total_minutes,
            60,
        )

        if hours:
            return (
                f"{hours} h {minutes} min"
            )

        return f"{minutes} min"

    lines = []

    for index, item in enumerate(
        overdue,
        start=1,
    ):
        lines.append(
            f"{index}. RFC: "
            f"{item['identifier']} — "
            f"{_format_elapsed(item['age_minutes'])}"
        )

    oldest_minutes = max(
        item["age_minutes"]
        for item in overdue
    )

    message = (
        "⏰ *RFC verificables pendientes*\n\n"
        f"Hay *{len(overdue)}* "
        "solicitud"
        f"{'es' if len(overdue) != 1 else ''} "
        "pendiente"
        f"{'s' if len(overdue) != 1 else ''} "
        "de respuesta:\n\n"
        + "\n".join(lines)
        + "\n\n"
        "*Más antigua:* "
        + _format_elapsed(
            oldest_minutes
        )
        + "."
    )

    try:
        evolution_send_text_to_group(
            provider_group_jid,
            message,
            instance_name=(
                provider_instance
            ),
        )

        # El mensaje ya fue aceptado por Evolution.
        # Ahora sí marcamos cada solicitud incluida para
        # que no vuelva a aparecer en el mismo nivel.
        reminder_pipe = redis_stats.pipeline()

        for item in overdue:
            reminder_pipe.set(
                item["reminder_sent_key"],
                str(time.time()),
                ex=172800,
            )

        reminder_pipe.execute()

        print(
            "[RFC VERIFIABLE PROVIDER "
            "REMINDER SENT]",
            {
                "request_key": request_key,
                "provider_code": provider_code,
                "provider_name": provider_name,
                "provider_group": (
                    provider_group_jid
                ),
                "provider_instance": (
                    provider_instance
                ),
                "threshold_minutes": (
                    reminder_minutes
                ),
                "pending_count": len(
                    overdue
                ),
                "oldest_minutes": (
                    oldest_minutes
                ),
            },
            flush=True,
        )

    except Exception as send_exc:
        # Si el envío falló, permitir que el retry
        # vuelva a intentarlo.
        redis_stats.delete(
            cooldown_key
        )

        print(
            "[RFC VERIFIABLE PROVIDER "
            "REMINDER ERROR]",
            {
                "request_key": request_key,
                "provider_code": provider_code,
                "minutes": reminder_minutes,
                "error": repr(
                    send_exc
                ),
            },
            flush=True,
        )

        raise

    return {
        "ok": True,
        "reminder_sent": True,
        "request_key": request_key,
        "provider_code": provider_code,
        "threshold_minutes": reminder_minutes,
        "pending_count": len(overdue),
        "oldest_minutes": oldest_minutes,
    }


def process_verifiable_timeout_job(
    request_key: str,
):
    """
    Se ejecuta 24 horas después de enviar una
    solicitud RFC verificable al proveedor.

    Si la solicitud todavía está pendiente:
    - avisa al cliente;
    - libera inflight y processing;
    - elimina el pendiente;
    - permite volver a solicitarla;
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

    verifiable_processing_key = (
        pending.get(
            "verifiable_processing_key"
        )
        or ""
    ).strip()

    provider_message_id = (
        pending.get("provider_message_id")
        or ""
    ).strip()

    timeout_job_data = {
        "is_verifiable": True,
        "evolution_instance": client_instance,
        "verifiable_original_type": original_type,
        "verifiable_original_identifier": original_identifier,
        "requester_label": requester_label,
        "batch_index": int(pending.get("batch_index") or 1),
        "batch_total": int(pending.get("batch_total") or 1),
    }

    try:
        if client_group_jid:
            evolution_send_text_to_group(
                client_group_jid,
                _job_client_message(
                    timeout_job_data,
                    title=(
                        "⏱️ RFC verificable "
                        "sin respuesta"
                    ),
                    requester_label=(
                        requester_label
                    ),
                    body=(
                        "No recibimos respuesta dentro "
                        "del tiempo permitido.\n"
                        "Ya puedes solicitarlo nuevamente."
                    ),
                ),
                instance_name=(
                    client_instance
                ),
            )

        print(
            "[RFC VERIFIABLE TIMEOUT SENT]",
            {
                "request_key":
                    request_key,
                "client_group":
                    client_group_jid,
                "client_instance":
                    client_instance,
                "original_type":
                    original_type,
                "original_identifier":
                    original_identifier,
            },
            flush=True,
        )

    except Exception as send_exc:
        print(
            "[RFC VERIFIABLE TIMEOUT "
            "SEND ERROR - RETRY]",
            {
                "request_key":
                    request_key,
                "client_group":
                    client_group_jid,
                "retry_remaining":
                    _rq_retry_remaining(),
                "error":
                    repr(send_exc),
            },
            flush=True,
        )

        # El cliente NO fue notificado.
        # No borrar pending ni processing.
        release_provider_result_claim(
            request_key
        )

        raise


    # ==========================================
    # EL AVISO SÍ SALIÓ.
    # AHORA SÍ CERRAR LA SOLICITUD.
    # ==========================================

    if inflight_key:
        redis_stats.delete(
            inflight_key
        )

    if verifiable_processing_key:
        redis_stats.delete(
            verifiable_processing_key
        )

    finish_pending(
        request_key,
        provider_message_id=(
            provider_message_id
        ),
    )

    release_provider_result_claim(
        request_key
    )

    print(
        "[RFC VERIFIABLE TIMEOUT FINALIZED]",
        {
            "request_key":
                request_key,
            "provider_message_id":
                provider_message_id,
        },
        flush=True,
    )

    return {
        "ok": True,
        "timed_out": True,
        "request_key": request_key,
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


def _rq_retry_remaining() -> int:
    """
    Devuelve cuántos reintentos RQ quedan
    para el job que se está ejecutando.
    """
    try:
        job = get_current_job()

        if not job:
            return 0

        return max(
            int(
                getattr(
                    job,
                    "retries_left",
                    0,
                )
                or 0
            ),
            0,
        )

    except Exception as retry_state_exc:
        print(
            "[RFC RQ RETRY STATE ERROR]",
            repr(retry_state_exc),
            flush=True,
        )

        return 0


def process_group_request_job(job_data: dict):
    global finish_pending
    # RFC_FINISH_PENDING_SCOPE_FIX_V1
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
    keep_inflight_for_retry = False

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

    verifiable_pdf_text_fallback_sent = False

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

        if requested_kind == "CURP":
            curp_result = analyze_curp(
                query or original_text or "",
                allow_repair=True,
            )

            if not curp_result.get("valid"):
                error_code = str(
                    curp_result.get("error")
                    or "INVALID_CURP"
                )

                if error_code == "INVALID_CHECK_DIGIT":
                    body = (
                        "La CURP tiene un dígito verificador incorrecto. "
                        "Revisa el último carácter antes de reenviarla."
                    )
                elif error_code == "INVALID_DATE":
                    body = "La fecha contenida en la CURP no es válida."
                else:
                    body = "La CURP no cumple con la estructura oficial esperada."

                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title="⚠️ CURP no válida",
                        requester_label=requester_label,
                        body=body,
                        family="result",
                        status="ERROR",
                    ),
                    instance_name=instance_name,
                )
                return

            normalized_curp = str(
                curp_result.get("normalized")
                or ""
            ).upper()

            if normalized_curp:
                query = normalized_curp
                job_data["query"] = normalized_curp

                if curp_result.get("corrected"):
                    print(
                        "[RFC CURP SAFE TYPO CORRECTED]",
                        {
                            "original": curp_result.get("original"),
                            "normalized": normalized_curp,
                            "corrections": curp_result.get("corrections"),
                        },
                        flush=True,
                    )

        if not _group_service_enabled_for_job(
            group_jid=group_jid,
            kind=requested_kind,
        ):
            print(
                "RFC_WORKER_GROUP_SERVICE_DISABLED =",
                {
                    "group_jid": group_jid,
                    "instance": instance_name,
                    "kind": requested_kind,
                    "request_key":
                        job_data.get(
                            "request_key"
                        ),
                },
                flush=True,
            )

            evolution_send_text_to_group(
                group_jid,
                _job_client_message(
                    job_data,
                    title="⚠️ Servicio temporalmente no disponible",
                    requester_label=requester_label,
                    body=(
                        "El servicio fue desactivado antes "
                        "de procesar esta solicitud."
                    ),
                ),
                instance_name=instance_name,
            )

            return

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

            # DOCIFY_VERIFICABLE_ASPOSE_BYPASS_V1
            # Solo DOCIFY MX verificable.
            # Si ya tenemos RFC + IDCIF de Roberto,
            # no esperar a Aspose durante la contingencia.
            if (
                is_verifiable
                # RFC_VERIF4_ASPOSE_BYPASS_ALL_CLIENTS_V1
                # Roberto puede responder solicitudes de distintos
                # bots cliente. El bypass depende del PROVEEDOR,
                # no del instance_name del cliente.
                and (
                    str(
                        job_data.get(
                            "verifiable_provider_db_name"
                        )
                        or ""
                    ).strip().upper()
                    == "RFC_VERIFIABLE_VERIF4"
                    or str(
                        job_data.get(
                            "verifiable_provider_name"
                        )
                        or ""
                    ).strip().upper()
                    == "ID ROBERTO LENTO"
                )
                and os.getenv(
                    "RFC_VERIF4_ASPOSE_BYPASS",
                    "0",
                ).strip() == "1"
                and str(
                    job_data.get("provider_rfc") or ""
                ).strip()
                and str(
                    job_data.get("provider_idcif") or ""
                ).strip()
            ):
                fake_response = requests.Response()
                fake_response.status_code = 500
                fake_response._content = (
                    b'{"error":"ASPOSE_PDF_CONVERT_FAIL:'
                    b'DOCIFY_EMERGENCY_BYPASS"}'
                )
                fake_response.url = (
                    BOT_INTERNAL_URL
                    + "/internal/generate-pdf"
                )

                print(
                    "[DOCIFY VERIFICABLE ASPOSE BYPASS]",
                    {
                        "rfc":
                            job_data.get("provider_rfc"),
                        "idcif":
                            job_data.get("provider_idcif"),
                    },
                    flush=True,
                )

                raise requests.HTTPError(
                    "DOCIFY ASPOSE BYPASS",
                    response=fake_response,
                )

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

                is_verifiable=is_verifiable,

                provider_rfc=(
                    job_data.get(
                        "provider_rfc"
                    )
                    or ""
                ),

                provider_idcif=(
                    job_data.get(
                        "provider_idcif"
                    )
                    or ""
                ),
            )

        else:
            raise RuntimeError("NO_TEXT_OR_MEDIA")

        # ASPOSE_VERIFICABLE_TEXT_FALLBACK_V1
        _fb_is_verifiable = (
            bool(is_verifiable)
            or str(
                job_data.get("_rfc_quota_family")
                or ""
            ).strip().upper() == "VERIFICABLE"
            or "verifiable_count_provider_success" in job_data
            or bool(job_data.get("verifiable_provider_code"))
        )

        if result.get("aspose_text_fallback") and _fb_is_verifiable:
            _fq = "\n".join(
                str(x)
                for x in (
                    result.get("fallback_query") or "",
                    query or "",
                    original_text or "",
                    job_data.get("rfc") or "",
                    job_data.get("idcif") or "",
                    job_data.get("provider_message") or "",
                    json.dumps(
                        job_data,
                        ensure_ascii=False,
                        default=str,
                    ),
                )
                if x
            )

            _rfc_m = re.search(
                r"(?i)\b(?:RFC\s*[:=-]?\s*)?([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b",
                _fq,
            )

            _idcif_m = re.search(
                r"(?i)\b(?:IDCIF|ID\s*CIF)\s*[:=-]?\s*(\d{11})\b",
                _fq,
            )

            if not _idcif_m:
                _idcif_m = re.search(
                    r"(?<!\d)(\d{11})(?!\d)",
                    _fq,
                )

            if not _rfc_m or not _idcif_m:
                raise RuntimeError(
                    "ASPOSE_VERIFICABLE_TEXT_PARSE_FAIL:"
                    + repr(_fq[:500])
                )

            _fb_rfc = _rfc_m.group(1).upper()
            _fb_idcif = _idcif_m.group(1)

            _fb_item_key = (
                f"RFC: {_fb_rfc}\\n"
                f"IDCIF: {_fb_idcif}"
            )

            _fb_reqkey = str(
                job_data.get("request_key")
                or ""
            ).strip()

            _fb_sent_key = (
                "rfc:verificable:text_fallback:"
                + _fb_reqkey
                + ":"
                + _fb_rfc
                + ":"
                + _fb_idcif
            )

            # Si ya fue enviado por este fallback, NO duplicar WhatsApp.
            if not redis_stats.exists(_fb_sent_key):
                _nl = chr(10)
                _fb_text = (
                    "✅ *RFC VERIFICABLE INFORMADO*"
                    + _nl + _nl
                    + f"RFC: *{_fb_rfc}*"
                    + _nl
                    + f"IDCIF: *{_fb_idcif}*"
                )

                print(
                    "[ASPOSE_VERIFICABLE_TEXT_SEND]",
                    {
                        "request_key": _fb_reqkey,
                        "group_jid": group_jid,
                        "rfc": _fb_rfc,
                        "idcif": _fb_idcif,
                    },
                    flush=True,
                )

                evolution_send_text_to_group(
                    group_jid,
                    _fb_text,
                    instance_name=instance_name,
                )

                redis_stats.set(
                    _fb_sent_key,
                    "1",
                    ex=604800,
                )

            # Contabilización normal del VERIFICABLE.
            _fb_recorded = record_success_once(
                job_data=job_data,
                group_jid=group_jid,
                group_name=group_name,
                kind="RFC_VERIFICABLE",
                count=1,
                item_key=_fb_item_key,
            )

            # ASPOSE_VERIFICABLE_PROVIDER_COUNT_V1
            _fb_provider_counted = False

            if bool(
                job_data.get(
                    "verifiable_count_provider_success",
                    True,
                )
            ):
                _fb_provider_key = (
                    "rfc:verificable:text_fallback:"
                    "provider_counted:"
                    + _fb_reqkey
                    + ":"
                    + _fb_rfc
                )

                _fb_claim_provider = redis_stats.set(
                    _fb_provider_key,
                    "PENDING",
                    nx=True,
                    ex=604800,
                )

                if _fb_claim_provider:
                    try:
                        record_verifiable_provider_success(
                            job_data
                        )

                        redis_stats.set(
                            _fb_provider_key,
                            "1",
                            ex=604800,
                        )

                        _fb_provider_counted = True

                        print(
                            "[ASPOSE_VERIFICABLE_PROVIDER_COUNTED]",
                            {
                                "request_key": _fb_reqkey,
                                "rfc": _fb_rfc,
                                "group_jid": group_jid,
                            },
                            flush=True,
                        )

                    except Exception:
                        redis_stats.delete(
                            _fb_provider_key
                        )
                        raise

            # El fallback ya entregó y contabilizó correctamente.
            # También debemos cerrar el pending del proveedor para
            # evitar recordatorios falsos en MESINO.
            try:
                from app.verifiable_flow import finish_pending

                if _fb_reqkey:
                    finish_pending(_fb_reqkey)

                    print(
                        "[ASPOSE_VERIFICABLE_PENDING_FINISHED]",
                        {
                            "request_key": _fb_reqkey,
                            "rfc": _fb_rfc,
                        },
                        flush=True,
                    )

            except Exception as _fb_finish_exc:
                # La entrega ya ocurrió; una falla de limpieza no debe
                # convertir el resultado exitoso en error.
                print(
                    "[ASPOSE_VERIFICABLE_PENDING_FINISH_WARN]",
                    {
                        "request_key": _fb_reqkey,
                        "error": repr(_fb_finish_exc),
                    },
                    flush=True,
                )

            print(
                "[ASPOSE_VERIFICABLE_TEXT_DONE]",
                {
                    "request_key": _fb_reqkey,
                    "group_jid": group_jid,
                    "rfc": _fb_rfc,
                    "idcif": _fb_idcif,
                    "recorded": bool(_fb_recorded),
                },
                flush=True,
            )

            return

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
            internal_error = result.get("error") or "UNKNOWN_RESULT_ERROR"
            print(
                "[RFC CLIENT RESULT ERROR HIDDEN]",
                {
                    "request_key": job_data.get("request_key"),
                    "internal_error": internal_error,
                },
                flush=True,
            )
            evolution_send_text_to_group(
                group_jid,
                _job_client_message(
                    job_data,
                    title="⚠️ No pudimos completar la solicitud",
                    requester_label=requester_label,
                    body=(
                        "Ocurrió una interrupción temporal.\n"
                        "Intenta nuevamente en 2-3 minutos."
                    ),
                ),
                instance_name=instance_name,
            )
            return

        kind = _classify_success_kind(query=query or "", original_text=original_text or "", msg_type=msg_type)
        if forced_success_kind:
            kind = forced_success_kind
        mode = (result.get("mode") or "single").strip().lower()

        if mode == "verifiable_text":
            if not is_verifiable:
                raise RuntimeError(
                    "VERIFIABLE_TEXT_ON_"
                    "NON_VERIFIABLE_JOB"
                )

            delivered_rfc = str(
                result.get("rfc")
                or job_data.get("provider_rfc")
                or ""
            ).strip().upper()

            delivered_idcif = str(
                result.get("idcif")
                or job_data.get("provider_idcif")
                or ""
            ).strip()

            delivery_text = str(
                result.get("text")
                or ""
            ).strip()

            if not delivered_rfc:
                raise RuntimeError(
                    "VERIFIABLE_TEXT_RFC_EMPTY"
                )

            if not delivered_idcif:
                raise RuntimeError(
                    "VERIFIABLE_TEXT_IDCIF_EMPTY"
                )

            if not delivery_text:
                delivery_text = (
                    f"RFC: {delivered_rfc}\n"
                    f"IDCIF: {delivered_idcif}"
                )

            delivery_item_key = (
                f"VERIFICABLE_TEXT:"
                f"{delivered_rfc}:"
                f"{delivered_idcif}"
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
                    "[RFC VERIFICABLE TEXT "
                    "DUPLICATE SUPPRESSED]",
                    {
                        "request_key":
                            verifiable_request_key,
                        "rfc": delivered_rfc,
                        "idcif": delivered_idcif,
                    },
                    flush=True,
                )
                return

            elapsed_seconds = max(
                0.0,
                time.time()
                - request_started_at_epoch,
            )

            client_text = _job_client_message(
                job_data,
                title="⚠️ Constancia no disponible en SAT",
                requester_label=requester_label,
                body=(
                    "El SAT no permitió generar la constancia en este momento.\n"
                    "Se entregan los datos localizados.\n\n"
                    "⏱️ Tiempo total: "
                    f"{_format_total_time(elapsed_seconds)}"
                ),
                result_mode=True,
                result_rfc=delivered_rfc,
                result_idcif=delivered_idcif,
            )

            try:
                evolution_send_text_to_group(
                    group_jid,
                    client_text,
                    instance_name=instance_name,
                )

            except requests.Timeout as send_exc:
                print(
                    "[RFC VERIFICABLE TEXT TIMEOUT]",
                    {
                        "error": repr(send_exc),
                        "delivery_lock_key":
                            delivery_lock_key,
                        "request_key":
                            verifiable_request_key,
                        "rfc":
                            delivered_rfc,
                        "idcif":
                            delivered_idcif,
                    },
                    flush=True,
                )

                release_delivery_claim(
                    delivery_lock_key
                )

                raise

            except Exception as send_exc:
                release_delivery_claim(
                    delivery_lock_key
                )

                print(
                    "[RFC VERIFICABLE TEXT "
                    "SEND ERROR]",
                    repr(send_exc),
                    flush=True,
                )

                raise

            try:
                # Marcar ENTREGA antes de contabilizar. Si PostgreSQL falla
                # después del envío, el retry repara contabilidad sin reenviar.
                mark_delivery_pending_accounting(
                    delivery_lock_key,
                    delivery_done_key,
                    job_data=job_data,
                    group_jid=group_jid,
                    group_name=group_name,
                    kind=kind,
                    count=1,
                    item_key=delivery_item_key,
                )

                # is_verifiable ya fuerza:
                # kind = RFC_VERIFICABLE
                success_recorded = (
                    record_success_once(
                        job_data=job_data,
                        group_jid=group_jid,
                        group_name=group_name,
                        kind=kind,
                        count=1,
                        item_key=delivery_item_key,
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

                mark_delivery_accounted(
                    delivery_done_key
                )

                if (
                    verifiable_request_key
                ):
                    completion_marked = (
                        mark_verifiable_completed_24h(
                            job_data
                        )
                    )

                    if not completion_marked:
                        raise RuntimeError(
                            "RFC_VERIFICABLE_"
                            "COMPLETED_24H_MARK_FAILED"
                        )

                    finish_pending(
                        verifiable_request_key,
                        provider_message_id=(
                            job_data.get(
                                "provider_request_msg_id"
                            )
                            or ""
                        ),
                    )

                    print(
                        "[RFC VERIFICABLE TEXT "
                        "PENDING FINISHED]",
                        {
                            "request_key":
                                verifiable_request_key,
                            "rfc": delivered_rfc,
                            "idcif": delivered_idcif,
                            "kind": kind,
                        },
                        flush=True,
                    )

                return

            except Exception as accounting_exc:
                print(
                    "[RFC VERIFICABLE TEXT "
                    "ACCOUNTING ERROR]",
                    repr(accounting_exc),
                    {
                        "group_jid":
                            group_jid,
                        "kind":
                            kind,
                        "item_key":
                            delivery_item_key,
                    },
                    flush=True,
                )

                raise

        if mode == "batch_zip":
            zip_url = (result.get("zip_url") or "").strip()
            file_name = (result.get("filename") or "constancias_lote.zip").strip()
            ok_count = int(result.get("ok_count") or 0)

            if not zip_url:
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title="⚠️ No pudimos entregar el lote",
                        requester_label=requester_label,
                        body=(
                            "El lote fue procesado, pero no se obtuvo "
                            "el enlace de entrega."
                        ),
                        include_identity=False,
                    ),
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
                    "[RFC PDF TIMEOUT]",
                    {
                        "error": repr(media_err),
                        "delivery_lock_key": delivery_lock_key,
                        "is_verifiable": is_verifiable,
                        "provider_rfc": job_data.get("provider_rfc"),
                        "provider_idcif": job_data.get("provider_idcif"),
                    },
                    flush=True,
                )

                release_delivery_claim(
                    delivery_lock_key
                )

                if is_verifiable:
                    fallback_rfc = (
                        job_data.get("provider_rfc")
                        or ""
                    ).strip().upper()

                    fallback_idcif = (
                        job_data.get("provider_idcif")
                        or ""
                    ).strip()

                    if fallback_rfc and fallback_idcif:
                        try:
                            evolution_send_text_to_group(
                                group_jid,
                                _job_client_message(
                                    job_data,
                                    title="⚠️ Problema al adjuntar la constancia",
                                    requester_label=requester_label,
                                    body=(
                                        "La constancia fue generada, pero hubo un problema "
                                        "temporal al adjuntar el PDF."
                                    ),
                                    result_mode=True,
                                    result_rfc=fallback_rfc,
                                    result_idcif=fallback_idcif,
                                ),
                                instance_name=instance_name,
                            )

                            print(
                                "[RFC VERIFICABLE PDF TIMEOUT "
                                "TEXT FALLBACK SENT]",
                                {
                                    "request_key":
                                        verifiable_request_key,
                                    "rfc": fallback_rfc,
                                    "idcif": fallback_idcif,
                                },
                                flush=True,
                            )

                            verifiable_pdf_text_fallback_sent = True

                        except Exception as fallback_send_exc:
                            print(
                                "[RFC VERIFICABLE PDF TIMEOUT "
                                "TEXT FALLBACK ERROR]",
                                repr(fallback_send_exc),
                                flush=True,
                            )

                            raise

                raise

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
                    _job_client_message(
                        job_data,
                        title="⚠️ Problema al adjuntar el lote",
                        requester_label=requester_label,
                        body=(
                            "El lote fue generado correctamente, pero no pudo adjuntarse.\n"
                            "Puedes abrirlo desde el siguiente enlace:\n"
                            f"{zip_url}"
                        ),
                        include_identity=False,
                    ),
                    instance_name=instance_name,
                )

                return

            try:
                if ok_count > 0:
                    mark_delivery_pending_accounting(
                        delivery_lock_key,
                        delivery_done_key,
                        job_data=job_data,
                        group_jid=group_jid,
                        group_name=group_name,
                        kind=kind,
                        count=ok_count,
                        item_key=delivery_item_key,
                    )
                else:
                    # Conserva la idempotencia de entrega aunque el lote no
                    # tenga elementos comercialmente contabilizables.
                    mark_delivery_done(
                        delivery_lock_key,
                        delivery_done_key,
                    )

                success_recorded = False

                if ok_count > 0:
                    success_recorded = (
                        record_success_once(
                            job_data=job_data,
                            group_jid=group_jid,
                            group_name=group_name,
                            kind=kind,
                            count=ok_count,
                            item_key=delivery_item_key,
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

                if ok_count > 0:
                    mark_delivery_accounted(
                        delivery_done_key
                    )

                if (
                    ok_count > 0
                    and is_verifiable
                    and verifiable_request_key
                ):
                    completion_marked = (
                        mark_verifiable_completed_24h(
                            job_data
                        )
                    )

                    if not completion_marked:
                        raise RuntimeError(
                            "RFC_VERIFIABLE_"
                            "COMPLETED_24H_MARK_FAILED"
                        )

                    finish_pending(
                        verifiable_request_key,
                        provider_message_id=(
                            job_data.get(
                                "provider_request_msg_id"
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
                            "mode": "batch_zip",
                            "ok_count": ok_count,
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
            delivered_success_count = 0
            batch_had_done_delivery = False

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
                        repair_verifiable_after_done=False,
                    )

                    if not claimed:
                        if redis_stats.exists(
                            delivery_done_key
                        ):
                            batch_had_done_delivery = True

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
                            "[RFC BATCH ITEM TIMEOUT - RETRY]",
                            repr(media_err),
                            delivery_lock_key,
                            flush=True,
                        )

                        release_delivery_claim(
                            delivery_lock_key
                        )

                        raise

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
                            _job_client_message(
                            job_data,
                            title="⚠️ Problema al adjuntar el documento",
                            requester_label=requester_label,
                            body=(
                                "El documento fue generado, pero no pudo adjuntarse.\n"
                                f"{pdf_url}"
                            ),
                            family="result",
                            status="AVISO",
                            type_override=(
                                "RFC IDCIF"
                                if idcif
                                else "RFC"
                            ),
                            data_override=(rfc or "N/D"),
                        ),
                            instance_name=instance_name,
                        )

                        continue

                    try:
                        mark_delivery_pending_accounting(
                            delivery_lock_key,
                            delivery_done_key,
                            job_data=job_data,
                            group_jid=group_jid,
                            group_name=group_name,
                            kind=kind,
                            count=1,
                            item_key=item_key,
                        )

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

                        mark_delivery_accounted(
                            delivery_done_key
                        )

                        delivered_success_count += 1

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
                    print(
                        "[RFC BATCH ITEM CLIENT ERROR HIDDEN]",
                        {"rfc": rfc, "idcif": idcif, "internal_error": err},
                        flush=True,
                    )
                    evolution_send_text_to_group(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title="⚠️ No pudimos completar la solicitud",
                            requester_label=requester_label,
                            body=(
                                "Ocurrió una interrupción temporal.\n"
                                "Intenta nuevamente en unos minutos."
                            ),
                            family="result",
                            status="ERROR",
                            type_override=(
                                "RFC IDCIF"
                                if idcif
                                else "RFC"
                            ),
                            data_override=(rfc or "N/D"),
                        ),
                        instance_name=instance_name
                    )

            if (
                provider_success_count > 0
                and bool(
                    job_data.get(
                        "verifiable_count_provider_success",
                        True,
                    )
                )
            ):
                record_verifiable_provider_success(
                    job_data,
                    count=provider_success_count,
                )

            if (
                (
                    delivered_success_count > 0
                    or batch_had_done_delivery
                )
                and is_verifiable
                and verifiable_request_key
            ):
                completion_marked = (
                    mark_verifiable_completed_24h(
                        job_data
                    )
                )

                if not completion_marked:
                    raise RuntimeError(
                        "RFC_VERIFIABLE_"
                        "COMPLETED_24H_MARK_FAILED"
                    )

                finish_pending(
                    verifiable_request_key,
                    provider_message_id=(
                        job_data.get(
                            "provider_request_msg_id"
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
                        "mode": "batch_multi",
                        "provider_success_count": (
                            provider_success_count
                        ),
                        "delivered_success_count": (
                            delivered_success_count
                        ),
                    },
                    flush=True,
                )

            return

        pdf_url = (result.get("pdf_url") or "").strip()
        file_name = (result.get("filename") or "documento.pdf").strip()

        if not pdf_url:
            evolution_send_text_to_group(
                group_jid,
                _job_client_message(
                    job_data,
                    title="⚠️ No pudimos entregar el documento",
                    requester_label=requester_label,
                    body=(
                        "El procesamiento terminó, pero no se obtuvo el enlace del PDF.\n"
                        "Intenta nuevamente en unos minutos."
                    ),
                ),
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


        try:
            evolution_send_media_to_group(
                group_jid=group_jid,
                media_url=pdf_url,
                file_name=file_name,
                instance_name=instance_name,
                caption=time_caption,
            )

        except (
            requests.Timeout,
            requests.ConnectionError,
            requests.HTTPError,
        ) as media_err:
            print(
                "[RFC PDF SEND FAILURE]",
                {
                    "error": repr(media_err),
                    "delivery_lock_key":
                        delivery_lock_key,
                    "is_verifiable":
                        is_verifiable,
                    "provider_rfc":
                        job_data.get(
                            "provider_rfc"
                        ),
                    "provider_idcif":
                        job_data.get(
                            "provider_idcif"
                        ),
                },
                flush=True,
            )

            if is_verifiable:
                fallback_rfc = (
                    job_data.get(
                        "provider_rfc"
                    )
                    or ""
                ).strip().upper()

                fallback_idcif = (
                    job_data.get(
                        "provider_idcif"
                    )
                    or ""
                ).strip()

                if (
                    fallback_rfc
                    and fallback_idcif
                ):
                    try:
                        evolution_send_text_to_group(
                            group_jid,
                            _job_client_message(
                                job_data,
                                title="⚠️ Problema al adjuntar la constancia",
                                requester_label=requester_label,
                                body=(
                                    "La constancia fue generada, pero hubo un problema "
                                    "temporal al adjuntar el PDF."
                                ),
                                result_mode=True,
                                result_rfc=fallback_rfc,
                                result_idcif=fallback_idcif,
                            ),
                            instance_name=instance_name,
                        )

                        # Desde este punto el cliente YA recibió
                        # un resultado válido por texto.
                        verifiable_pdf_text_fallback_sent = True

                        print(
                            "[RFC VERIFICABLE PDF "
                            "TEXT FALLBACK SENT]",
                            {
                                "request_key":
                                    verifiable_request_key,
                                "rfc":
                                    fallback_rfc,
                                "idcif":
                                    fallback_idcif,
                            },
                            flush=True,
                        )

                    except Exception as fallback_exc:
                        # No se entregó ni PDF ni texto:
                        # liberar para permitir otro intento.
                        release_delivery_claim(
                            delivery_lock_key
                        )

                        print(
                            "[RFC VERIFICABLE PDF "
                            "TEXT FALLBACK ERROR]",
                            {
                                "request_key":
                                    verifiable_request_key,
                                "error":
                                    repr(fallback_exc),
                            },
                            flush=True,
                        )

                        raise

                    try:
                        mark_delivery_pending_accounting(
                            delivery_lock_key,
                            delivery_done_key,
                            job_data=job_data,
                            group_jid=group_jid,
                            group_name=group_name,
                            kind=kind,
                            count=1,
                            item_key=delivery_item_key,
                        )

                        # IMPORTANTE:
                        # is_verifiable ya fuerza kind a
                        # RFC_VERIFICABLE más arriba.
                        success_recorded = (
                            record_success_once(
                                job_data=job_data,
                                group_jid=group_jid,
                                group_name=group_name,
                                kind=kind,
                                count=1,
                                item_key=delivery_item_key,
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

                        # La entrega lógica ya se marcó antes de accounting.
                        mark_delivery_accounted(
                            delivery_done_key
                        )

                        if (
                            verifiable_request_key
                        ):
                            completion_marked = (
                                mark_verifiable_completed_24h(
                                    job_data
                                )
                            )

                            if not completion_marked:
                                raise RuntimeError(
                                    "RFC_VERIFICABLE_"
                                    "COMPLETED_24H_MARK_FAILED"
                                )

                            finish_pending(
                                verifiable_request_key,
                                provider_message_id=(
                                    job_data.get(
                                        "provider_request_msg_id"
                                    )
                                    or ""
                                ),
                            )

                            print(
                                "[RFC VERIFICABLE PDF "
                                "TEXT FALLBACK PENDING FINISHED]",
                                {
                                    "request_key":
                                        verifiable_request_key,
                                    "rfc":
                                        fallback_rfc,
                                    "idcif":
                                        fallback_idcif,
                                    "kind":
                                        kind,
                                },
                                flush=True,
                            )

                        print(
                            "[RFC VERIFICABLE PDF "
                            "TEXT FALLBACK COMPLETED]",
                            {
                                "request_key":
                                    verifiable_request_key,
                                "kind":
                                    kind,
                                "success_recorded":
                                    success_recorded,
                                "rfc":
                                    fallback_rfc,
                                "idcif":
                                    fallback_idcif,
                            },
                            flush=True,
                        )

                        # Ya hubo entrega + contabilización +
                        # cierre del pendiente.
                        return

                    except Exception as accounting_exc:
                        print(
                            "[RFC VERIFICABLE PDF "
                            "TEXT FALLBACK ACCOUNTING ERROR]",
                            {
                                "request_key":
                                    verifiable_request_key,
                                "kind":
                                    kind,
                                "item_key":
                                    delivery_item_key,
                                "error":
                                    repr(accounting_exc),
                            },
                            flush=True,
                        )

                        # No mandar el mensaje genérico porque
                        # RFC+IDCIF ya llegó al cliente.
                        raise

            # Si no es verificable o faltó RFC/IDCIF,
            # no hubo fallback válido.
            release_delivery_claim(
                delivery_lock_key
            )

            raise

        except Exception as media_err:
            print(
                "[RFC PDF SEND ERROR]",
                repr(media_err),
                flush=True,
            )

            if is_verifiable:
                fallback_rfc = (
                    job_data.get(
                        "provider_rfc"
                    )
                    or ""
                ).strip().upper()

                fallback_idcif = (
                    job_data.get(
                        "provider_idcif"
                    )
                    or ""
                ).strip()

                if (
                    fallback_rfc
                    and fallback_idcif
                ):
                    try:
                        evolution_send_text_to_group(
                            group_jid,
                            _job_client_message(
                                job_data,
                                title="⚠️ Problema al adjuntar la constancia",
                                requester_label=requester_label,
                                body=(
                                    "La constancia fue generada, pero hubo un problema "
                                    "temporal al adjuntar el PDF."
                                ),
                                result_mode=True,
                                result_rfc=fallback_rfc,
                                result_idcif=fallback_idcif,
                            ),
                            instance_name=instance_name,
                        )

                        verifiable_pdf_text_fallback_sent = True

                        print(
                            "[RFC VERIFICABLE PDF "
                            "TEXT FALLBACK SENT]",
                            {
                                "request_key":
                                    verifiable_request_key,
                                "rfc":
                                    fallback_rfc,
                                "idcif":
                                    fallback_idcif,
                            },
                            flush=True,
                        )

                    except Exception as fallback_exc:
                        release_delivery_claim(
                            delivery_lock_key
                        )

                        print(
                            "[RFC VERIFICABLE PDF "
                            "TEXT FALLBACK ERROR]",
                            repr(fallback_exc),
                            flush=True,
                        )

                        raise

                    try:
                        mark_delivery_pending_accounting(
                            delivery_lock_key,
                            delivery_done_key,
                            job_data=job_data,
                            group_jid=group_jid,
                            group_name=group_name,
                            kind=kind,
                            count=1,
                            item_key=delivery_item_key,
                        )

                        success_recorded = (
                            record_success_once(
                                job_data=job_data,
                                group_jid=group_jid,
                                group_name=group_name,
                                kind=kind,
                                count=1,
                                item_key=delivery_item_key,
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

                        mark_delivery_accounted(
                            delivery_done_key
                        )

                        if verifiable_request_key:
                            completion_marked = (
                                mark_verifiable_completed_24h(
                                    job_data
                                )
                            )

                            if not completion_marked:
                                raise RuntimeError(
                                    "RFC_VERIFICABLE_"
                                    "COMPLETED_24H_MARK_FAILED"
                                )

                            finish_pending(
                                verifiable_request_key,
                                provider_message_id=(
                                    job_data.get(
                                        "provider_request_msg_id"
                                    )
                                    or ""
                                ),
                            )

                            print(
                                "[RFC VERIFICABLE PDF "
                                "TEXT FALLBACK PENDING FINISHED]",
                                {
                                    "request_key":
                                        verifiable_request_key,
                                    "rfc":
                                        fallback_rfc,
                                    "idcif":
                                        fallback_idcif,
                                    "kind":
                                        kind,
                                },
                                flush=True,
                            )

                        print(
                            "[RFC VERIFICABLE PDF "
                            "TEXT FALLBACK COMPLETED]",
                            {
                                "request_key":
                                    verifiable_request_key,
                                "kind":
                                    kind,
                                "success_recorded":
                                    success_recorded,
                            },
                            flush=True,
                        )

                        return

                    except Exception as accounting_exc:
                        print(
                            "[RFC VERIFICABLE PDF "
                            "TEXT FALLBACK ACCOUNTING ERROR]",
                            repr(accounting_exc),
                            flush=True,
                        )

                        raise

            release_delivery_claim(
                delivery_lock_key
            )

            evolution_send_text_to_group(
                group_jid,
                _job_client_message(
                    job_data,
                    title="⚠️ Problema al entregar el documento",
                    requester_label=requester_label,
                    body=(
                        "El documento fue generado, pero no pudo adjuntarse.\n"
                        "Puedes abrirlo desde el siguiente enlace:\n"
                        f"{pdf_url}"
                    ),
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
                    _job_client_message(
                        job_data,
                        title="ℹ️ Corrección de dato",
                        requester_label=requester_label,
                        body=(
                            f"Dato enviado: {original_identifier}\n"
                            f"RFC localizado: {corrected_rfc}\n\n"
                            "La constancia fue generada con el RFC localizado."
                        ),
                        family="result",
                        status="AVISO",
                        data_override=original_identifier,
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
            mark_delivery_pending_accounting(
                delivery_lock_key,
                delivery_done_key,
                job_data=job_data,
                group_jid=group_jid,
                group_name=group_name,
                kind=kind,
                count=1,
                item_key=delivery_item_key,
            )

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

            mark_delivery_accounted(
                delivery_done_key
            )

            # Una vez entregado y contabilizado correctamente,
            # elimina definitivamente el pendiente verificable.
            if (
                is_verifiable
                and verifiable_request_key
            ):
                completion_marked = (
                    mark_verifiable_completed_24h(
                        job_data
                    )
                )

                if not completion_marked:
                    raise RuntimeError(
                        "RFC_VERIFIABLE_"
                        "COMPLETED_24H_MARK_FAILED"
                    )

                finish_pending(
                    verifiable_request_key,
                    provider_message_id=(
                        job_data.get(
                            "provider_request_msg_id"
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
        print(
            "process_group_request_job HTTPError:",
            repr(e),
            flush=True,
        )
        traceback.print_exc()

        status_code = int(
            getattr(
                getattr(
                    e,
                    "response",
                    None,
                ),
                "status_code",
                0,
            )
            or 0
        )

        transient_http_error = (
            status_code == 0
            or status_code in {
                408,
                425,
                429,
            }
            or status_code >= 500
        )

        retry_remaining = (
            _rq_retry_remaining()
        )

        resp_text = ""
        err_code = ""

        try:
            resp_text = (
                e.response.text
                or ""
            )
        except Exception:
            pass

        print(
            "process_group_request_job "
            "HTTP response body:",
            resp_text,
            flush=True,
        )

        # ==========================================
        # RFC_VERIFIABLE_ASPOSE_HTTP_FALLBACK_V1
        #
        # Si RFC+IDCIF ya fueron entregados por el
        # proveedor, una caída de Aspose NO debe
        # impedir entrega ni contabilización.
        # ==========================================
        if (
            is_verifiable
            and "ASPOSE_PDF_CONVERT_FAIL"
                in resp_text.upper()
        ):
            fallback_rfc = str(
                job_data.get("provider_rfc")
                or ""
            ).strip().upper()

            fallback_idcif = str(
                job_data.get("provider_idcif")
                or ""
            ).strip()

            if fallback_rfc and fallback_idcif:
                delivery_item_key = (
                    "VERIFICABLE_ASPOSE_TEXT:"
                    f"{fallback_rfc}:"
                    f"{fallback_idcif}"
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
                        "[RFC VERIFICABLE ASPOSE "
                        "TEXT DUPLICATE SUPPRESSED]",
                        {
                            "rfc": fallback_rfc,
                            "idcif": fallback_idcif,
                        },
                        flush=True,
                    )
                    return

                try:
                    evolution_send_text_to_group(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title=(
                                "⚠️ Problema temporal "
                                "al generar la constancia"
                            ),
                            requester_label=(
                                requester_label
                            ),
                            body=(
                                "El RFC e IDCIF fueron "
                                "localizados correctamente, "
                                "pero no fue posible generar "
                                "el PDF en este momento."
                            ),
                            result_mode=True,
                            result_rfc=fallback_rfc,
                            result_idcif=fallback_idcif,
                        ),
                        instance_name=instance_name,
                    )

                    print(
                        "[RFC VERIFICABLE ASPOSE "
                        "TEXT FALLBACK SENT]",
                        {
                            "request_key":
                                job_data.get(
                                    "verifiable_request_key"
                                ),
                            "rfc": fallback_rfc,
                            "idcif": fallback_idcif,
                        },
                        flush=True,
                    )

                except Exception:
                    release_delivery_claim(
                        delivery_lock_key
                    )
                    raise

                success_recorded = (
                    record_success_once(
                        job_data=job_data,
                        group_jid=group_jid,
                        group_name=group_name,
                        kind="RFC_VERIFICABLE",
                        count=1,
                        item_key=delivery_item_key,
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

                verifiable_key = str(
                    job_data.get(
                        "verifiable_request_key"
                    )
                    or job_data.get("request_key")
                    or ""
                ).strip()

                if verifiable_key:
                    completion_marked = (
                        mark_verifiable_completed_24h(
                            job_data
                        )
                    )

                    if not completion_marked:
                        raise RuntimeError(
                            "RFC_VERIFICABLE_"
                            "COMPLETED_24H_MARK_FAILED"
                        )

                    finish_pending(
                        verifiable_key,
                        provider_message_id=(
                            job_data.get(
                                "provider_request_msg_id"
                            )
                            or ""
                        ),
                    )

                print(
                    "[RFC VERIFICABLE ASPOSE "
                    "TEXT FALLBACK COMPLETED]",
                    {
                        "request_key":
                            verifiable_key,
                        "rfc": fallback_rfc,
                        "idcif": fallback_idcif,
                        "success_recorded":
                            success_recorded,
                    },
                    flush=True,
                )

                return

        # ==========================================
        # ERROR HTTP TRANSITORIO:
        # mientras queden retries, no avisar todavía
        # al cliente. RQ debe reintentar.
        # ==========================================
        if (
            transient_http_error
            and retry_remaining > 0
        ):
            keep_inflight_for_retry = True

            print(
                "[RFC HTTP TRANSIENT - RQ RETRY]",
                {
                    "status_code":
                        status_code,
                    "retry_remaining":
                        retry_remaining,
                    "inflight_retained":
                        inflight_key,
                },
                flush=True,
            )

            raise

        try:
            try:
                obj = (
                    json.loads(resp_text)
                    if resp_text
                    else {}
                )

                err_code = str(
                    obj.get("error")
                    or obj.get("detail")
                    or ""
                ).strip().upper()

            except Exception:
                err_code = ""

            if (
                "QR_NOT_SAT_DOMAIN"
                in resp_text
                or err_code
                == "QR_NOT_SAT_DOMAIN"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ QR no válido para SAT"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "El QR no corresponde a un "
                            "enlace oficial del SAT."
                        ),
                    ),
                    instance_name=instance_name,
                )

            elif (
                "QR_NOT_READABLE"
                in resp_text
                or err_code
                == "QR_NOT_READABLE"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ No pudimos leer el QR"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "Envíalo más cerca, más nítido "
                            "y con buena luz."
                        ),
                    ),
                    instance_name=instance_name,
                )

            elif (
                "MIME_NOT_SUPPORTED"
                in resp_text
                or err_code
                == "MIME_NOT_SUPPORTED"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ Archivo no compatible"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "Ese tipo de archivo aún no "
                            "es compatible.\n"
                            "Envíalo como imagen."
                        ),
                        include_identity=False,
                    ),
                    instance_name=instance_name,
                )

            # ==========================================
            # RFC_VERIF4_ACCEPT_PROVIDER_RESULT_V1
            #
            # REGLA ROBERTO / VERIF4:
            # si el proveedor ya entregó RFC + IDCIF,
            # esos datos se entregan y contabilizan aunque
            # la validación posterior del SAT reporte:
            # sin CIF, sin régimen, suspendido, cancelado,
            # inactivo o datos fiscales incompletos.
            # ==========================================
            elif (
                is_verifiable
                and (
                    str(
                        job_data.get(
                            "verifiable_provider_db_name"
                        )
                        or ""
                    ).strip().upper()
                    == "RFC_VERIFIABLE_VERIF4"
                    or str(
                        job_data.get(
                            "verifiable_provider_code"
                        )
                        or ""
                    ).strip().upper()
                    == "VERIF4"
                    or str(
                        job_data.get(
                            "verifiable_provider_name"
                        )
                        or ""
                    ).strip().upper()
                    == "ID ROBERTO LENTO"
                )
                and str(
                    job_data.get("provider_rfc")
                    or ""
                ).strip()
                and str(
                    job_data.get("provider_idcif")
                    or ""
                ).strip()
                and err_code in {
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
                }
            ):
                delivered_rfc = str(
                    job_data.get("provider_rfc")
                    or ""
                ).strip().upper()

                delivered_idcif = str(
                    job_data.get("provider_idcif")
                    or ""
                ).strip()

                delivery_item_key = (
                    "VERIFICABLE_PROVIDER_ACCEPTED:"
                    f"{delivered_rfc}:"
                    f"{delivered_idcif}"
                )

                (
                    delivery_claimed,
                    delivery_lock_key,
                    delivery_done_key,
                ) = claim_delivery_once(
                    job_data,
                    item_key=delivery_item_key,
                    repair_verifiable_after_done=True,
                )

                if not delivery_claimed:
                    print(
                        "[RFC VERIF4 RESULT ALREADY DELIVERED]",
                        {
                            "request_key":
                                verifiable_request_key,
                            "rfc": delivered_rfc,
                            "idcif": delivered_idcif,
                        },
                        flush=True,
                    )
                    return

                try:
                    evolution_send_text_to_group(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title=(
                                "✅ RFC verificable localizado"
                            ),
                            requester_label=(
                                requester_label
                            ),
                            body=(
                                f"_RFC localizado_: "
                                f"*{delivered_rfc}*\n"
                                f"_IDCIF_: "
                                f"*{delivered_idcif}*"
                            ),
                        ),
                        instance_name=instance_name,
                    )

                    verifiable_pdf_text_fallback_sent = True

                    success_recorded = (
                        record_success_once(
                            job_data,
                            group_jid,
                            group_name,
                            kind="RFC_VERIFICABLE",
                            count=1,
                            item_key=delivery_item_key,
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

                    completion_marked = (
                        mark_verifiable_completed_24h(
                            job_data
                        )
                    )

                    if not completion_marked:
                        raise RuntimeError(
                            "RFC_VERIFICABLE_"
                            "COMPLETED_24H_MARK_FAILED"
                        )

                    from app.verifiable_flow import (
                        finish_pending
                        as _finish_pending_verif4
                    )

                    _finish_pending_verif4(
                        verifiable_request_key,
                        provider_message_id=(
                            job_data.get(
                                "provider_request_msg_id"
                            )
                            or ""
                        ),
                    )

                    print(
                        "[RFC VERIF4 PROVIDER RESULT ACCEPTED]",
                        {
                            "request_key":
                                verifiable_request_key,
                            "rfc": delivered_rfc,
                            "idcif": delivered_idcif,
                            "sat_error_ignored":
                                err_code,
                            "success_recorded":
                                success_recorded,
                        },
                        flush=True,
                    )

                    return

                except Exception:
                    release_delivery_claim(
                        delivery_lock_key
                    )
                    raise

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
                    client_reason_map[
                        err_code
                    ]
                )

                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ Constancia no generada"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            f"{client_reason}.\n"
                            "No se generó la constancia."
                        ),
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
                        "error_code":
                            err_code,
                        "client_group":
                            group_jid,
                        "provider_group": (
                            job_data.get(
                                "verifiable_provider_group"
                            )
                        ),
                        "provider_rfc": (
                            job_data.get(
                                "provider_rfc"
                            )
                        ),
                        "provider_idcif": (
                            job_data.get(
                                "provider_idcif"
                            )
                        ),
                    },
                    flush=True,
                )
                _finalize_verifiable_terminal_rejection(
                    job_data
                )

            elif (
                "CLIENT_CURP_NOT_FOUND_OR_WRONG"
                in resp_text
                or err_code
                == "CLIENT_CURP_NOT_FOUND_OR_WRONG"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ CURP no localizada"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "Verifica que esté escrita "
                            "correctamente y vuelve a enviarla."
                        ),
                    ),
                    instance_name=instance_name,
                )

            elif (
                "CLIENT_RFC_NOT_FOUND_OR_WRONG"
                in resp_text
                or err_code
                == "CLIENT_RFC_NOT_FOUND_OR_WRONG"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ RFC no localizado"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "Verifica que esté escrito "
                            "correctamente y vuelve a enviarlo."
                        ),
                    ),
                    instance_name=instance_name,
                )

            elif (
                "CLIENT_CHECKID_INCOMPLETE_DATA_CLON_REQUIRED"
                in resp_text
                or err_code.startswith(
                    "CLIENT_CHECKID_"
                    "INCOMPLETE_DATA_CLON_REQUIRED"
                )
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ Información incompleta"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "Se encontró información, pero "
                            "está incompleta.\n"
                            "No se generó el documento para "
                            "evitar entregar datos incorrectos."
                        ),
                    ),
                    instance_name=instance_name,
                )

            elif (
                "CLIENT_CHECKID_INCOMPLETE_DATA"
                in resp_text
                or err_code
                == "CLIENT_CHECKID_INCOMPLETE_DATA"
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ Información incompleta"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "Se encontró información, pero "
                            "está incompleta.\n"
                            "No se generó el documento para "
                            "evitar entregar datos incorrectos.\n"
                            "Verifica la CURP/RFC o intenta "
                            "nuevamente más tarde."
                        ),
                    ),
                    instance_name=instance_name,
                )

            elif (
                "FALTA APELLIDO PATERNO"
                in err_code
                or "FALTA APELLIDO PATERNO"
                in resp_text.upper()
                or "FALTA APELLIDO MATERNO"
                in err_code
                or "FALTA APELLIDO MATERNO"
                in resp_text.upper()
                or "FALTA NOMBRE"
                in err_code
                or "FALTA NOMBRE"
                in resp_text.upper()
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ CURP sin información suficiente"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "No se encontró información "
                            "suficiente para esta CURP.\n"
                            "Verifica que esté escrita "
                            "correctamente o que se encuentre "
                            "certificada."
                        ),
                    ),
                    instance_name=instance_name,
                )

            elif (
                "GOB_CURP_FAIL:TIMEOUTEXCEPTION"
                in err_code
                or "GOB_CURP_FAIL:TIMEOUTEXCEPTION"
                in resp_text.upper()
            ):
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ Consulta de CURP "
                            "sin respuesta"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "El servicio de consulta no "
                            "respondió a tiempo.\n"
                            "La CURP no fue marcada como "
                            "inexistente.\n"
                            "Intenta nuevamente en unos "
                            "momentos."
                        ),
                    ),
                    instance_name=instance_name,
                )

            else:
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ No pudimos completar "
                            "la solicitud"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "Ocurrió una interrupción temporal.\n"
                            "Intenta nuevamente en 2-3 minutos."
                        ),
                    ),
                    instance_name=instance_name,
                )

        except Exception as notice_exc:
            print(
                "[RFC HTTP CLIENT NOTICE ERROR]",
                repr(notice_exc),
                flush=True,
            )

        # Si era transitorio y ya no quedan retries,
        # mantener el job como FAILED.
        if transient_http_error:
            raise

    except Exception as e:
        print(
            "process_group_request_job error:",
            repr(e),
            flush=True,
        )
        traceback.print_exc()

        retry_remaining = (
            _rq_retry_remaining()
        )

        if retry_remaining > 0:
            keep_inflight_for_retry = True

            print(
                "[RFC JOB EXCEPTION - RQ RETRY]",
                {
                    "retry_remaining":
                        retry_remaining,
                    "error":
                        repr(e),
                    "inflight_retained":
                        inflight_key,
                },
                flush=True,
            )

            raise

        if not verifiable_pdf_text_fallback_sent:
            try:
                evolution_send_text_to_group(
                    group_jid,
                    _job_client_message(
                        job_data,
                        title=(
                            "⚠️ No pudimos completar "
                            "la solicitud"
                        ),
                        requester_label=(
                            requester_label
                        ),
                        body=(
                            "Ocurrió una interrupción temporal.\n"
                            "Intenta nuevamente en 2-3 minutos."
                        ),
                    ),
                    instance_name=instance_name,
                )

            except Exception as notice_exc:
                print(
                    "[RFC FINAL CLIENT NOTICE ERROR]",
                    repr(notice_exc),
                    flush=True,
                )

        # No quedan retries.
        # El job queda FAILED.
        raise

    finally:
        reservation_key = str(
            job_data.get("_rfc_quota_reservation_key")
            or ""
        ).strip()

        if (
            reservation_key
            and not bool(job_data.get("_rfc_quota_committed"))
        ):
            if keep_inflight_for_retry:
                print(
                    "[RFC QUOTA RESERVATION RETAINED FOR RETRY]",
                    reservation_key,
                    flush=True,
                )
            else:
                try:
                    _rfc_release_quota_reservation(job_data)
                except Exception as quota_release_exc:
                    print(
                        "[RFC QUOTA RESERVATION RELEASE ERROR]",
                        repr(quota_release_exc),
                        flush=True,
                    )

        if keep_inflight_for_retry:
            print(
                "[RFC REQUEST INFLIGHT RETAINED FOR RETRY]",
                {
                    "inflight_key":
                        inflight_key,
                    "retry_remaining":
                        _rq_retry_remaining(),
                },
                flush=True,
            )

        else:
            release_request_inflight(
                inflight_key
            )

        if (
            is_verifiable
            and verifiable_request_key
        ):
            if keep_inflight_for_retry:
                print(
                    "[RFC VERIFIABLE RESULT CLAIM "
                    "RETAINED FOR RETRY]",
                    {
                        "request_key":
                            verifiable_request_key,
                        "retry_remaining":
                            _rq_retry_remaining(),
                    },
                    flush=True,
                )

            else:
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
                        "[RFC VERIFIABLE RESULT CLAIM "
                        "RELEASE ERROR]",
                        repr(claim_release_exc),
                        flush=True,
                    )

def process_rfc_batch_child_job(
    child_payload: dict,
):
    """
    Ejecuta un hijo de un mensaje multilínea
    fuera del webhook HTTP.

    DOCIFY_BATCH_RQ_RETRY_WRAPPER_V1
    """

    import asyncio
    import os

    from app.rfc_webhook import (
        _RFCBatchSyntheticRequest,
        evolution_rfc_webhook,
    )

    result = asyncio.run(
        evolution_rfc_webhook(
            _RFCBatchSyntheticRequest(
                child_payload
            )
        )
    )

    if (
        isinstance(result, dict)
        and result.get("error")
        == (
            "verifiable_provider_"
            "transient_unavailable"
        )
    ):
        print(
            "RFC_BATCH_CHILD_TRANSIENT_"
            "PROVIDER_RETRY =",
            {
                "pid": os.getpid(),
                "forced_code": (
                    result.get(
                        "forced_code"
                    )
                    or ""
                ),
                "retries_left": (
                    result.get(
                        "retries_left"
                    )
                    or 0
                ),
            },
            flush=True,
        )

        raise RuntimeError(
            "RFC_BATCH_CHILD_TRANSIENT_"
            "PROVIDER_UNAVAILABLE:"
            + str(
                result.get(
                    "forced_code"
                )
                or ""
            )
        )

    return result


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
                # RFC_REQUEST_LOG_VERIFIABLE_PROVIDER_V3
                #
                # El detalle Roberto / Isaac es EXCLUSIVO
                # de DOCIFY MX (docifybot8mx).
                #
                # Todos los demás bots conservan exactamente
                # el formato histórico:
                # provider_name=RFC_VERIFICABLE
                # provider_group_id=RFC
                "provider_name": (
                    (
                        job_data.get(
                            "verifiable_provider_name"
                        )
                        or job_data.get(
                            "verifiable_provider_db_name"
                        )
                        or "RFC_VERIFICABLE"
                    )
                    if (
                        act_type == "RFC_VERIFICABLE"
                        and str(
                            instance_name or ""
                        ).strip().lower()
                        == "docifybot8mx"
                    )
                    else (
                        "RFC_VERIFICABLE"
                        if act_type == "RFC_VERIFICABLE"
                        else "RFC"
                    )
                ),
                "provider_group_id": (
                    (
                        job_data.get(
                            "verifiable_provider_group"
                        )
                        or "RFC"
                    )
                    if (
                        act_type == "RFC_VERIFICABLE"
                        and str(
                            instance_name or ""
                        ).strip().lower()
                        == "docifybot8mx"
                    )
                    else "RFC"
                ),
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



def _rfc_quota_reservation_key(job_data: dict, family: str) -> str:
    execution_key = (
        job_data.get("execution_key")
        or job_data.get("msg_id")
        or job_data.get("request_key")
        or ""
    ).strip()
    instance = (
        job_data.get("evolution_instance")
        or job_data.get("instance_name")
        or EVOLUTION_INSTANCE
        or ""
    ).strip()
    group_jid = str(job_data.get("group_jid") or "").strip()
    requester = str(job_data.get("requester_number") or "").strip()
    query = re.sub(
        r"\s+",
        " ",
        str(job_data.get("query") or job_data.get("original_text") or "").strip().upper(),
    )

    if not execution_key:
        raise RuntimeError("RFC_QUOTA_RESERVATION_IDENTITY_EMPTY")

    raw = "|".join([
        instance,
        group_jid,
        requester,
        execution_key,
        family,
        query,
    ])
    return "rfc_quota:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _rfc_quota_ensure_table(conn):
    from sqlalchemy import text

    exists = conn.execute(
        text("SELECT to_regclass('public.rfc_quota_reservations')")
    ).scalar()

    if not exists:
        raise RuntimeError(
            "RFC_QUOTA_RESERVATIONS_TABLE_MISSING"
        )


def _rfc_try_reserve_quota(
    conn,
    *,
    job_data: dict,
    owner: str,
    instance_name: str,
    group_jid: str,
    family: str,
    count: int,
) -> tuple[bool, str]:
    """Reserva cupo ANTES de llamar proveedores o entregar archivos.

    CLON descuenta el saldo global al reservar y lo devuelve si el job termina
    sin éxito. Los límites por bot/grupo usan esta tabla para contar reservas
    activas sin inflar los contadores visibles de "used".
    """
    from datetime import datetime, timezone
    from sqlalchemy import text

    count = max(int(count or 1), 1)
    reservation_key = str(
        job_data.get(
            "_rfc_quota_reservation_key"
        )
        or ""
    ).strip()

    if not reservation_key:
        reservation_key = (
            _rfc_quota_reservation_key(
                job_data,
                family,
            )
        )

    _rfc_quota_ensure_table(conn)

    # Recupera reservas huérfanas de procesos terminados a la fuerza.
    # Los jobs RFC tienen timeout muy inferior a 2 horas.
    stale_rows = conn.execute(text("""
        SELECT
            reservation_key,
            owner_instance,
            family,
            count,
            wallet_clon_reserved
        FROM rfc_quota_reservations
        WHERE status = 'RESERVED'
          AND (
                (
                    family = 'VERIFICABLE'
                    AND updated_at < now() - interval '26 hours'
                )
                OR
                (
                    family <> 'VERIFICABLE'
                    AND updated_at < now() - interval '2 hours'
                )
              )
        FOR UPDATE SKIP LOCKED
    """)).mappings().all()

    for stale in stale_rows:
        if bool(stale.get("wallet_clon_reserved")):
            conn.execute(text("""
                UPDATE rfc_owner_wallets
                SET clon_balance = clon_balance + :count,
                    updated_at = now()
                WHERE owner_instance = :owner
            """), {
                "count": int(stale.get("count") or 1),
                "owner": stale.get("owner_instance"),
            })

        conn.execute(text("""
            UPDATE rfc_quota_reservations
            SET status = 'RELEASED', updated_at = now()
            WHERE reservation_key = :reservation_key
              AND status = 'RESERVED'
        """), {
            "reservation_key": stale.get("reservation_key"),
        })

    existing = conn.execute(text("""
        SELECT status
        FROM rfc_quota_reservations
        WHERE reservation_key = :reservation_key
        FOR UPDATE
    """), {"reservation_key": reservation_key}).mappings().first()

    if existing and str(existing.get("status") or "").upper() in {"RESERVED", "COMMITTED"}:
        job_data["_rfc_quota_reservation_key"] = reservation_key
        job_data["_rfc_quota_family"] = family
        return True, "EXISTING"

    # Serializa el recurso lógico para que dos workers no puedan reservar el
    # último cupo al mismo tiempo.
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"), {
        "lock_key": f"RFC_QUOTA|{owner}|{instance_name}|{group_jid}|{family}",
    })

    wallet = conn.execute(text("""
        SELECT clon_balance, idcif_enabled, idcif_expires_at
        FROM rfc_owner_wallets
        WHERE owner_instance = :owner
        LIMIT 1
        FOR UPDATE
    """), {"owner": owner}).mappings().first()

    bot = conn.execute(text("""
        SELECT
            COALESCE(clon_limit, 0) AS clon_limit,
            COALESCE(clon_used, 0) AS clon_used,
            COALESCE(idcif_limit, 0) AS idcif_limit,
            COALESCE(idcif_used, 0) AS idcif_used,
            COALESCE(verifiable_limit, 0) AS verifiable_limit,
            COALESCE(verifiable_used, 0) AS verifiable_used,
            COALESCE(verifiable_enabled, FALSE) AS verifiable_enabled,
            is_active,
            is_blocked
        FROM bot_control
        WHERE instance_name = :instance_name
        LIMIT 1
        FOR UPDATE
    """), {"instance_name": instance_name}).mappings().first()

    if not wallet or not bot or not bool(bot.get("is_active")) or bool(bot.get("is_blocked")):
        return False, "SERVICE_NOT_AVAILABLE"

    active_instance = int(conn.execute(text("""
        SELECT COALESCE(SUM(count), 0)
        FROM rfc_quota_reservations
        WHERE status = 'RESERVED'
          AND family = :family
          AND instance_name = :instance_name
          AND reservation_key <> :reservation_key
    """), {
        "family": family,
        "instance_name": instance_name,
        "reservation_key": reservation_key,
    }).scalar() or 0)

    group_promo = conn.execute(text("""
        SELECT
            id,
            COALESCE(clon_total, 0) AS clon_total,
            COALESCE(clon_used, 0) AS clon_used,
            COALESCE(idcif_total, 0) AS idcif_total,
            COALESCE(idcif_used, 0) AS idcif_used,
            COALESCE(verifiable_total, 0) AS verifiable_total,
            COALESCE(verifiable_used, 0) AS verifiable_used,
            COALESCE(shared_group_limit_verifiable, 0) AS shared_limit_verifiable,
            COALESCE(shared_group_used_verifiable, 0) AS shared_used_verifiable,
            COALESCE(shared_key, '') AS shared_key
        FROM group_promotions
        WHERE group_jid = :group_jid
          AND is_active = TRUE
        ORDER BY updated_at DESC NULLS LAST, id DESC
        LIMIT 1
        FOR UPDATE
    """), {"group_jid": group_jid}).mappings().first()

    active_group = int(conn.execute(text("""
        SELECT COALESCE(SUM(count), 0)
        FROM rfc_quota_reservations
        WHERE status = 'RESERVED'
          AND family = :family
          AND group_jid = :group_jid
          AND reservation_key <> :reservation_key
    """), {
        "family": family,
        "group_jid": group_jid,
        "reservation_key": reservation_key,
    }).scalar() or 0)

    shared_key = ""

    if family == "CLON":
        if int(wallet.get("clon_balance") or 0) < count:
            return False, "GLOBAL_CLON_EXHAUSTED"

        bot_limit = int(bot.get("clon_limit") or 0)
        bot_used = int(bot.get("clon_used") or 0)
        if bot_limit > 0 and bot_used + active_instance + count > bot_limit:
            return False, "BOT_CLON_LIMIT"

        if group_promo:
            total = int(group_promo.get("clon_total") or 0)
            used = int(group_promo.get("clon_used") or 0)
            if total > 0 and used + active_group + count > total:
                return False, "GROUP_CLON_LIMIT"

        updated = conn.execute(text("""
            UPDATE rfc_owner_wallets
            SET clon_balance = clon_balance - :count,
                updated_at = now()
            WHERE owner_instance = :owner
              AND clon_balance >= :count
        """), {"owner": owner, "count": count})
        if updated.rowcount != 1:
            return False, "GLOBAL_CLON_EXHAUSTED"

        wallet_clon_reserved = True

    elif family == "IDCIF":
        if not bool(wallet.get("idcif_enabled")):
            return False, "IDCIF_DISABLED"

        expires_at = wallet.get("idcif_expires_at")
        if not expires_at:
            return False, "IDCIF_NOT_ACTIVE"

        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        if expires_at <= datetime.now(timezone.utc):
            return False, "IDCIF_EXPIRED"

        bot_limit = int(bot.get("idcif_limit") or 0)
        bot_used = int(bot.get("idcif_used") or 0)
        if bot_limit > 0 and bot_used + active_instance + count > bot_limit:
            return False, "BOT_IDCIF_LIMIT"

        if group_promo:
            total = int(group_promo.get("idcif_total") or 0)
            used = int(group_promo.get("idcif_used") or 0)
            if total > 0 and used + active_group + count > total:
                return False, "GROUP_IDCIF_LIMIT"

        wallet_clon_reserved = False

    elif family == "VERIFICABLE":
        if not bool(bot.get("verifiable_enabled")):
            return False, "VERIFIABLE_DISABLED"

        bot_limit = int(bot.get("verifiable_limit") or 0)
        bot_used = int(bot.get("verifiable_used") or 0)
        if bot_limit > 0 and bot_used + active_instance + count > bot_limit:
            return False, "BOT_VERIFIABLE_LIMIT"

        if group_promo:
            total = int(group_promo.get("verifiable_total") or 0)
            used = int(group_promo.get("verifiable_used") or 0)
            if total > 0 and used + active_group + count > total:
                return False, "GROUP_VERIFIABLE_LIMIT"

            shared_key = str(group_promo.get("shared_key") or "").strip()
            shared_limit = int(group_promo.get("shared_limit_verifiable") or 0)
            shared_used = int(group_promo.get("shared_used_verifiable") or 0)

            if shared_key and shared_limit > 0:
                conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"), {
                    "lock_key": f"RFC_QUOTA_SHARED|{shared_key}|VERIFICABLE",
                })
                active_shared = int(conn.execute(text("""
                    SELECT COALESCE(SUM(count), 0)
                    FROM rfc_quota_reservations
                    WHERE status = 'RESERVED'
                      AND family = 'VERIFICABLE'
                      AND shared_key = :shared_key
                      AND reservation_key <> :reservation_key
                """), {
                    "shared_key": shared_key,
                    "reservation_key": reservation_key,
                }).scalar() or 0)
                if shared_used + active_shared + count > shared_limit:
                    return False, "SHARED_VERIFIABLE_LIMIT"

        wallet_clon_reserved = False

    else:
        return True, "UNKNOWN_NO_RESERVATION"

    conn.execute(text("""
        INSERT INTO rfc_quota_reservations (
            reservation_key,
            family,
            owner_instance,
            instance_name,
            group_jid,
            shared_key,
            count,
            wallet_clon_reserved,
            status,
            created_at,
            updated_at
        ) VALUES (
            :reservation_key,
            :family,
            :owner,
            :instance_name,
            :group_jid,
            :shared_key,
            :count,
            :wallet_clon_reserved,
            'RESERVED',
            now(),
            now()
        )
        ON CONFLICT (reservation_key)
        DO UPDATE SET
            status = 'RESERVED',
            shared_key = EXCLUDED.shared_key,
            count = EXCLUDED.count,
            wallet_clon_reserved = EXCLUDED.wallet_clon_reserved,
            updated_at = now()
    """), {
        "reservation_key": reservation_key,
        "family": family,
        "owner": owner,
        "instance_name": instance_name,
        "group_jid": group_jid,
        "shared_key": shared_key,
        "count": count,
        "wallet_clon_reserved": wallet_clon_reserved,
    })

    job_data["_rfc_quota_reservation_key"] = reservation_key
    job_data["_rfc_quota_family"] = family

    print("[RFC QUOTA RESERVED]", {
        "reservation_key": reservation_key,
        "family": family,
        "instance_name": instance_name,
        "group_jid": group_jid,
        "count": count,
    }, flush=True)

    return True, "RESERVED"


def _rfc_release_quota_reservation(job_data: dict) -> bool:
    reservation_key = str(job_data.get("_rfc_quota_reservation_key") or "").strip()
    if not reservation_key:
        return False

    from sqlalchemy import text

    engine = _rfc_final_engine()
    with engine.begin() as conn:
        _rfc_quota_ensure_table(conn)
        row = conn.execute(text("""
            SELECT reservation_key, owner_instance, count, wallet_clon_reserved, status
            FROM rfc_quota_reservations
            WHERE reservation_key = :reservation_key
            FOR UPDATE
        """), {"reservation_key": reservation_key}).mappings().first()

        if not row or str(row.get("status") or "").upper() != "RESERVED":
            return False

        if bool(row.get("wallet_clon_reserved")):
            conn.execute(text("""
                UPDATE rfc_owner_wallets
                SET clon_balance = clon_balance + :count,
                    updated_at = now()
                WHERE owner_instance = :owner
            """), {
                "count": int(row.get("count") or 1),
                "owner": row.get("owner_instance"),
            })

        conn.execute(text("""
            UPDATE rfc_quota_reservations
            SET status = 'RELEASED', updated_at = now()
            WHERE reservation_key = :reservation_key
              AND status = 'RESERVED'
        """), {"reservation_key": reservation_key})

    print("[RFC QUOTA RELEASED]", reservation_key, flush=True)
    return True


def _rfc_mark_quota_committed(conn, reservation_key: str) -> bool:
    from sqlalchemy import text
    if not reservation_key:
        return False
    _rfc_quota_ensure_table(conn)
    result = conn.execute(text("""
        UPDATE rfc_quota_reservations
        SET status = 'COMMITTED', updated_at = now()
        WHERE reservation_key = :reservation_key
          AND status IN ('RESERVED', 'COMMITTED')
    """), {"reservation_key": reservation_key})
    return bool(result.rowcount)



def _rfc_safe_limit_notice(
    group_jid,
    message,
    instance_name=None,
    **kwargs,
):
    """
    Aviso operativo que nunca debe alterar
    la decision de cuota por un fallo de WhatsApp.
    """
    try:
        evolution_send_text_to_group(
            group_jid,
            message,
            instance_name=instance_name,
            **kwargs,
        )
        return True
    except Exception as notice_exc:
        print(
            "[RFC_LIMIT_NOTICE_SEND_ERROR]",
            {
                "group_jid": group_jid,
                "instance_name": instance_name,
                "error": repr(notice_exc),
            },
            flush=True,
        )
        return False



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
                _rfc_safe_limit_notice(
                    group_jid,
                    _job_client_message(
                            job_data,
                            title='⚠️ Servicio no disponible',
                            requester_label=requester_label,
                            body='No fue posible validar la configuración del servicio RFC.',
                            family="service",
                            status='NO CONFIGURADO',
                        ),
                    instance_name=instance_name
                )
                return False

            if not bool(bot.get("is_active")):
                _rfc_safe_limit_notice(
                    group_jid,
                    _job_client_message(
                            job_data,
                            title='⚠️ Servicio no disponible',
                            requester_label=requester_label,
                            body='El servicio está temporalmente inactivo.',
                            family="service",
                            status='INACTIVO',
                        ),
                    instance_name=instance_name
                )
                return False

            if bool(bot.get("is_blocked")):
                _rfc_safe_limit_notice(
                    group_jid,
                    _job_client_message(
                            job_data,
                            title='⚠️ Servicio no disponible',
                            requester_label=requester_label,
                            body='El servicio está temporalmente bloqueado.',
                            family="service",
                            status='BLOQUEADO',
                        ),
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
                        _rfc_safe_limit_notice(
                            group_jid,
                            _job_client_message(
                            job_data,
                            title='⚠️ RFC CLON no disponible',
                            requester_label=requester_label,
                            body='Este grupo ya no tiene RFC CLON disponibles.',
                            family="service",
                            status='LÍMITE ALCANZADO',
                        ),
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
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC CLON no disponible',
                            requester_label=requester_label,
                            body='No hay RFC CLON disponibles en este momento.',
                            family="service",
                            status='NO DISPONIBLE',
                        ),
                        instance_name=instance_name
                    )
                    return False

                # clon_limit = 0 significa ilimitado para ese bot, pero siempre sujeto al saldo global.
                if clon_limit > 0 and clon_used >= clon_limit:
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC CLON no disponible',
                            requester_label=requester_label,
                            body='Se alcanzó el límite disponible de RFC CLON.',
                            family="service",
                            status='LÍMITE ALCANZADO',
                        ),
                        instance_name=instance_name
                    )
                    return False

                reserved, reserve_reason = _rfc_try_reserve_quota(
                    conn,
                    job_data=job_data,
                    owner=owner,
                    instance_name=instance_name,
                    group_jid=group_jid,
                    family=family,
                    count=count,
                )
                if not reserved:
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC CLON no disponible',
                            requester_label=requester_label,
                            body='El último cupo disponible fue tomado por otra solicitud. Intenta nuevamente.',
                            family="service",
                            status='LÍMITE ALCANZADO',
                        ),
                        instance_name=instance_name,
                    )
                    print("[RFC_CLON_ATOMIC_RESERVATION_REJECTED]", reserve_reason, flush=True)
                    return False

                print("[RFC_CLON_BOT_AND_GLOBAL_CHECK_OK]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "global_balance": global_balance,
                    "clon_limit": clon_limit,
                    "clon_used": clon_used,
                    "kind": kind,
                    "reservation": reserve_reason,
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
                        _rfc_safe_limit_notice(
                            group_jid,
                            _job_client_message(
                            job_data,
                            title='⚠️ RFC IDCIF no disponible',
                            requester_label=requester_label,
                            body='Este grupo ya no tiene RFC IDCIF disponibles.',
                            family="service",
                            status='LÍMITE ALCANZADO',
                        ),
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
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC IDCIF no disponible',
                            requester_label=requester_label,
                            body='El servicio RFC IDCIF no está activo actualmente.',
                            family="service",
                            status='INACTIVO',
                        ),
                        instance_name=instance_name
                    )
                    return False

                expires_at = wallet.get("idcif_expires_at")
                if not expires_at:
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC IDCIF no disponible',
                            requester_label=requester_label,
                            body='El servicio RFC IDCIF no tiene una vigencia activa.',
                            family="service",
                            status='NO VIGENTE',
                        ),
                        instance_name=instance_name
                    )
                    return False

                now = datetime.now(timezone.utc)
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)

                if expires_at <= now:
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC IDCIF no disponible',
                            requester_label=requester_label,
                            body='La vigencia del servicio RFC IDCIF ha finalizado.',
                            family="service",
                            status='NO VIGENTE',
                        ),
                        instance_name=instance_name
                    )
                    return False

                # idcif_limit = 0 significa ilimitado para ese bot, pero siempre sujeto al plan semanal global.
                if idcif_limit > 0 and idcif_used >= idcif_limit:
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC IDCIF no disponible',
                            requester_label=requester_label,
                            body='Se alcanzó el límite disponible de RFC IDCIF.',
                            family="service",
                            status='LÍMITE ALCANZADO',
                        ),
                        instance_name=instance_name
                    )
                    return False

                reserved, reserve_reason = _rfc_try_reserve_quota(
                    conn,
                    job_data=job_data,
                    owner=owner,
                    instance_name=instance_name,
                    group_jid=group_jid,
                    family=family,
                    count=count,
                )
                if not reserved:
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC IDCIF no disponible',
                            requester_label=requester_label,
                            body='El último cupo disponible fue tomado por otra solicitud. Intenta nuevamente.',
                            family="service",
                            status='LÍMITE ALCANZADO',
                        ),
                        instance_name=instance_name,
                    )
                    print("[RFC_IDCIF_ATOMIC_RESERVATION_REJECTED]", reserve_reason, flush=True)
                    return False

                print("[RFC_IDCIF_BOT_AND_GLOBAL_CHECK_OK]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "idcif_limit": idcif_limit,
                    "idcif_used": idcif_used,
                    "expires_at": str(expires_at),
                    "kind": kind,
                    "reservation": reserve_reason,
                }, flush=True)

                return True

            if family == "VERIFICABLE":
                if not bool(
                    bot.get("verifiable_enabled")
                ):
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC verificable no disponible',
                            requester_label=requester_label,
                            body='El servicio RFC verificable no está activo actualmente.',
                            family="service",
                            status='INACTIVO',
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
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC verificable no disponible',
                            requester_label=requester_label,
                            body='No hay RFC verificables disponibles en este momento.',
                            family="service",
                            status='LÍMITE ALCANZADO',
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
                        _rfc_safe_limit_notice(
                            group_jid,
                            _job_client_message(
                            job_data,
                            title='⚠️ RFC verificable no disponible',
                            requester_label=requester_label,
                            body='Este grupo no tiene RFC verificables asignados.',
                            family="service",
                            status='NO DISPONIBLE',
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
                        _rfc_safe_limit_notice(
                            group_jid,
                            _job_client_message(
                            job_data,
                            title='⚠️ RFC verificable no disponible',
                            requester_label=requester_label,
                            body='Este grupo ya no tiene RFC verificables disponibles.',
                            family="service",
                            status='LÍMITE ALCANZADO',
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
                        _rfc_safe_limit_notice(
                            group_jid,
                            _job_client_message(
                            job_data,
                            title='⚠️ RFC verificable no disponible',
                            requester_label=requester_label,
                            body='Este grupo alcanzó su límite de RFC verificables.',
                            family="service",
                            status='LÍMITE ALCANZADO',
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

                reserved, reserve_reason = _rfc_try_reserve_quota(
                    conn,
                    job_data=job_data,
                    owner=owner,
                    instance_name=instance_name,
                    group_jid=group_jid,
                    family=family,
                    count=count,
                )
                if not reserved:
                    _rfc_safe_limit_notice(
                        group_jid,
                        _job_client_message(
                            job_data,
                            title='⚠️ RFC verificable no disponible',
                            requester_label=requester_label,
                            body='El último cupo disponible fue tomado por otra solicitud. Intenta nuevamente.',
                            family="service",
                            status='LÍMITE ALCANZADO',
                        ),
                        instance_name=instance_name,
                    )
                    print("[RFC_VERIFICABLE_ATOMIC_RESERVATION_REJECTED]", reserve_reason, flush=True)
                    return False

                print("[RFC_VERIFICABLE_ATOMIC_RESERVATION_OK]", reserve_reason, flush=True)
                return True

        return True

    except Exception as e:
        print("[RFC_FINAL_BOT_LIMIT_CHECK_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
        }, flush=True)
        try:
            _rfc_safe_limit_notice(
                group_jid,
                _job_client_message(
                            job_data,
                            title='⚠️ Error validando saldo/límite RFC',
                            requester_label=requester_label,
                            body='No fue posible validar el saldo/límite RFC. Intenta nuevamente.',
                            family="service",
                            status='ERROR',
                        ),
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
        reservation_key = str(
            job_data.get("_rfc_quota_reservation_key")
            or ""
        ).strip()

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
                if reservation_key:
                    # El saldo ya quedó apartado atómicamente ANTES de procesar.
                    conn.execute(text("""
                        UPDATE rfc_owner_wallets
                        SET
                            clon_used = clon_used + :count,
                            updated_at = now()
                        WHERE owner_instance = :owner
                    """), {"count": count, "owner": owner})
                else:
                    # Compatibilidad defensiva con jobs creados antes del despliegue.
                    wallet_result = conn.execute(text("""
                        UPDATE rfc_owner_wallets
                        SET
                            clon_balance = clon_balance - :count,
                            clon_used = clon_used + :count,
                            updated_at = now()
                        WHERE owner_instance = :owner
                          AND clon_balance >= :count
                    """), {"count": count, "owner": owner})
                    if wallet_result.rowcount != 1:
                        raise RuntimeError("RFC_CLON_GLOBAL_CONSUME_FAILED")

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

            if reservation_key:
                if not _rfc_mark_quota_committed(
                    conn,
                    reservation_key,
                ):
                    raise RuntimeError(
                        "RFC_QUOTA_RESERVATION_COMMIT_FAILED"
                    )

        # Llegar aquí significa que engine.begin() salió correctamente
        # y PostgreSQL confirmó toda la transacción comercial + reserva.
        try:
            _rfc_clear_panel_cache_v2()
        except Exception:
            pass

        job_data["_rfc_quota_committed"] = True
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
    # ============================================================
    # RFC VERIFICABLE YA RESERVADO ANTES DE IR AL PROVEEDOR
    #
    # La solicitud verificable ya pasó el control comercial y
    # reservó cuota ANTES de enviarse al proveedor.
    #
    # Cuando vuelve RFC + IDCIF no debe intentar reservar/validar
    # nuevamente esa misma cuota, porque puede rechazarse contra
    # su propia reserva y dejar un pending fantasma.
    # ============================================================

    if str(kind or "").strip().upper() == "RFC_VERIFICABLE":
        reservation_key = str(
            job_data.get("_rfc_quota_reservation_key")
            or ""
        ).strip()

        reservation_family = str(
            job_data.get("_rfc_quota_family")
            or ""
        ).strip().upper()

        if (
            reservation_key
            and reservation_family == "VERIFICABLE"
        ):
            try:
                from sqlalchemy import text

                engine = _rfc_final_engine()

                with engine.begin() as conn:
                    _rfc_quota_ensure_table(conn)

                    row = conn.execute(
                        text("""
                            SELECT
                                reservation_key,
                                family,
                                instance_name,
                                group_jid,
                                status
                            FROM rfc_quota_reservations
                            WHERE reservation_key = :reservation_key
                            LIMIT 1
                        """),
                        {
                            "reservation_key":
                                reservation_key
                        },
                    ).mappings().first()

                if row:
                    status = str(
                        row.get("status") or ""
                    ).strip().upper()

                    db_family = str(
                        row.get("family") or ""
                    ).strip().upper()

                    db_instance = str(
                        row.get("instance_name") or ""
                    ).strip()

                    db_group = str(
                        row.get("group_jid") or ""
                    ).strip()

                    if (
                        status == "RELEASED"
                        and db_family == "VERIFICABLE"
                        and db_instance == str(instance_name or "").strip()
                        and db_group == str(group_jid or "").strip()
                    ):
                        # El proveedor ya respondió con resultado válido,
                        # pero el timeout previo liberó la reserva.
                        #
                        # Revivimos ESA MISMA reserva para que el flujo
                        # normal pueda entregar y después marcarla
                        # COMMITTED.
                        with engine.begin() as conn:
                            _rfc_quota_ensure_table(conn)

                            revived = conn.execute(
                                text("""
                                    UPDATE rfc_quota_reservations
                                    SET
                                        status = 'RESERVED',
                                        updated_at = now()
                                    WHERE
                                        reservation_key = :reservation_key
                                        AND family = 'VERIFICABLE'
                                        AND instance_name = :instance_name
                                        AND group_jid = :group_jid
                                        AND status = 'RELEASED'
                                """),
                                {
                                    "reservation_key":
                                        reservation_key,
                                    "instance_name":
                                        str(instance_name or "").strip(),
                                    "group_jid":
                                        str(group_jid or "").strip(),
                                },
                            )

                        if revived.rowcount == 1:
                            status = "RESERVED"

                            print(
                                "[RFC_VERIFICABLE_RELEASED_RESERVATION_REVIVED]",
                                {
                                    "reservation_key":
                                        reservation_key,
                                    "instance_name":
                                        instance_name,
                                    "group_jid":
                                        group_jid,
                                },
                                flush=True,
                            )

                    if (
                        status in ("RESERVED", "COMMITTED")
                        and db_family == "VERIFICABLE"
                        and db_instance == str(instance_name or "").strip()
                        and db_group == str(group_jid or "").strip()
                    ):
                        print(
                            "[RFC_VERIFICABLE_EXISTING_RESERVATION_OK]",
                            {
                                "reservation_key":
                                    reservation_key,
                                "status": status,
                                "instance_name":
                                    instance_name,
                                "group_jid":
                                    group_jid,
                            },
                            flush=True,
                        )

                        return True

                    print(
                        "[RFC_VERIFICABLE_EXISTING_RESERVATION_INVALID]",
                        {
                            "reservation_key":
                                reservation_key,
                            "status": status,
                            "family": db_family,
                            "instance_name":
                                db_instance,
                            "group_jid":
                                db_group,
                        },
                        flush=True,
                    )

                else:
                    print(
                        "[RFC_VERIFICABLE_EXISTING_RESERVATION_NOT_FOUND]",
                        reservation_key,
                        flush=True,
                    )

            except Exception as reservation_exc:
                print(
                    "[RFC_VERIFICABLE_EXISTING_RESERVATION_CHECK_ERROR]",
                    repr(reservation_exc),
                    flush=True,
                )

    return _rfc_final_check_global(
        job_data,
        group_jid,
        group_name,
        instance_name,
        kind,
    )


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