import os
import re
import json
import hashlib
import time

from datetime import timedelta

from fastapi import APIRouter, Request

from app.queue import request_queue
from app.services.evolution import send_text
from app.db import SessionLocal
from app.models import (
    AuthorizedGroup,
    BotControl,
    ProviderSetting,
)
from core_sat import (
    consultar_curp_bot,
    consultar_curp_nuevo_leon,
    calcular_rfc_moffin,
)

from app.verifiable_flow import (
    VERIFIABLE_PROVIDER_GROUP,
    VERIFIABLE_PROVIDER_INSTANCE,
    VERIFIABLE_TIMEOUT_SEC,
    parse_verifiable_request,
    extract_rfc_idcif,
    extract_rfc_idcif_pairs,
    extract_quoted_message_id,
    verifiable_request_key,
    save_pending,
    load_pending,
    associate_provider_message,
    request_key_from_provider_message,
    claim_provider_result,
    release_provider_result_claim,
    finish_pending,
    find_pending_request_by_provider_rfc,
    load_verifiable_providers,
    verifiable_provider_by_group,
    _near_curp_rfc_match,
    _near_rfc_match,
)

router = APIRouter()

MAIN_PANEL_INSTANCE = os.getenv("MAIN_PANEL_INSTANCE", "grupo02").strip()
GROUP_COMMAND = os.getenv("GROUP_COMMAND", "/csf").strip() or "/csf"
LOCALIZACIONES_PROVIDER_GROUP = (
    os.getenv(
        "LOCALIZACIONES_PROVIDER_GROUP",
        "120363409752881042@g.us",
    )
    or "120363409752881042@g.us"
).strip()

BLOCKED_GROUPS_KEY = "blocked_groups_no_response"

REQUEST_INFLIGHT_TTL_SEC = int(
    os.getenv(
        "REQUEST_INFLIGHT_TTL_SEC",
        "1200",
    ) or "1200"
)

DUPLICATE_NOTICE_TTL_SEC = int(
    os.getenv(
        "DUPLICATE_NOTICE_TTL_SEC",
        "60",
    ) or "60"
)

CURP_RE = re.compile(r"\b[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d\b", re.I)
RFC_RE = re.compile(r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b", re.I)
IDCIF_RE = re.compile(r"\b\d{11}\b")
SAT_QR_RE = re.compile(r"(?:D1=10|D3=)", re.I)


def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _upper(s: str) -> str:
    return _norm_text(s).upper()


def _curp_to_moffin_rfc_cached(
    curp: str
) -> str:
    curp = re.sub(
        r"\s+",
        "",
        str(curp or "")
    ).strip().upper()

    if not CURP_RE.fullmatch(curp):
        raise ValueError(
            f"CURP_MOFFIN_INVALIDA:{curp}"
        )

    redis_conn = (
        request_queue.connection
    )

    cache_key = (
        "rfc:moffin:curp:v2:"
        f"{curp}"
    )

    try:
        cached = redis_conn.get(
            cache_key
        )

        if isinstance(cached, bytes):
            cached = cached.decode(
                "utf-8",
                errors="ignore"
            )

        cached = str(
            cached or ""
        ).strip().upper()

        if (
            re.fullmatch(
                r"[A-ZÑ&]{4}"
                r"\d{6}"
                r"[A-Z0-9]{3}",
                cached
            )
            and cached[4:10]
            == curp[4:10]
        ):
            print(
                "[VERIFIABLE_MOFFIN_CACHE_HIT]",
                {
                    "curp": curp,
                    "rfc": cached,
                },
                flush=True,
            )

            return cached

    except Exception as cache_exc:
        print(
            "[VERIFIABLE_MOFFIN_CACHE_GET_FAIL]",
            repr(cache_exc),
            flush=True,
        )

    try:
        datos_curp = (
            consultar_curp_nuevo_leon(
                curp
            )
            or {}
        )
    
        print(
            "[VERIFIABLE_NL_CURP_OK]",
            {
                "curp": curp,
            },
            flush=True,
        )
    
    except Exception as nl_error:
        print(
            "[VERIFIABLE_NL_CURP_FAIL]",
            {
                "curp": curp,
                "error": repr(nl_error),
            },
            flush=True,
        )
    
        datos_curp = (
            consultar_curp_bot(
                curp
            )
            or {}
        )

    nombre = (
        datos_curp.get("NOMBRE")
        or ""
    ).strip()

    apellido_paterno = (
        datos_curp.get(
            "PRIMER_APELLIDO"
        )
        or ""
    ).strip()

    apellido_materno = (
        datos_curp.get(
            "SEGUNDO_APELLIDO"
        )
        or ""
    ).strip()

    fecha_nacimiento = (
        datos_curp.get(
            "FECHA_NACIMIENTO"
        )
        or ""
    ).strip()

    if (
        not apellido_paterno
        and apellido_materno
    ):
        apellido_paterno, apellido_materno = (
            apellido_materno,
            ""
        )

    if (
        not nombre
        or not apellido_paterno
        or not fecha_nacimiento
    ):
        raise RuntimeError(
            "VERIFIABLE_MOFFIN_"
            "CURP_DATA_INCOMPLETE:"
            f"curp={curp}"
        )

    rfc = calcular_rfc_moffin(
        nombre,
        apellido_paterno,
        apellido_materno,
        fecha_nacimiento
    ).strip().upper()

    if not re.fullmatch(
        r"[A-ZÑ&]{4}"
        r"\d{6}"
        r"[A-Z0-9]{3}",
        rfc
    ):
        raise RuntimeError(
            "VERIFIABLE_MOFFIN_"
            f"RFC_INVALID:{rfc}"
        )

    if rfc[4:10] != curp[4:10]:
        raise RuntimeError(
            "VERIFIABLE_MOFFIN_"
            "RFC_CURP_DATE_MISMATCH:"
            f"curp={curp}:"
            f"rfc={rfc}"
        )

    try:
        redis_conn.set(
            cache_key,
            rfc,
            ex=30 * 24 * 60 * 60
        )

    except Exception as cache_exc:
        print(
            "[VERIFIABLE_MOFFIN_CACHE_SET_FAIL]",
            repr(cache_exc),
            flush=True,
        )

    print(
        "[VERIFIABLE_CURP_TO_RFC_OK]",
        {
            "curp": curp,
            "rfc": rfc,
        },
        flush=True,
    )

    return rfc


def _is_group(jid: str) -> bool:
    return (jid or "").endswith("@g.us")


def _is_group_blocked(group_jid: str) -> bool:
    """
    Consulta el mismo set Redis que modifica el mini panel:
    blocked_groups_no_response

    No usa import desde app.main para evitar imports circulares.
    request_queue.connection usa el Redis configurado por esta misma app.
    """
    group_jid = (group_jid or "").strip()

    if not group_jid:
        return False

    redis_conn = request_queue.connection

    try:
        return bool(redis_conn.sismember(BLOCKED_GROUPS_KEY, group_jid))

    except Exception as e:
        # No permitir solicitudes si no es posible confirmar el bloqueo.
        # Es más seguro ignorar que cobrar/procesar un grupo posiblemente bloqueado.
        print(
            "RFC_GROUP_BLOCK_CHECK_ERROR =",
            {
                "group_jid": group_jid,
                "redis_key": BLOCKED_GROUPS_KEY,
                "error": repr(e),
            },
            flush=True,
        )
        raise


def _verifiable_group_config(
    db,
    group_jid: str,
    instance_name: str,
) -> dict:
    group_jid = str(
        group_jid or ""
    ).strip()

    instance_name = str(
        instance_name or ""
    ).strip()

    if not group_jid:
        return {
            "exists": False,
            "owned": False,
            "enabled": False,
            "owner_instance": "",
        }

    row = (
        db.query(AuthorizedGroup)
        .filter(
            AuthorizedGroup.group_jid
            == group_jid
        )
        .first()
    )

    if not row:
        return {
            "exists": False,
            "owned": False,
            "enabled": False,
            "owner_instance": "",
        }

    owner_instance = str(
        row.owner_instance or ""
    ).strip()

    owned = bool(
        owner_instance
        and instance_name
        and owner_instance == instance_name
    )

    return {
        "exists": True,
        "owned": owned,
        "enabled": bool(
            getattr(
                row,
                "verifiable_enabled",
                False,
            )
        ),
        "owner_instance": owner_instance,
    }


def _extract_text(message: dict, data: dict) -> str:
    if not isinstance(message, dict):
        message = {}

    return (
        message.get("conversation")
        or (message.get("extendedTextMessage") or {}).get("text")
        or (message.get("imageMessage") or {}).get("caption")
        or (message.get("documentMessage") or {}).get("caption")
        or data.get("text")
        or data.get("body")
        or ""
    )


def _extract_media(message: dict):
    if not isinstance(message, dict):
        return "", "", ""

    if "imageMessage" in message:
        img = message.get("imageMessage") or {}
        return "image", img.get("url") or "", img.get("mimetype") or "image/jpeg"

    if "documentMessage" in message:
        doc = message.get("documentMessage") or {}
        return "document", doc.get("url") or "", doc.get("mimetype") or ""

    return "", "", ""


def _parse_rfc_query(text: str, msg_type: str = "") -> dict:
    raw = text or ""
    up = _upper(raw)

    if up.startswith(GROUP_COMMAND.upper()):
        raw = raw[len(GROUP_COMMAND):].strip()
        up = _upper(raw)

    if SAT_QR_RE.search(raw):
        return {"ok": True, "type": "QR_TEXT", "query": raw.strip()}

    rfc = RFC_RE.search(up)
    idcif = IDCIF_RE.search(up)
    curp = CURP_RE.search(up)

    if rfc and idcif:
        return {
            "ok": True,
            "type": "RFC_IDCIF",
            "query": f"RFC: {rfc.group(0).upper()}\nIDCIF: {idcif.group(0)}",
        }

    if curp:
        return {"ok": True, "type": "CURP", "query": curp.group(0).upper()}

    if rfc:
        return {"ok": True, "type": "RFC_ONLY", "query": rfc.group(0).upper()}

    if msg_type in ("image", "document"):
        return {"ok": True, "type": msg_type.upper(), "query": ""}

    return {
        "ok": False,
        "type": "INVALID_INPUT",
        "error": (
            ""
        ),
    }


def _authorized_group_exists(db, group_jid: str, instance_name: str) -> bool:
    row = db.query(AuthorizedGroup).filter(AuthorizedGroup.group_jid == group_jid).first()
    if not row:
        return False

    owner = (getattr(row, "owner_instance", "") or "").strip()
    if owner and owner != instance_name:
        return False

    if bool(getattr(row, "is_hidden", False)):
        return False

    return True


def _upsert_authorized_group(db, group_jid: str, instance_name: str):
    row = db.query(AuthorizedGroup).filter(AuthorizedGroup.group_jid == group_jid).first()

    if not row:
        row = AuthorizedGroup(
            group_jid=group_jid,
            group_name=group_jid,
            owner_instance=instance_name,
        )
        db.add(row)
        db.flush()

    row.owner_instance = instance_name

    if not getattr(row, "group_name", None):
        row.group_name = group_jid

    if hasattr(row, "is_hidden"):
        row.is_hidden = False

    if hasattr(row, "hidden_in_main"):
        row.hidden_in_main = False

    db.commit()
    return row


def _dedupe_key(
    instance: str,
    remote_jid: str,
    requester: str,
    query: str,
    msg_id: str,
) -> str:
    """
    El mismo webhook conserva el mismo msg_id y se bloquea.

    Cuando el usuario vuelve a enviar la misma solicitud,
    WhatsApp genera otro msg_id y se permite procesarla.
    """
    normalized_query = re.sub(
        r"\s+",
        " ",
        (query or "").strip().upper(),
    )

    base = "|".join(
        [
            (instance or "").strip(),
            (remote_jid or "").strip(),
            (requester or "").strip(),
            normalized_query,
            (msg_id or "").strip(),
        ]
    )

    return hashlib.sha1(
        base.encode("utf-8")
    ).hexdigest()


def _bot_label_from_db(instance_name: str | None) -> str:
    inst = (instance_name or "").strip()

    if not inst:
        return "RFC"

    db = SessionLocal()
    try:
        row = (
            db.query(BotControl)
            .filter(BotControl.instance_name == inst)
            .first()
        )

        if row and (row.label or "").strip():
            return row.label.strip()

        return inst
    finally:
        db.close()


def _extract_sent_message_id(
    response: dict | None,
) -> str:
    response = response or {}

    key = response.get("key") or {}

    return str(
        key.get("id")
        or response.get("messageId")
        or response.get("id")
        or ""
    ).strip()


def _verifiable_bot_config(
    db,
    instance_name: str,
) -> dict:
    row = (
        db.query(BotControl)
        .filter(
            BotControl.instance_name
            == (instance_name or "").strip()
        )
        .first()
    )

    if not row:
        return {
            "exists": False,
            "enabled": False,
            "price": 0.0,
            "limit": 0,
            "used": 0,
            "is_active": False,
            "is_blocked": False,
        }

    return {
        "exists": True,

        "enabled": bool(
            getattr(
                row,
                "verifiable_enabled",
                False,
            )
        ),

        "price": float(
            getattr(
                row,
                "sale_price_verifiable",
                0,
            )
            or 0
        ),

        "limit": int(
            getattr(
                row,
                "verifiable_limit",
                0,
            )
            or 0
        ),

        "used": int(
            getattr(
                row,
                "verifiable_used",
                0,
            )
            or 0
        ),

        "is_active": bool(
            getattr(
                row,
                "is_active",
                True,
            )
        ),

        "is_blocked": bool(
            getattr(
                row,
                "is_blocked",
                False,
            )
        ),
    }


def _get_or_create_verifiable_provider_setting(
    db,
    provider: dict,
):
    db_name = (
        provider.get("db_name")
        or ""
    ).strip().upper()

    row = (
        db.query(ProviderSetting)
        .filter(
            ProviderSetting.provider_name
            == db_name
        )
        .first()
    )

    if row:
        return row

    row = ProviderSetting(
        provider_name=db_name,
        is_enabled=bool(
            provider.get(
                "default_enabled",
                True,
            )
        ),
        weight=float(
            provider.get(
                "default_weight",
                1,
            )
            or 0
        ),
    )

    db.add(row)
    db.commit()
    db.refresh(row)

    return row


def _verifiable_providers_runtime(
    db,
) -> list[dict]:
    result: list[dict] = []

    for provider in (
        load_verifiable_providers()
    ):
        setting = (
            _get_or_create_verifiable_provider_setting(
                db,
                provider,
            )
        )

        item = dict(provider)

        item["enabled"] = bool(
            setting.is_enabled
        )

        item["weight"] = float(
            setting.weight
            or 0
        )

        result.append(item)

    return result


def _choose_verifiable_provider(
    db,
) -> dict:
    import random

    providers = [
        provider
        for provider in (
            _verifiable_providers_runtime(
                db
            )
        )
        if provider.get("enabled")
        and float(
            provider.get("weight")
            or 0
        ) > 0
    ]

    if not providers:
        return {}

    weights = [
        float(
            provider.get("weight")
            or 0
        )
        for provider in providers
    ]

    return random.choices(
        providers,
        weights=weights,
        k=1,
    )[0]


def _queue_verifiable_pair_for_pending(
    *,
    verifiable_key: str,
    pending: dict,
    provider_rfc: str,
    provider_idcif: str,
    remote_jid: str,
    instance_name: str,
    provider_response_msg_id: str,
    quoted_message_id: str = "",
    matched_by: str = "",
    fanout_index: int = 0,
    identifier_corrected: bool | None = None,
) -> dict:
    """
    Encola un resultado para una solicitud pendiente
    ya seleccionada.

    Se utiliza tanto para una coincidencia normal como
    para entregar a varias solicitudes idénticas.
    """
    verifiable_key = (
        verifiable_key or ""
    ).strip()

    pending = pending or {}

    if not verifiable_key or not pending:
        return {
            "ok": False,
            "queued": False,
            "request_key": verifiable_key,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "reason": "pending_missing",
        }

    if not claim_provider_result(
        verifiable_key
    ):
        return {
            "ok": False,
            "queued": False,
            "request_key": verifiable_key,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "reason": "result_already_claimed",
        }

    original_type = (
        pending.get(
            "original_query_type"
        )
        or ""
    ).strip().upper()

    original_identifier = (
        pending.get(
            "original_identifier"
        )
        or ""
    ).strip().upper()

    client_instance = (
        pending.get(
            "client_instance"
        )
        or MAIN_PANEL_INSTANCE
    ).strip()

    client_group_jid = (
        pending.get(
            "client_group_jid"
        )
        or ""
    ).strip()

    requester_number = (
        pending.get(
            "requester_number"
        )
        or ""
    ).strip()

    requester_name = (
        pending.get(
            "requester_name"
        )
        or ""
    ).strip()

    requester_label = (
        pending.get(
            "requester_label"
        )
        or requester_name
        or "Usuario"
    ).strip()

    original_msg_id = (
        pending.get(
            "client_msg_id"
        )
        or ""
    ).strip()

    normal_request_key = (
        pending.get(
            "normal_request_key"
        )
        or verifiable_key
    ).strip()

    inflight_key = (
        pending.get(
            "inflight_key"
        )
        or ""
    ).strip()

    generated_query = (
        f"RFC: {provider_rfc}\n"
        f"IDCIF: {provider_idcif}"
    )

    if identifier_corrected is None:
        correction_detected = (
            matched_by
            in {
                "curp_rfc_near_correction",
                "rfc_near_correction",
            }
        )
    else:
        correction_detected = bool(
            identifier_corrected
        )

    job_data = {
        "requester_number": requester_number,
        "requester_name": requester_name,
        "requester_label": requester_label,
        "group_jid": client_group_jid,
        "group_name": client_group_jid,
        "original_text": generated_query,
        "query": generated_query,
        "query_type": "RFC_VERIFICABLE",
        "forced_success_kind": (
            "RFC_VERIFICABLE"
        ),
        "msg_type": "",
        "media_id": "",
        "msg_id": original_msg_id,
        "mime_type": "",
        "evolution_instance": (
            client_instance
        ),
        "request_key": normal_request_key,
        "inflight_key": inflight_key,
        "execution_key": (
            original_msg_id
            or verifiable_key
        ),
        "request_started_at_epoch": float(
            pending.get(
                "request_started_at_epoch"
            )
            or time.time()
        ),
        "is_verifiable": True,
        "verifiable_price": float(
            pending.get(
                "verifiable_price"
            )
            or 0
        ),
        "verifiable_request_key": (
            verifiable_key
        ),
        "verifiable_original_type": (
            original_type
        ),
        "verifiable_original_identifier": (
            original_identifier
        ),
        "provider_rfc": provider_rfc,
        "provider_idcif": provider_idcif,
        "provider_response_msg_id": (
            provider_response_msg_id
        ),
        "provider_quoted_msg_id": (
            quoted_message_id
        ),
        "provider_match_method": (
            matched_by
        ),
        "verifiable_identifier_corrected": (
            correction_detected
        ),
        "verifiable_fanout_index": (
            int(fanout_index or 0)
        ),
        "verifiable_count_provider_success": (
            int(fanout_index or 0)
            in (0, 1)
        ),
        "verifiable_provider_code": (
            pending.get(
                "provider_code"
            )
            or ""
        ),
        "verifiable_provider_db_name": (
            pending.get(
                "provider_db_name"
            )
            or ""
        ),
        "verifiable_provider_name": (
            pending.get(
                "provider_name"
            )
            or ""
        ),
        "verifiable_provider_group": (
            pending.get(
                "provider_group_jid"
            )
            or remote_jid
        ),
        "verifiable_provider_instance": (
            pending.get(
                "provider_instance"
            )
            or instance_name
        ),
    }

    final_rq_job_id = (
        "rfc-verifiable-result:"
        f"{verifiable_key}"
    )

    try:
        request_queue.enqueue(
            "worker_jobs."
            "process_group_request_job",
            job_data,
            job_id=final_rq_job_id,
            job_timeout=900,
            result_ttl=0,
            failure_ttl=1200,
        )

    except Exception:
        release_provider_result_claim(
            verifiable_key
        )
        raise

    print(
        "RFC_VERIFIABLE_PAIR_QUEUED =",
        {
            "job_id": final_rq_job_id,
            "request_key": verifiable_key,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "client_group": client_group_jid,
            "provider_group": remote_jid,
            "matched_by": matched_by,
            "fanout_index": fanout_index,
            "corrected": correction_detected,
        },
        flush=True,
    )

    return {
        "ok": True,
        "queued": True,
        "job_id": final_rq_job_id,
        "request_key": verifiable_key,
        "rfc": provider_rfc,
        "idcif": provider_idcif,
        "matched_by": matched_by,
        "fanout_index": fanout_index,
        "corrected": correction_detected,
    }


def _queue_one_verifiable_provider_pair(
    *,
    provider_rfc: str,
    provider_idcif: str,
    remote_jid: str,
    instance_name: str,
    provider_response_msg_id: str,
    quoted_message_id: str = "",
) -> dict:
    """
    Relaciona una pareja RFC/IDCIF con su pendiente
    y crea exactamente un job RFC verificable.

    Para mensajes con varias parejas no se utiliza
    la cita, porque una sola cita no puede representar
    todas las solicitudes incluidas en la lista.
    """

    provider_rfc = (
        provider_rfc or ""
    ).strip().upper()

    provider_idcif = (
        provider_idcif or ""
    ).strip()

    quoted_message_id = (
        quoted_message_id or ""
    ).strip()

    verifiable_key = ""
    pending = {}
    matched_without_quote = False
    matched_by = ""

    # Para una respuesta individual sí podemos utilizar
    # la relación directa con el mensaje citado.
    if quoted_message_id:
        verifiable_key = (
            request_key_from_provider_message(
                quoted_message_id
            )
        )

        if verifiable_key:
            pending = load_pending(
                verifiable_key
            )

    # Para listas o mensajes sin cita se relaciona
    # cada RFC contra sus solicitudes pendientes.
    if not verifiable_key:
        fallback_match = (
            find_pending_request_by_provider_rfc(
                provider_rfc,
                provider_group_jid=remote_jid,
                provider_instance=instance_name,
            )
        )

        if not fallback_match.get("ok"):
            matches = (
                fallback_match.get("matches")
                or []
            )
        
            same_original_request = bool(
                fallback_match.get(
                    "same_original_request"
                )
            )
        
            if (
                fallback_match.get("reason")
                == "ambiguous_pending_match"
                and same_original_request
                and len(matches) > 1
            ):
                fanout_identifier_corrected = any(
                    (
                        match.get("matched_by")
                        in {
                            "curp_rfc_near_correction",
                            "rfc_near_correction",
                        }
                    )
                    for match in matches
                )
                fanout_results = []
        
                for fanout_index, match in enumerate(
                    matches,
                    start=1,
                ):
                    request_key = (
                        match.get("request_key")
                        or ""
                    ).strip()
        
                    match_pending = (
                        match.get("pending")
                        or {}
                    )
        
                    result = (
                        _queue_verifiable_pair_for_pending(
                            verifiable_key=request_key,
                            pending=match_pending,
                            provider_rfc=provider_rfc,
                            provider_idcif=(
                                provider_idcif
                            ),
                            remote_jid=remote_jid,
                            instance_name=instance_name,
                            provider_response_msg_id=(
                                provider_response_msg_id
                            ),
                            quoted_message_id="",
                            matched_by=(
                                "identical_pending_fanout"
                            ),
                            fanout_index=fanout_index,
                            identifier_corrected=(
                                fanout_identifier_corrected
                            ),
                        )
                    )
        
                    fanout_results.append(result)
        
                queued_results = [
                    item
                    for item in fanout_results
                    if item.get("queued")
                ]
        
                return {
                    "ok": bool(queued_results),
                    "queued": bool(queued_results),
                    "fanout": True,
                    "fanout_total": len(matches),
                    "fanout_queued": len(
                        queued_results
                    ),
                    "rfc": provider_rfc,
                    "idcif": provider_idcif,
                    "results": fanout_results,
                }
        
            return {
                "ok": False,
                "rfc": provider_rfc,
                "idcif": provider_idcif,
                "reason": (
                    fallback_match.get("reason")
                    or "pending_not_found"
                ),
                "matches": matches,
            }

        verifiable_key = (
            fallback_match.get(
                "request_key"
            )
            or ""
        ).strip()

        pending = (
            fallback_match.get("pending")
            or {}
        )

        matched_without_quote = True
        matched_by = (
            fallback_match.get(
                "matched_by"
            )
            or ""
        )

    if not verifiable_key:
        return {
            "ok": False,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "reason": "request_key_empty",
        }

    if not pending:
        pending = load_pending(
            verifiable_key
        )

    if not pending:
        return {
            "ok": False,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "request_key": verifiable_key,
            "reason": (
                "pending_expired_or_missing"
            ),
        }

    expected_provider_group = (
        pending.get(
            "provider_group_jid"
        )
        or ""
    ).strip()

    expected_provider_instance = (
        pending.get(
            "provider_instance"
        )
        or ""
    ).strip()

    if (
        expected_provider_group
        and expected_provider_group
        != remote_jid
    ):
        return {
            "ok": False,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "request_key": verifiable_key,
            "reason": (
                "provider_group_mismatch"
            ),
            "expected_group": (
                expected_provider_group
            ),
            "received_group": remote_jid,
        }

    if (
        expected_provider_instance
        and expected_provider_instance
        != instance_name
    ):
        return {
            "ok": False,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "request_key": verifiable_key,
            "reason": (
                "provider_instance_mismatch"
            ),
        }

    original_type = (
        pending.get(
            "original_query_type"
        )
        or ""
    ).strip().upper()

    original_identifier = (
        pending.get(
            "original_identifier"
        )
        or ""
    ).strip().upper()

    effective_match_method = (
        matched_by or ""
    ).strip()
    
    if quoted_message_id:
        if original_type == "CURP":
            physical_rfc_prefix = (
                provider_rfc[:10]
                if len(provider_rfc) == 13
                else ""
            )
    
            if (
                physical_rfc_prefix
                and original_identifier[:10]
                == physical_rfc_prefix
            ):
                effective_match_method = (
                    "quoted_curp_rfc_prefix"
                )
    
            elif _near_curp_rfc_match(
                original_identifier,
                provider_rfc,
            ):
                effective_match_method = (
                    "curp_rfc_near_correction"
                )
    
            else:
                return {
                    "ok": False,
                    "rfc": provider_rfc,
                    "idcif": provider_idcif,
                    "request_key": verifiable_key,
                    "reason": (
                        "quoted_curp_rfc_mismatch"
                    ),
                    "original_identifier": (
                        original_identifier
                    ),
                }
    
        elif original_type == "RFC_ONLY":
            if provider_rfc == original_identifier:
                effective_match_method = (
                    "quoted_exact_rfc"
                )
    
            elif _near_rfc_match(
                original_identifier,
                provider_rfc,
            ):
                effective_match_method = (
                    "rfc_near_correction"
                )
    
            else:
                return {
                    "ok": False,
                    "rfc": provider_rfc,
                    "idcif": provider_idcif,
                    "request_key": verifiable_key,
                    "reason": (
                        "quoted_rfc_mismatch"
                    ),
                    "expected_rfc": (
                        original_identifier
                    ),
                }
    
    elif not effective_match_method:
        effective_match_method = (
            "unquoted_pending_match"
        )

    # Una respuesta citada también puede corresponder a
    # varias solicitudes pendientes exactamente iguales.
    #
    # Solo se hace fanout si:
    # - la cita ya identificó una solicitud válida;
    # - existen varios pendientes;
    # - todos tienen exactamente el mismo tipo e identificador.
    if quoted_message_id:
        sibling_match = (
            find_pending_request_by_provider_rfc(
                provider_rfc,
                provider_group_jid=remote_jid,
                provider_instance=instance_name,
            )
        )

        sibling_matches = (
            sibling_match.get("matches")
            or []
        )

        if (
            not sibling_match.get("ok")
            and sibling_match.get("reason")
            == "ambiguous_pending_match"
            and bool(
                sibling_match.get(
                    "same_original_request"
                )
            )
            and len(sibling_matches) > 1
        ):
            fanout_identifier_corrected = any(
                (
                    match.get("matched_by")
                    in {
                        "curp_rfc_near_correction",
                        "rfc_near_correction",
                    }
                )
                for match in sibling_matches
            )
            fanout_results = []

            for fanout_index, match in enumerate(
                sibling_matches,
                start=1,
            ):
                sibling_request_key = (
                    match.get("request_key")
                    or ""
                ).strip()

                sibling_pending = (
                    match.get("pending")
                    or {}
                )

                result = (
                    _queue_verifiable_pair_for_pending(
                        verifiable_key=(
                            sibling_request_key
                        ),
                        pending=sibling_pending,
                        provider_rfc=provider_rfc,
                        provider_idcif=provider_idcif,
                        remote_jid=remote_jid,
                        instance_name=instance_name,
                        provider_response_msg_id=(
                            provider_response_msg_id
                        ),
                        quoted_message_id=(
                            quoted_message_id
                        ),
                        matched_by=(
                            "quoted_identical_pending_fanout"
                        ),
                        fanout_index=fanout_index,
                        identifier_corrected=(
                            fanout_identifier_corrected
                        ),
                    )
                )

                fanout_results.append(result)

            queued_results = [
                item
                for item in fanout_results
                if item.get("queued")
            ]

            return {
                "ok": bool(queued_results),
                "queued": bool(queued_results),
                "fanout": True,
                "quoted_fanout": True,
                "fanout_total": len(
                    sibling_matches
                ),
                "fanout_queued": len(
                    queued_results
                ),
                "rfc": provider_rfc,
                "idcif": provider_idcif,
                "results": fanout_results,
            }

    return _queue_verifiable_pair_for_pending(
        verifiable_key=verifiable_key,
        pending=pending,
        provider_rfc=provider_rfc,
        provider_idcif=provider_idcif,
        remote_jid=remote_jid,
        instance_name=instance_name,
        provider_response_msg_id=(
            provider_response_msg_id
        ),
        quoted_message_id=(
            quoted_message_id
        ),
        matched_by=effective_match_method,
    )


def _extract_verifiable_no_id_items(
    provider_text: str,
) -> list[dict]:
    """
    Extrae respuestas negativas individuales y de listas mixtas.

    Admite:
    - NO ID
    - CURP NO ID
    - SIN ID
    - S/ID
    - RFC NO ID
    - CURP NO ID
    """

    results: list[dict] = []

    bare_no_id_re = re.compile(
        r"^(?:"
        r"(?:CURP\s+)?NO\s*ID"
        r"|(?:CURP\s+)?SIN\s*ID"
        r"|(?:CURP\s+)?NO\s+HAY\s+ID"
        r"|(?:CURP\s+)?S\s*/\s*ID"
        r")$",
        re.I,
    )

    identifier_no_id_re = re.compile(
        r"^(?P<identifier>"
        r"(?:[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})"
        r"|(?:[A-Z][AEIOUX][A-Z]{2}\d{6}"
        r"[HM][A-Z]{5}[A-Z0-9]\d)"
        r")"
        r"\s*(?:[-:|]\s*)?"
        r"(?P<status>"
        r"S\s*/\s*ID"
        r"|SIN\s+ID"
        r"|NO\s+ID"
        r"|NO\s+HAY\s+ID"
        r")$",
        re.I,
    )

    for line_number, raw_line in enumerate(
        (provider_text or "").splitlines(),
        start=1,
    ):
        normalized_line = re.sub(
            r"\s+",
            " ",
            (raw_line or "").strip().upper(),
        )

        if not normalized_line:
            continue

        identifier_match = (
            identifier_no_id_re.fullmatch(
                normalized_line
            )
        )

        if identifier_match:
            results.append(
                {
                    "identifier": (
                        identifier_match.group(
                            "identifier"
                        )
                        or ""
                    ).strip().upper(),
                    "status": (
                        identifier_match.group(
                            "status"
                        )
                        or ""
                    ).strip().upper(),
                    "line_number": line_number,
                    "raw_line": raw_line,
                }
            )
            continue

        if bare_no_id_re.fullmatch(
            normalized_line
        ):
            results.append(
                {
                    "identifier": "",
                    "status": normalized_line,
                    "line_number": line_number,
                    "raw_line": raw_line,
                }
            )

    return results


def _find_verifiable_no_id_pending(
    *,
    identifier: str,
    quoted_message_id: str,
    remote_jid: str,
    instance_name: str,
    allow_quote: bool,
) -> dict:
    """
    Busca la solicitud pendiente.

    - Usa cita para una respuesta individual.
    - Usa el RFC/CURP incluido en la línea para listas.
    """

    identifier = (
        identifier or ""
    ).strip().upper()

    quoted_message_id = (
        quoted_message_id or ""
    ).strip()

    request_key = ""
    pending = {}
    matched_by = ""

    if allow_quote and quoted_message_id:
        request_key = (
            request_key_from_provider_message(
                quoted_message_id
            )
            or ""
        ).strip()

        if request_key:
            pending = (
                load_pending(request_key)
                or {}
            )
            matched_by = "quoted_message"

    if not request_key and identifier:
        fallback_match = (
            find_pending_request_by_provider_rfc(
                identifier,
                provider_group_jid=remote_jid,
                provider_instance=instance_name,
            )
            or {}
        )

        if fallback_match.get("ok"):
            request_key = (
                fallback_match.get(
                    "request_key"
                )
                or ""
            ).strip()

            pending = (
                fallback_match.get("pending")
                or {}
            )

            matched_by = (
                fallback_match.get("matched_by")
                or "provider_identifier"
            )

    if request_key and not pending:
        pending = (
            load_pending(request_key)
            or {}
        )

    if not request_key:
        return {
            "ok": False,
            "identifier": identifier,
            "reason": (
                "pending_not_found"
                if identifier
                else "missing_identifier_and_quote"
            ),
        }

    if not pending:
        return {
            "ok": False,
            "identifier": identifier,
            "request_key": request_key,
            "reason": (
                "pending_expired_or_missing"
            ),
        }

    return {
        "ok": True,
        "request_key": request_key,
        "pending": pending,
        "identifier": identifier,
        "matched_by": matched_by,
    }


def _send_verifiable_no_id_to_client(
    *,
    request_key: str,
    pending: dict,
    remote_jid: str,
    instance_name: str,
    quoted_message_id: str = "",
    provider_response_msg_id: str = "",
    matched_identifier: str = "",
    matched_by: str = "",
) -> dict:
    request_key = (
        request_key or ""
    ).strip()

    pending = pending or {}

    expected_group = (
        pending.get("provider_group_jid")
        or ""
    ).strip()

    expected_instance = (
        pending.get("provider_instance")
        or ""
    ).strip()

    if (
        expected_group
        and expected_group != remote_jid
    ):
        return {
            "ok": False,
            "sent": False,
            "request_key": request_key,
            "reason": "provider_group_mismatch",
        }

    if (
        expected_instance
        and expected_instance != instance_name
    ):
        return {
            "ok": False,
            "sent": False,
            "request_key": request_key,
            "reason": "provider_instance_mismatch",
        }

    if not claim_provider_result(request_key):
        return {
            "ok": False,
            "sent": False,
            "request_key": request_key,
            "reason": "result_already_claimed",
        }

    client_group = (
        pending.get("client_group_jid")
        or ""
    ).strip()

    client_instance = (
        pending.get("client_instance")
        or MAIN_PANEL_INSTANCE
    ).strip()

    requester_label = (
        pending.get("requester_label")
        or pending.get("requester_name")
        or "Usuario"
    ).strip()

    inflight_key = (
        pending.get("inflight_key")
        or ""
    ).strip()

    original_identifier = (
        pending.get("original_identifier")
        or matched_identifier
        or "dato solicitado"
    ).strip().upper()

    original_type = (
        pending.get("original_query_type")
        or ""
    ).strip().upper()

    stored_provider_message_id = (
        pending.get("provider_message_id")
        or quoted_message_id
        or ""
    ).strip()

    if original_type == "CURP":
        identifier_label = (
            f"la CURP {original_identifier}"
        )
    elif original_type == "RFC_ONLY":
        identifier_label = (
            f"el RFC {original_identifier}"
        )
    else:
        identifier_label = original_identifier

    try:
        send_text(
            client_group,
            (
                f"⚠️ {requester_label}, "
                "el proveedor informó que no hay "
                "ID disponible para "
                f"{identifier_label}."
            ),
            instance_name=client_instance,
            fast=True,
        )

        print(
            "RFC_VERIFIABLE_NO_ID_SENT =",
            {
                "request_key": request_key,
                "client_group": client_group,
                "client_instance": client_instance,
                "original_identifier": (
                    original_identifier
                ),
                "matched_identifier": (
                    matched_identifier
                ),
                "matched_by": matched_by,
                "provider_response_msg_id": (
                    provider_response_msg_id
                ),
            },
            flush=True,
        )

    except Exception as send_exc:
        release_provider_result_claim(
            request_key
        )

        print(
            "RFC_VERIFIABLE_NO_ID_"
            "CLIENT_SEND_ERROR =",
            {
                "request_key": request_key,
                "client_group": client_group,
                "error": repr(send_exc),
            },
            flush=True,
        )

        return {
            "ok": False,
            "sent": False,
            "request_key": request_key,
            "reason": "client_send_error",
            "error": repr(send_exc),
        }

    if inflight_key:
        try:
            request_queue.connection.delete(
                inflight_key
            )
        except Exception as inflight_exc:
            print(
                "RFC_VERIFIABLE_NO_ID_"
                "INFLIGHT_RELEASE_ERROR =",
                {
                    "request_key": request_key,
                    "inflight_key": inflight_key,
                    "error": repr(inflight_exc),
                },
                flush=True,
            )

    finish_pending(
        request_key,
        provider_message_id=(
            stored_provider_message_id
        ),
    )

    return {
        "ok": True,
        "sent": True,
        "request_key": request_key,
        "client_group": client_group,
        "original_identifier": (
            original_identifier
        ),
    }


@router.post("/webhook/evolution-rfc")
async def evolution_rfc_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return {"ok": False, "error": "invalid_json"}

    db = SessionLocal()

    try:
        event = str(payload.get("event") or "").strip().lower()
        if event not in {"messages.upsert", "messages_upsert"}:
            return {"ok": True, "ignored": "event", "event": event}

        instance_name = (payload.get("instance") or MAIN_PANEL_INSTANCE).strip()

        data = payload.get("data") or {}
        key = data.get("key") or {}
        message = data.get("message") or {}

        from_me = bool(key.get("fromMe"))
        remote_jid = (
            key.get("remoteJid")
            or data.get("remoteJid")
            or data.get("chatId")
            or ""
        )

        msg_id = key.get("id") or data.get("id") or data.get("messageId") or ""
        push_name = data.get("pushName") or data.get("pushname") or data.get("notifyName") or ""

        is_group = _is_group(remote_jid)

        requester_wa_id = (
            key.get("participant")
            or data.get("participant")
            or data.get("sender")
            or remote_jid
            or ""
        )

        text = _extract_text(message, data)
        msg_type, media_url, mime_type = _extract_media(message)

        # Para Evolution /chat/getBase64FromMediaMessage se usa el ID del mensaje.
        # NO usar la URL mmg.whatsapp.net como media_id.
        media_id = msg_id if msg_type else ""

        cmd_preview = (text or "").strip().lower()
        admin_group_commands = {
            "/groupid", "groupid", "/idgrupo", "idgrupo", "/id",
            "/addgroup", "addgroup", "/addgroupo", "addgroupo",
            "/autorizar", "autorizar",
        }

        # ======================================================
        # RFC VERIFICABLE:
        # RESPUESTA DEL GRUPO PROVEEDOR
        # ======================================================
        current_verifiable_provider = (
            verifiable_provider_by_group(
                remote_jid,
                instance_name,
            )
        )
        
        is_verifiable_provider_group = bool(
            current_verifiable_provider
        )

        if (
            is_verifiable_provider_group
            and not from_me
        ):
            print(
                "RFC_VERIFIABLE_QUOTE_PAYLOAD =",
                json.dumps(
                    {
                        "data_keys": (
                            list(data.keys())
                            if isinstance(data, dict)
                            else []
                        ),
                        "message": message,
                        "context_info": (
                            data.get("contextInfo")
                            if isinstance(data, dict)
                            else None
                        ),
                    },
                    ensure_ascii=False,
                    default=str,
                ),
                flush=True,
            )
            
            top_context_info = (
                data.get("contextInfo")
                if isinstance(data, dict)
                and isinstance(
                    data.get("contextInfo"),
                    dict,
                )
                else {}
            )
            
            quoted_message_id = str(
                top_context_info.get("stanzaId")
                or top_context_info.get("quotedStanzaId")
                or top_context_info.get("quotedStanzaID")
                or extract_quoted_message_id(
                    message,
                    data,
                )
                or ""
            ).strip()

            print(
                "RFC_VERIFIABLE_PROVIDER_IN =",
                {
                    "instance": instance_name,
                    "group_jid": remote_jid,
                    "msg_id": msg_id,
                    "quoted_message_id": (
                        quoted_message_id
                    ),
                    "text": text,
                },
                flush=True,
            )

            # ==================================================
            # RESPUESTAS DEL PROVEEDOR:
            # INDIVIDUALES, CITADAS Y LISTAS MIXTAS
            # ==================================================
            provider_no_id_items = (
                _extract_verifiable_no_id_items(
                    text
                )
            )

            provider_pairs = (
                extract_rfc_idcif_pairs(
                    text
                )
            )

            total_provider_results = (
                len(provider_no_id_items)
                + len(provider_pairs)
            )

            if total_provider_results == 0:
                print(
                    "RFC_VERIFIABLE_PROVIDER_INVALID =",
                    {
                        "reason": (
                            "no_valid_provider_results"
                        ),
                        "quoted_message_id": (
                            quoted_message_id
                        ),
                        "text": text,
                    },
                    flush=True,
                )

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_invalid_"
                        "provider_response"
                    ),
                }

            is_single_result = (
                total_provider_results == 1
            )

            no_id_results = []

            for no_id_index, no_id_item in enumerate(
                provider_no_id_items,
                start=1,
            ):
                identifier = (
                    no_id_item.get("identifier")
                    or ""
                ).strip().upper()

                pending_match = (
                    _find_verifiable_no_id_pending(
                        identifier=identifier,
                        quoted_message_id=(
                            quoted_message_id
                        ),
                        remote_jid=remote_jid,
                        instance_name=instance_name,

                        # Una cita solo puede representar
                        # un resultado individual.
                        allow_quote=is_single_result,
                    )
                )

                if not pending_match.get("ok"):
                    result = {
                        "ok": False,
                        "sent": False,
                        "identifier": identifier,
                        "reason": (
                            pending_match.get("reason")
                            or "pending_not_found"
                        ),
                        "line_number": (
                            no_id_item.get(
                                "line_number"
                            )
                        ),
                    }

                    no_id_results.append(result)

                    print(
                        "RFC_VERIFIABLE_NO_ID_"
                        "MATCH_FAILED =",
                        result,
                        flush=True,
                    )

                    continue

                result = (
                    _send_verifiable_no_id_to_client(
                        request_key=(
                            pending_match[
                                "request_key"
                            ]
                        ),
                        pending=(
                            pending_match["pending"]
                        ),
                        remote_jid=remote_jid,
                        instance_name=instance_name,
                        quoted_message_id=(
                            quoted_message_id
                            if is_single_result
                            else ""
                        ),
                        provider_response_msg_id=(
                            msg_id
                        ),
                        matched_identifier=identifier,
                        matched_by=(
                            pending_match.get(
                                "matched_by"
                            )
                            or ""
                        ),
                    )
                )

                result["line_number"] = (
                    no_id_item.get("line_number")
                )

                result["no_id_index"] = no_id_index

                no_id_results.append(result)

            pair_results = []

            for pair_index, (
                provider_rfc,
                provider_idcif,
            ) in enumerate(
                provider_pairs,
                start=1,
            ):
                try:
                    result = (
                        _queue_one_verifiable_provider_pair(
                            provider_rfc=provider_rfc,
                            provider_idcif=(
                                provider_idcif
                            ),
                            remote_jid=remote_jid,
                            instance_name=instance_name,
                            provider_response_msg_id=(
                                msg_id
                            ),

                            # La cita se usa únicamente
                            # cuando todo el mensaje contiene
                            # un solo resultado.
                            quoted_message_id=(
                                quoted_message_id
                                if is_single_result
                                else ""
                            ),
                        )
                    )

                except Exception as pair_exc:
                    result = {
                        "ok": False,
                        "queued": False,
                        "rfc": provider_rfc,
                        "idcif": provider_idcif,
                        "reason": (
                            "pair_processing_exception"
                        ),
                        "error": repr(pair_exc),
                    }

                result["pair_index"] = pair_index

                pair_results.append(result)

            sent_no_id_results = [
                item
                for item in no_id_results
                if item.get("sent")
            ]

            failed_no_id_results = [
                item
                for item in no_id_results
                if not item.get("sent")
            ]

            queued_pair_results = [
                item
                for item in pair_results
                if item.get("queued")
            ]

            failed_pair_results = [
                item
                for item in pair_results
                if not item.get("queued")
            ]

            print(
                "RFC_VERIFIABLE_PROVIDER_RESULT =",
                {
                    "provider_group": remote_jid,
                    "provider_instance": (
                        instance_name
                    ),
                    "provider_message_id": msg_id,
                    "quoted_message_id": (
                        quoted_message_id
                    ),
                    "is_single_result": (
                        is_single_result
                    ),
                    "no_id_detected": len(
                        provider_no_id_items
                    ),
                    "no_id_sent": len(
                        sent_no_id_results
                    ),
                    "no_id_failed": len(
                        failed_no_id_results
                    ),
                    "pairs_detected": len(
                        provider_pairs
                    ),
                    "pairs_queued": len(
                        queued_pair_results
                    ),
                    "pairs_failed": len(
                        failed_pair_results
                    ),
                    "no_id_results": (
                        no_id_results
                    ),
                    "pair_results": pair_results,
                },
                flush=True,
            )

            return {
                "ok": True,
                "flow": "RFC_VERIFICABLE",
                "batch": not is_single_result,
                "no_id_detected": len(
                    provider_no_id_items
                ),
                "no_id_sent": len(
                    sent_no_id_results
                ),
                "no_id_failed": len(
                    failed_no_id_results
                ),
                "pairs_detected": len(
                    provider_pairs
                ),
                "pairs_queued": len(
                    queued_pair_results
                ),
                "pairs_failed": len(
                    failed_pair_results
                ),
                "no_id_results": no_id_results,
                "pair_results": pair_results,
            }

        # Ignorar mensajes propios, EXCEPTO comandos administrativos en grupos.
        # Esto permite que tú, desde el WhatsApp conectado al bot, puedas mandar /groupid y /addgroup.
        if from_me and not (is_group and cmd_preview in admin_group_commands):
            print("RFC_FROM_ME_IGNORED =", {
                "remote_jid": remote_jid,
                "is_group": is_group,
                "text": text,
            }, flush=True)
            return {"ok": True, "ignored": "from_me"}

        print("RFC_WEBHOOK_IN =", {
            "event": event,
            "instance": instance_name,
            "remote_jid": remote_jid,
            "participant": requester_wa_id,
            "msg_id": msg_id,
            "is_group": is_group,
            "text": text,
            "msg_type": msg_type,
        }, flush=True)

        # ======================================================
        # PRIVADOS: IGNORAR TODO
        # ======================================================
        if not is_group:
            print("RFC_PRIVATE_IGNORED =", {
                "remote_jid": remote_jid,
                "participant": requester_wa_id,
                "text": text,
            }, flush=True)
            return {"ok": True, "ignored": "private_chat"}

        # ======================================================
        # BLOQUEO DEL MINI PANEL
        # Debe ir ANTES de validar, responder ACK o encolar.
        # ======================================================
        try:
            group_is_blocked = _is_group_blocked(remote_jid)
        except Exception:
            # Fail closed: ante duda de Redis, no procesar solicitudes.
            return {
                "ok": True,
                "ignored": "group_block_check_unavailable",
                "group_jid": remote_jid,
                "instance": instance_name,
            }

        if group_is_blocked:
            print(
                "RFC_GROUP_BLOCKED_IGNORED =",
                {
                    "group_jid": remote_jid,
                    "instance": instance_name,
                    "requester_wa_id": requester_wa_id,
                    "msg_id": msg_id,
                    "redis_key": BLOCKED_GROUPS_KEY,
                },
                flush=True,
            )

            return {
                "ok": True,
                "ignored": "group_blocked",
                "group_jid": remote_jid,
                "instance": instance_name,
            }

        cmd = (text or "").strip().lower()

        # ======================================================
        # /groupid
        # ======================================================
        if cmd in {"/groupid", "groupid", "/idgrupo", "idgrupo", "/id"}:
            if not from_me:
                print(
                    "RFC_ADMIN_COMMAND_DENIED =",
                    {
                        "command": cmd,
                        "group_jid": remote_jid,
                        "instance": instance_name,
                        "requester_wa_id": requester_wa_id,
                    },
                    flush=True,
                )
                return {"ok": True, "ignored": "admin_command_denied"}

            try:
                send_text(
                    remote_jid,
                    f"🆔 ID del grupo:\n{remote_jid}\n\nInstancia: {instance_name}",
                    instance_name=instance_name,
                )
            except Exception as e:
                print("RFC_GROUPID_SEND_ERROR =", repr(e), flush=True)

            print("RFC_GROUPID_OK =", {"group_jid": remote_jid, "instance": instance_name}, flush=True)
            return {"ok": True, "groupid": remote_jid, "instance": instance_name}

        # ======================================================
        # /addgroup
        # ======================================================
        if cmd in {"/addgroup", "addgroup", "/addgroupo", "addgroupo", "/autorizar", "autorizar"}:
            if not from_me:
                print(
                    "RFC_ADMIN_COMMAND_DENIED =",
                    {
                        "command": cmd,
                        "group_jid": remote_jid,
                        "instance": instance_name,
                        "requester_wa_id": requester_wa_id,
                    },
                    flush=True,
                )
                return {"ok": True, "ignored": "admin_command_denied"}

            try:
                _upsert_authorized_group(db, remote_jid, instance_name)

                try:
                    send_text(
                        remote_jid,
                        "✅ Grupo autorizado para RFC.\n\n"
                        "Ya pueden enviar:\n"
                        "• CURP\n"
                        "• RFC\n"
                        "• RFC + IDCIF\n"
                        "• QR SAT",
                        instance_name=instance_name,
                    )
                except Exception as e:
                    print("RFC_ADDGROUP_SEND_ERROR =", repr(e), flush=True)

                print("RFC_ADDGROUP_OK =", {"group_jid": remote_jid, "instance": instance_name}, flush=True)
                return {"ok": True, "addgroup": True, "group_jid": remote_jid, "instance": instance_name}

            except Exception as e:
                db.rollback()
                print("RFC_ADDGROUP_ERROR =", repr(e), flush=True)
                return {"ok": False, "error": "addgroup_failed"}

        # ======================================================
        # Solo procesar solicitudes si grupo autorizado
        # ======================================================
        if not _authorized_group_exists(db, remote_jid, instance_name):
            print("RFC_GROUP_NOT_AUTHORIZED =", {
                "group_jid": remote_jid,
                "instance": instance_name,
                "text": text,
            }, flush=True)
            return {
                "ok": True,
                "ignored": "group_not_authorized",
                "group_jid": remote_jid,
                "instance": instance_name,
            }

        verifiable = parse_verifiable_request(
            text
        )

        if verifiable.get("is_verifiable"):
            if not verifiable.get("ok"):
                try:
                    send_text(
                        remote_jid,
                        verifiable.get("error"),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception as send_exc:
                    print(
                        "RFC_VERIFIABLE_INVALID_"
                        "SEND_ERROR =",
                        repr(send_exc),
                        flush=True,
                    )

                return {
                    "ok": True,
                    "ignored": (
                        "invalid_verifiable_request"
                    ),
                }

            configured_verifiable_providers = (
                load_verifiable_providers()
            )
            
            if not configured_verifiable_providers:
                print(
                    "RFC_VERIFIABLE_CONFIG_ERROR =",
                    "providers_empty",
                    flush=True,
                )
            
                try:
                    send_text(
                        remote_jid,
                        (
                            "⚠️ El servicio de RFC "
                            "verificable no está configurado."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception:
                    pass
            
                return {
                    "ok": False,
                    "error": (
                        "verifiable_providers_"
                        "not_configured"
                    ),
                }

            original_identifier = (
                verifiable["identifier"]
            )

            original_query_type = (
                verifiable["query_type"]
            )

            requester_label = (
                push_name or "Usuario"
            )

            verifiable_config = (
                _verifiable_bot_config(
                    db,
                    instance_name,
                )
            )

            if not verifiable_config["exists"]:
                print(
                    "RFC_VERIFICABLE_BOT_NOT_FOUND =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                    },
                    flush=True,
                )

                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "este bot no tiene configurado "
                            "RFC verificable."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                    
                except Exception:
                    pass

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_bot_not_configured"
                    ),
                }

            if not verifiable_config["enabled"]:
                print(
                    "RFC_VERIFICABLE_DISABLED =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                        "identifier": (
                            original_identifier
                        ),
                    },
                    flush=True,
                )

                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "RFC verificable no está activo "
                            "para este bot."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception:
                    pass

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_disabled"
                    ),
                }

            verifiable_group_config = (
                _verifiable_group_config(
                    db,
                    remote_jid,
                    instance_name,
                )
            )

            if not verifiable_group_config[
                "exists"
            ]:
                print(
                    "RFC_VERIFIABLE_GROUP_NOT_FOUND =",
                    {
                        "instance":
                            instance_name,
                        "group_jid":
                            remote_jid,
                        "identifier":
                            original_identifier,
                    },
                    flush=True,
                )

                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "este grupo no está configurado "
                            "para RFC verificable."
                        ),
                        instance_name=
                            instance_name,
                        fast=True,
                    )
                except Exception:
                    pass

                return {
                    "ok": True,
                    "ignored":
                        "verifiable_group_not_found",
                }

            if not verifiable_group_config[
                "owned"
            ]:
                print(
                    "RFC_VERIFIABLE_GROUP_OWNER_MISMATCH =",
                    {
                        "instance":
                            instance_name,
                        "owner_instance":
                            verifiable_group_config[
                                "owner_instance"
                            ],
                        "group_jid":
                            remote_jid,
                        "identifier":
                            original_identifier,
                    },
                    flush=True,
                )

                return {
                    "ok": True,
                    "ignored":
                        "verifiable_group_owner_mismatch",
                }

            if not verifiable_group_config[
                "enabled"
            ]:
                print(
                    "RFC_VERIFIABLE_GROUP_DISABLED =",
                    {
                        "instance":
                            instance_name,
                        "group_jid":
                            remote_jid,
                        "identifier":
                            original_identifier,
                    },
                    flush=True,
                )

                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "RFC verificable no está activo "
                            "para este grupo."
                        ),
                        instance_name=
                            instance_name,
                        fast=True,
                    )
                except Exception:
                    pass

                return {
                    "ok": True,
                    "ignored":
                        "verifiable_group_disabled",
                }

            if not verifiable_config.get(
                "is_active",
                True,
            ):
                print(
                    "RFC_VERIFICABLE_BOT_INACTIVE =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                        "identifier": (
                            original_identifier
                        ),
                    },
                    flush=True,
                )

                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "este bot no está activo."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception:
                    pass

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_bot_inactive"
                    ),
                }

            if verifiable_config.get(
                "is_blocked",
                False,
            ):
                print(
                    "RFC_VERIFICABLE_BOT_BLOCKED =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                        "identifier": (
                            original_identifier
                        ),
                    },
                    flush=True,
                )

                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "este bot está bloqueado."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception:
                    pass

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_bot_blocked"
                    ),
                }

            verifiable_limit = int(
                verifiable_config.get("limit")
                or 0
            )

            verifiable_used = int(
                verifiable_config.get("used")
                or 0
            )

            if (
                verifiable_limit > 0
                and verifiable_used
                >= verifiable_limit
            ):
                print(
                    "RFC_VERIFICABLE_NO_BALANCE =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                        "identifier": (
                            original_identifier
                        ),
                        "limit": (
                            verifiable_limit
                        ),
                        "used": (
                            verifiable_used
                        ),
                    },
                    flush=True,
                )

                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "este bot ya no tiene RFC "
                            "verificables disponibles."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception:
                    pass

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_limit_reached"
                    ),
                }

            normalized_query = (
                "VERIFICABLE:"
                f"{original_query_type}:"
                f"{original_identifier}"
            )

            command_key = _dedupe_key(
                instance_name,
                remote_jid,
                requester_wa_id,
                normalized_query,
                msg_id,
            )

            redis_conn = (
                request_queue.connection
            )

            inflight_key = (
                f"rfc:inflight:{command_key}"
            )

            if not redis_conn.set(
                inflight_key,
                "1",
                nx=True,
                ex=86400,
            ):
                duplicate_notice_key = (
                    "rfc:verifiable:"
                    "duplicate_notice:"
                    f"{command_key}"
                )

                if redis_conn.set(
                    duplicate_notice_key,
                    "1",
                    nx=True,
                    ex=DUPLICATE_NOTICE_TTL_SEC,
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                f"⏳ {requester_label}, "
                                "este RFC verificable "
                                "ya está siendo procesado."
                            ),
                            instance_name=(
                                instance_name
                            ),
                            fast=True,
                        )
                    except Exception:
                        pass

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_already_"
                        "processing"
                    ),
                }

            selected_provider = (
                _choose_verifiable_provider(
                    db
                )
            )
            
            if not selected_provider:
                redis_conn.delete(
                    inflight_key
                )
            
                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "no hay proveedores de RFC "
                            "verificable activos."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception:
                    pass
            
                return {
                    "ok": False,
                    "error": (
                        "verifiable_provider_"
                        "not_available"
                    ),
                }
            
            provider_code = (
                selected_provider["code"]
            )
            
            provider_db_name = (
                selected_provider["db_name"]
            )
            
            provider_name = (
                selected_provider["name"]
            )
            
            provider_group_jid = (
                selected_provider["group_jid"]
            )
            
            provider_instance_name = (
                selected_provider[
                    "instance_name"
                ]
            )

            provider_query_type = (
                original_query_type
            )
            
            provider_identifier = (
                original_identifier
            )
            
            provider_text = (
                original_identifier
            )
            
            must_convert_curp_to_rfc = (
                original_query_type == "CURP"
                and provider_group_jid
                == LOCALIZACIONES_PROVIDER_GROUP
            )
            
            if must_convert_curp_to_rfc:
                try:
                    provider_identifier = (
                        _curp_to_moffin_rfc_cached(
                            original_identifier
                        )
                    )
            
                    provider_query_type = (
                        "RFC_ONLY"
                    )
            
                    provider_text = (
                        provider_identifier
                    )
            
                    print(
                        "[VERIFIABLE_PROVIDER_"
                        "CURP_CONVERTED_TO_RFC]",
                        {
                            "client_curp": (
                                original_identifier
                            ),
                            "provider_rfc": (
                                provider_identifier
                            ),
                            "provider_group": (
                                provider_group_jid
                            ),
                            "provider_instance": (
                                provider_instance_name
                            ),
                        },
                        flush=True,
                    )
            
                except Exception as conversion_exc:
                    redis_conn.delete(
                        inflight_key
                    )
            
                    print(
                        "[VERIFIABLE_PROVIDER_"
                        "CURP_TO_RFC_FAIL]",
                        {
                            "curp": (
                                original_identifier
                            ),
                            "provider_group": (
                                provider_group_jid
                            ),
                            "error": repr(
                                conversion_exc
                            ),
                        },
                        flush=True,
                    )
            
                    try:
                        send_text(
                            remote_jid,
                            (
                                f"⚠️ {requester_label}, "
                                "no fue posible convertir "
                                "la CURP a RFC verificable. "
                                "Intenta nuevamente."
                            ),
                            instance_name=(
                                instance_name
                            ),
                            fast=True,
                        )
                    except Exception:
                        pass
            
                    return {
                        "ok": False,
                        "error": (
                            "verifiable_curp_to_"
                            "rfc_failed"
                        ),
                    }

            pending_payload = {
                "normal_request_key": (
                    command_key
                ),
                "inflight_key": (
                    inflight_key
                ),
                "client_instance": (
                    instance_name
                ),
                "client_group_jid": (
                    remote_jid
                ),
                "requester_number": (
                    requester_wa_id
                ),
                "requester_name": push_name,
                "requester_label": (
                    requester_label
                ),
                "client_msg_id": msg_id,
                "request_started_at_epoch": (
                    time.time()
                ),
                "original_text": text,
                "original_query_type": (
                    original_query_type
                ),
                "original_identifier": (
                    original_identifier
                ),
                "provider_query_type": (
                    provider_query_type
                ),
                "provider_identifier": (
                    provider_identifier
                ),
                "provider_text": (
                    provider_text
                ),
                "verifiable_price": float(
                    verifiable_config["price"]
                ),
                "provider_code": (
                    provider_code
                ),
                "provider_db_name": (
                    provider_db_name
                ),
                "provider_name": (
                    provider_name
                ),
                "provider_group_jid": (
                    provider_group_jid
                ),
                "provider_instance": (
                    provider_instance_name
                ),
            }

            save_pending(
                command_key,
                pending_payload,
            )

            try:
                provider_response = send_text(
                    provider_group_jid,
                    provider_text,
                    instance_name=(
                        provider_instance_name
                    ),
                )

                provider_message_id = (
                    _extract_sent_message_id(
                        provider_response
                    )
                )

                if not provider_message_id:
                    raise RuntimeError(
                        "VERIFIABLE_PROVIDER_"
                        "MESSAGE_ID_EMPTY"
                    )

                associate_provider_message(
                    command_key,
                    provider_message_id,
                )

                pending_payload[
                    "provider_message_id"
                ] = provider_message_id
                
                save_pending(
                    command_key,
                    pending_payload,
                )

            except Exception as provider_exc:
                redis_conn.delete(
                    inflight_key
                )
                
                finish_pending(
                    command_key
                )

                print(
                    "RFC_VERIFIABLE_PROVIDER_"
                    "SEND_ERROR =",
                    {
                        "request_key": (
                            command_key
                        ),
                        "provider_code": (
                            provider_code
                        ),
                        "provider_db_name": (
                            provider_db_name
                        ),
                        "provider_name": (
                            provider_name
                        ),
                        "provider_group": (
                            provider_group_jid
                        ),
                        "provider_instance": (
                            provider_instance_name
                        ),
                        "error": repr(
                            provider_exc
                        ),
                    },
                    flush=True,
                )

                try:
                    send_text(
                        remote_jid,
                        (
                            f"⚠️ {requester_label}, "
                            "no fue posible enviar la "
                            "solicitud al proveedor de "
                            "RFC verificables."
                        ),
                        instance_name=(
                            instance_name
                        ),
                        fast=True,
                    )
                except Exception:
                    pass

                return {
                    "ok": False,
                    "error": (
                        "verifiable_provider_"
                        "send_failed"
                    ),
                }

            try:
                send_text(
                    remote_jid,
                    (
                        f"🔎 {requester_label}, "
                        "tu RFC verificable fue enviado.\n"
                        "Se entregará cuando el proveedor responda."
                    ),
                    instance_name=instance_name,
                    fast=True,
                )
            except Exception as ack_exc:
                print(
                    "RFC_VERIFIABLE_ACK_ERROR =",
                    repr(ack_exc),
                    flush=True,
                )

            print(
                "RFC_VERIFIABLE_SENT_TO_PROVIDER =",
                {
                    "request_key": command_key,
                    "identifier": (
                        original_identifier
                    ),
                    "query_type": (
                        original_query_type
                    ),
                    "provider_code": (
                        provider_code
                    ),
                    "provider_db_name": (
                        provider_db_name
                    ),
                    "provider_name": (
                        provider_name
                    ),
                    "provider_group": (
                        provider_group_jid
                    ),
                    "provider_instance": (
                        provider_instance_name
                    ),
                    "provider_message_id": (
                        provider_message_id
                    ),
                    "client_instance": (
                        instance_name
                    ),
                    "client_group": remote_jid,
                },
                flush=True,
            )

            return {
                "ok": True,
                "waiting_provider": True,
                "flow": "RFC_VERIFICABLE",
                "request_key": command_key,
            }

        parsed = _parse_rfc_query(text, msg_type=msg_type)

        if not parsed.get("ok"):
            try:
                send_text(
                    remote_jid,
                    parsed.get("error"),
                    instance_name=instance_name,
                )
            except Exception as e:
                print("RFC_INVALID_SEND_ERROR =", repr(e), flush=True)

            return {"ok": True, "ignored": "invalid_input"}

        query = parsed.get("query") or ""
        requester_label = push_name or "Usuario"

        normalized_query = re.sub(r"\s+", " ", (query or text or msg_id).strip().upper())
        command_key = _dedupe_key(instance_name, remote_jid, requester_wa_id, normalized_query, msg_id)

        redis_conn = request_queue.connection
        inflight_key = f"rfc:inflight:{command_key}"

        if not redis_conn.set(
            inflight_key,
            "1",
            nx=True,
            ex=REQUEST_INFLIGHT_TTL_SEC,
        ):
            print(
                "RFC_DUPLICATE_IGNORED =",
                {
                    "inflight_key": inflight_key,
                    "instance": instance_name,
                    "group_jid": remote_jid,
                    "requester": requester_wa_id,
                    "query": normalized_query,
                },
                flush=True,
            )
        
            duplicate_notice_key = (
                f"rfc:duplicate_notice:{command_key}"
            )
        
            if redis_conn.set(
                duplicate_notice_key,
                "1",
                nx=True,
                ex=DUPLICATE_NOTICE_TTL_SEC,
            ):
                try:
                    send_text(
                        remote_jid,
                        (
                            f"⏳ {requester_label}, esta solicitud "
                            "ya está siendo procesada.\n"
                            "No es necesario volver a enviarla."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
        
                except Exception as duplicate_notice_exc:
                    print(
                        "RFC_DUPLICATE_NOTICE_SEND_ERROR =",
                        repr(duplicate_notice_exc),
                        flush=True,
                    )
        
            return {
                "ok": True,
                "ignored": "already_processing",
                "message": (
                    "La misma solicitud ya está siendo procesada."
                ),
            }

        job_data = {
            "requester_number": requester_wa_id,
            "requester_name": push_name,
            "requester_label": requester_label,
            "group_jid": remote_jid,
            "group_name": remote_jid,
            "original_text": text,
            "query": query,
            "query_type": parsed.get("type"),
            "msg_type": msg_type,
            "media_id": media_id,
            "msg_id": msg_id,
            "mime_type": mime_type,
            "evolution_instance": instance_name,
            "request_started_at_epoch": time.time(),
            "request_key": command_key,
            "inflight_key": inflight_key,
            "execution_key": (
                msg_id
                or f"{command_key}:{int(time.time())}"
            ),
        }

        rq_job_id = f"rfc-group-request:{command_key}"

        try:
            job = request_queue.enqueue(
                "worker_jobs.process_group_request_job",
                job_data,
                job_id=rq_job_id,
                job_timeout=900,
                result_ttl=0,
                failure_ttl=1200,
            )
        
        except Exception as enqueue_exc:
            error_text = str(enqueue_exc).lower()
        
            if (
                "already exists" in error_text
                or "already exists in" in error_text
                or "duplicate" in error_text
            ):
                print(
                    "RFC_RQ_DUPLICATE_JOB_BLOCKED =",
                    {
                        "job_id": rq_job_id,
                        "error": repr(enqueue_exc),
                    },
                    flush=True,
                )
        
                return {
                    "ok": True,
                    "ignored": "already_processing",
                    "job_id": rq_job_id,
                }
        
            # Si realmente no se creó el job,
            # no debemos dejar la solicitud bloqueada.
            redis_conn.delete(inflight_key)
        
            print(
                "RFC_JOB_ENQUEUE_ERROR =",
                {
                    "job_id": rq_job_id,
                    "error": repr(enqueue_exc),
                    "inflight_released": inflight_key,
                },
                flush=True,
            )
        
            raise
        
        print(
            "RFC_JOB_QUEUED =",
            {
                "job_id": job.id,
                "request_key": command_key,
                "inflight_key": inflight_key,
                "job_data": job_data,
            },
            flush=True,
        )

        ack_key = f"rfc:ack:{instance_name}:{msg_id}"

        if redis_conn.set(
            ack_key,
            "1",
            nx=True,
            ex=300,
        ):
            try:
                send_text(
                    remote_jid,
                    (
                        f"{_bot_label_from_db(instance_name)}\n"
                        f"Solicitud recibida de {requester_label}.\n"
                        "Esto puede tardar unos segundos..."
                    ),
                    instance_name=instance_name,
                    fast=True,
                )
        
            except Exception as ack_exc:
                print(
                    "RFC_ACK_SEND_ERROR =",
                    repr(ack_exc),
                    flush=True,
                )

        return {
            "ok": True,
            "queued": True,
            "type": parsed.get("type"),
            "instance": instance_name,
            "group_jid": remote_jid,
        }

    except Exception as e:
        print("RFC_WEBHOOK_FATAL_ERROR =", repr(e), flush=True)
        try:
            db.rollback()
        except Exception:
            pass
        return {"ok": False, "error": "webhook_exception"}

    finally:
        try:
            db.close()
        except Exception:
            pass
