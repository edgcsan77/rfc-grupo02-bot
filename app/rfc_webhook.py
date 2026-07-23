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
)

router = APIRouter()

MAIN_PANEL_INSTANCE = os.getenv("MAIN_PANEL_INSTANCE", "grupo02").strip()
GROUP_COMMAND = os.getenv("GROUP_COMMAND", "/csf").strip() or "/csf"

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
            return {
                "ok": False,
                "rfc": provider_rfc,
                "idcif": provider_idcif,
                "reason": (
                    fallback_match.get("reason")
                    or "pending_not_found"
                ),
                "matches": (
                    fallback_match.get("matches")
                    or []
                ),
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

    if (
        original_type == "RFC_ONLY"
        and provider_rfc
        != original_identifier
    ):
        return {
            "ok": False,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "request_key": verifiable_key,
            "reason": "rfc_mismatch",
            "expected_rfc": (
                original_identifier
            ),
        }

    if not claim_provider_result(
        verifiable_key
    ):
        return {
            "ok": False,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "request_key": verifiable_key,
            "reason": (
                "result_already_claimed"
            ),
        }

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

    job_data = {
        "requester_number": (
            requester_number
        ),
        "requester_name": (
            requester_name
        ),
        "requester_label": (
            requester_label
        ),
        "group_jid": (
            client_group_jid
        ),
        "group_name": (
            client_group_jid
        ),
        "original_text": (
            generated_query
        ),
        "query": generated_query,
        "query_type": (
            "RFC_VERIFICABLE"
        ),
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
        "request_key": (
            normal_request_key
        ),
        "inflight_key": (
            inflight_key
        ),
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
        "provider_idcif": (
            provider_idcif
        ),
        "provider_response_msg_id": (
            provider_response_msg_id
        ),
        "provider_quoted_msg_id": (
            quoted_message_id
        ),
        "provider_matched_without_quote": (
            matched_without_quote
        ),
        "provider_match_method": (
            matched_by
            or (
                "quoted_message"
                if quoted_message_id
                else ""
            )
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
        job = request_queue.enqueue(
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

    stored_provider_message_id = (
        pending.get(
            "provider_message_id"
        )
        or quoted_message_id
        or ""
    ).strip()

    finish_pending(
        verifiable_key,
        provider_message_id=(
            stored_provider_message_id
        ),
    )

    print(
        "RFC_VERIFIABLE_PAIR_QUEUED =",
        {
            "job_id": job.id,
            "request_key": (
                verifiable_key
            ),
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "client_group": (
                client_group_jid
            ),
            "provider_group": (
                remote_jid
            ),
            "matched_by": (
                matched_by
                or "quoted_message"
            ),
        },
        flush=True,
    )

    return {
        "ok": True,
        "queued": True,
        "job_id": job.id,
        "request_key": verifiable_key,
        "rfc": provider_rfc,
        "idcif": provider_idcif,
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
            # RESPUESTA NEGATIVA DEL PROVEEDOR:
            # "NO ID" citando la solicitud original
            # ==================================================
            provider_response_normalized = re.sub(
                r"\s+",
                " ",
                (text or "").strip().upper(),
            )

            provider_no_record = bool(
                re.fullmatch(
                    r"(?:NO\s*ID|SIN\s*ID|NO\s+HAY\s+ID)",
                    provider_response_normalized,
                    flags=re.I,
                )
            )

            if provider_no_record:
                # Para evitar asociar un "NO ID" al cliente
                # incorrecto, obligatoriamente debe venir citado.
                if not quoted_message_id:
                    print(
                        "RFC_VERIFICABLE_NO_ID_IGNORED =",
                        {
                            "reason": (
                                "missing_quoted_message_id"
                            ),
                            "text": text,
                            "msg_id": msg_id,
                        },
                        flush=True,
                    )

                    return {
                        "ok": True,
                        "ignored": (
                            "verifiable_no_id_"
                            "missing_quote"
                        ),
                    }

                no_record_request_key = (
                    request_key_from_provider_message(
                        quoted_message_id
                    )
                )

                if not no_record_request_key:
                    print(
                        "RFC_VERIFICABLE_NO_ID_IGNORED =",
                        {
                            "reason": (
                                "quoted_message_not_found"
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
                            "verifiable_no_id_"
                            "message_not_found"
                        ),
                    }

                no_record_pending = load_pending(
                    no_record_request_key
                )

                if not no_record_pending:
                    print(
                        "RFC_VERIFICABLE_NO_ID_IGNORED =",
                        {
                            "reason": (
                                "pending_expired_or_missing"
                            ),
                            "request_key": (
                                no_record_request_key
                            ),
                        },
                        flush=True,
                    )

                    return {
                        "ok": True,
                        "ignored": (
                            "verifiable_no_id_"
                            "pending_missing"
                        ),
                    }

                no_record_expected_group = (
                    no_record_pending.get(
                        "provider_group_jid"
                    )
                    or ""
                ).strip()

                no_record_expected_instance = (
                    no_record_pending.get(
                        "provider_instance"
                    )
                    or ""
                ).strip()

                if (
                    no_record_expected_group
                    and no_record_expected_group
                    != remote_jid
                ):
                    print(
                        "RFC_VERIFIABLE_NO_ID_"
                        "PROVIDER_GROUP_MISMATCH =",
                        {
                            "request_key": (
                                no_record_request_key
                            ),
                            "expected_group": (
                                no_record_expected_group
                            ),
                            "received_group": (
                                remote_jid
                            ),
                        },
                        flush=True,
                    )

                    return {
                        "ok": True,
                        "ignored": (
                            "verifiable_no_id_"
                            "provider_group_mismatch"
                        ),
                    }

                if (
                    no_record_expected_instance
                    and no_record_expected_instance
                    != instance_name
                ):
                    print(
                        "RFC_VERIFIABLE_NO_ID_"
                        "PROVIDER_INSTANCE_MISMATCH =",
                        {
                            "request_key": (
                                no_record_request_key
                            ),
                            "expected_instance": (
                                no_record_expected_instance
                            ),
                            "received_instance": (
                                instance_name
                            ),
                        },
                        flush=True,
                    )

                    return {
                        "ok": True,
                        "ignored": (
                            "verifiable_no_id_"
                            "provider_instance_mismatch"
                        ),
                    }

                # Impide procesar dos veces la misma
                # respuesta negativa del proveedor.
                if not claim_provider_result(
                    no_record_request_key
                ):
                    print(
                        "RFC_VERIFICABLE_NO_ID_DUPLICATE =",
                        {
                            "request_key": (
                                no_record_request_key
                            ),
                            "quoted_message_id": (
                                quoted_message_id
                            ),
                        },
                        flush=True,
                    )

                    return {
                        "ok": True,
                        "ignored": (
                            "verifiable_no_id_"
                            "already_claimed"
                        ),
                    }

                no_record_client_group = (
                    no_record_pending.get(
                        "client_group_jid"
                    )
                    or ""
                ).strip()

                no_record_client_instance = (
                    no_record_pending.get(
                        "client_instance"
                    )
                    or MAIN_PANEL_INSTANCE
                ).strip()

                no_record_requester_label = (
                    no_record_pending.get(
                        "requester_label"
                    )
                    or no_record_pending.get(
                        "requester_name"
                    )
                    or "Usuario"
                ).strip()

                no_record_inflight_key = (
                    no_record_pending.get(
                        "inflight_key"
                    )
                    or ""
                ).strip()

                stored_provider_message_id = (
                    no_record_pending.get(
                        "provider_message_id"
                    )
                    or quoted_message_id
                    or ""
                ).strip()

                try:
                    no_record_identifier = (
                        no_record_pending.get(
                            "original_identifier"
                        )
                        or "dato solicitado"
                    ).strip().upper()
                    
                    no_record_original_type = (
                        no_record_pending.get(
                            "original_query_type"
                        )
                        or ""
                    ).strip().upper()
                    
                    if no_record_original_type == "CURP":
                        no_record_identifier_label = (
                            f"la CURP {no_record_identifier}"
                        )
                    elif no_record_original_type == "RFC_ONLY":
                        no_record_identifier_label = (
                            f"el RFC {no_record_identifier}"
                        )
                    else:
                        no_record_identifier_label = (
                            no_record_identifier
                        )
                    
                    send_text(
                        no_record_client_group,
                        (
                            f"⚠️ {no_record_requester_label}, "
                            "no hay registro disponible para "
                            f"{no_record_identifier_label}."
                        ),
                        instance_name=(
                            no_record_client_instance
                        ),
                        fast=True,
                    )

                    print(
                        "RFC_VERIFICABLE_NO_ID_SENT =",
                        {
                            "request_key": (
                                no_record_request_key
                            ),
                            "client_group": (
                                no_record_client_group
                            ),
                            "client_instance": (
                                no_record_client_instance
                            ),
                            "original_identifier": (
                                no_record_pending.get(
                                    "original_identifier"
                                )
                            ),
                        },
                        flush=True,
                    )

                except Exception as no_record_send_exc:
                    print(
                        "RFC_VERIFICABLE_NO_ID_"
                        "CLIENT_SEND_ERROR =",
                        {
                            "request_key": (
                                no_record_request_key
                            ),
                            "client_group": (
                                no_record_client_group
                            ),
                            "error": repr(
                                no_record_send_exc
                            ),
                        },
                        flush=True,
                    )

                finally:
                    # Libera el bloqueo de la solicitud para que
                    # el cliente pueda mandarla nuevamente.
                    if no_record_inflight_key:
                        try:
                            request_queue.connection.delete(
                                no_record_inflight_key
                            )
                        except Exception as inflight_exc:
                            print(
                                "RFC_VERIFICABLE_NO_ID_"
                                "INFLIGHT_RELEASE_ERROR =",
                                repr(inflight_exc),
                                flush=True,
                            )

                    # Elimina el pendiente y la asociación con
                    # el mensaje enviado al proveedor.
                    finish_pending(
                        no_record_request_key,
                        provider_message_id=(
                            stored_provider_message_id
                        ),
                    )

                return {
                    "ok": True,
                    "no_record": True,
                    "flow": "RFC_VERIFICABLE",
                    "request_key": (
                        no_record_request_key
                    ),
                }

            provider_pairs = (
                extract_rfc_idcif_pairs(
                    text
                )
            )

            if not provider_pairs:
                print(
                    "RFC_VERIFIABLE_PROVIDER_INVALID =",
                    {
                        "reason": (
                            "no_rfc_idcif_pairs"
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

            is_batch_provider_response = (
                len(provider_pairs) > 1
            )

            results = []

            for (
                pair_index,
                (
                    provider_rfc,
                    provider_idcif,
                ),
            ) in enumerate(
                provider_pairs,
                start=1,
            ):
                try:
                    result = (
                        _queue_one_verifiable_provider_pair(
                            provider_rfc=(
                                provider_rfc
                            ),
                            provider_idcif=(
                                provider_idcif
                            ),
                            remote_jid=(
                                remote_jid
                            ),
                            instance_name=(
                                instance_name
                            ),
                            provider_response_msg_id=(
                                msg_id
                            ),

                            # Una cita sirve únicamente cuando
                            # la respuesta contiene una pareja.
                            # En listas cada RFC se relaciona
                            # contra su pendiente individual.
                            quoted_message_id=(
                                quoted_message_id
                                if not is_batch_provider_response
                                else ""
                            ),
                        )
                    )

                except Exception as pair_exc:
                    result = {
                        "ok": False,
                        "queued": False,
                        "rfc": provider_rfc,
                        "idcif": (
                            provider_idcif
                        ),
                        "reason": (
                            "pair_processing_exception"
                        ),
                        "error": repr(
                            pair_exc
                        ),
                    }

                result["pair_index"] = (
                    pair_index
                )

                results.append(result)

            queued_results = [
                item
                for item in results
                if item.get("queued")
            ]

            failed_results = [
                item
                for item in results
                if not item.get("queued")
            ]

            print(
                "RFC_VERIFIABLE_BATCH_RESULT =",
                {
                    "provider_group": (
                        remote_jid
                    ),
                    "provider_instance": (
                        instance_name
                    ),
                    "provider_message_id": (
                        msg_id
                    ),
                    "pairs_detected": len(
                        provider_pairs
                    ),
                    "pairs_queued": len(
                        queued_results
                    ),
                    "pairs_failed": len(
                        failed_results
                    ),
                    "results": results,
                },
                flush=True,
            )

            return {
                "ok": True,
                "flow": "RFC_VERIFICABLE",
                "batch": (
                    is_batch_provider_response
                ),
                "pairs_detected": len(
                    provider_pairs
                ),
                "pairs_queued": len(
                    queued_results
                ),
                "pairs_failed": len(
                    failed_results
                ),
                "results": results,
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

            provider_text = (
                f"{original_identifier}"
            )

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
