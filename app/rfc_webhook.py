import os
import re
import json
import hashlib
import time

from fastapi import APIRouter, Request

from app.queue import request_queue
from app.services.evolution import send_text
from app.db import SessionLocal
from app.models import AuthorizedGroup, BotControl

from app.verifiable_flow import (
    VERIFIABLE_PROVIDER_GROUP,
    VERIFIABLE_PROVIDER_INSTANCE,
    VERIFIABLE_TIMEOUT_SEC,
    parse_verifiable_request,
    extract_rfc_idcif,
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
            "⚠️ No detecté una solicitud válida.\n\n"
            "Puedes enviar:\n"
            "• CURP\n"
            "• RFC\n"
            "• RFC + IDCIF\n"
            "• QR del SAT en imagen"
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


def _dedupe_key(instance: str, remote_jid: str, requester: str, query: str, msg_id: str) -> str:
    base = f"{instance}|{remote_jid}|{requester}|{query or msg_id}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


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
        is_verifiable_provider_group = (
            bool(VERIFIABLE_PROVIDER_GROUP)
            and remote_jid
            == VERIFIABLE_PROVIDER_GROUP
            and instance_name
            == VERIFIABLE_PROVIDER_INSTANCE
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

            provider_rfc, provider_idcif = (
                extract_rfc_idcif(text)
            )
            
            if (
                not provider_rfc
                or not provider_idcif
            ):
                print(
                    "RFC_VERIFIABLE_PROVIDER_INVALID =",
                    {
                        "reason": (
                            "missing_rfc_or_idcif"
                        ),
                        "quoted_message_id": (
                            quoted_message_id
                        ),
                        "text": text,
                        "rfc": provider_rfc,
                        "idcif": provider_idcif,
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
            
            
            verifiable_key = ""
            pending = {}
            matched_without_quote = False
            matched_by = ""
            
            
            # Método principal: mensaje citado.
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
            
            
            # Respaldo: el proveedor no citó el mensaje.
            if not verifiable_key:
                fallback_match = (
                    find_pending_request_by_provider_rfc(
                        provider_rfc
                    )
                )
            
                if fallback_match.get("ok"):
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
            
                    print(
                        "RFC_VERIFIABLE_PROVIDER_"
                        "MATCHED_WITHOUT_QUOTE =",
                        {
                            "request_key": (
                                verifiable_key
                            ),
                            "provider_rfc": (
                                provider_rfc
                            ),
                            "matched_by": matched_by,
                            "original_type": (
                                fallback_match.get(
                                    "original_type"
                                )
                            ),
                            "original_identifier": (
                                fallback_match.get(
                                    "original_identifier"
                                )
                            ),
                        },
                        flush=True,
                    )
            
                else:
                    reason = (
                        fallback_match.get("reason")
                        or "unknown"
                    )
            
                    matches = (
                        fallback_match.get("matches")
                        or []
                    )
            
                    print(
                        "RFC_VERIFIABLE_PROVIDER_"
                        "UNMATCHED_WITHOUT_QUOTE =",
                        {
                            "reason": reason,
                            "provider_rfc": (
                                provider_rfc
                            ),
                            "quoted_message_id": (
                                quoted_message_id
                            ),
                            "matches_count": len(
                                matches
                            ),
                            "candidate_request_keys": [
                                item.get("request_key")
                                for item in matches
                                if isinstance(item, dict)
                            ],
                        },
                        flush=True,
                    )
            
                    return {
                        "ok": True,
                        "ignored": (
                            "verifiable_provider_"
                            + reason
                        ),
                    }

            if not verifiable_key:
                print(
                    "RFC_VERIFIABLE_PROVIDER_IGNORED =",
                    {
                        "reason": (
                            "provider_message_not_found"
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
                        "verifiable_provider_"
                        "message_not_found"
                    ),
                }

            if not pending:
                pending = load_pending(
                    verifiable_key
                )
            
            if not pending:
                print(
                    "RFC_VERIFIABLE_PROVIDER_IGNORED =",
                    {
                        "reason": (
                            "pending_expired_or_missing"
                        ),
                        "request_key": (
                            verifiable_key
                        ),
                    },
                    flush=True,
                )

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_pending_missing"
                    ),
                }

            original_type = (
                pending.get("original_query_type")
                or ""
            ).strip().upper()

            original_identifier = (
                pending.get("original_identifier")
                or ""
            ).strip().upper()

            # Cuando el cliente originalmente envió RFC,
            # la respuesta debe conservar ese mismo RFC.
            if (
                original_type == "RFC_ONLY"
                and provider_rfc
                != original_identifier
            ):
                print(
                    "RFC_VERIFIABLE_RFC_MISMATCH =",
                    {
                        "expected": (
                            original_identifier
                        ),
                        "received": provider_rfc,
                        "request_key": (
                            verifiable_key
                        ),
                    },
                    flush=True,
                )

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_rfc_mismatch"
                    ),
                }

            if not claim_provider_result(
                verifiable_key
            ):
                print(
                    "RFC_VERIFIABLE_PROVIDER_"
                    "DUPLICATE_RESULT =",
                    verifiable_key,
                    flush=True,
                )

                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_result_already_"
                        "claimed"
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
                pending.get("client_msg_id")
                or ""
            ).strip()

            normal_request_key = (
                pending.get("normal_request_key")
                or verifiable_key
            ).strip()

            inflight_key = (
                pending.get("inflight_key")
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
                "query_type": "RFC_VERIFICABLE",
                "forced_success_kind": "RFC_VERIFICABLE",
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

                # Trazabilidad.
                "is_verifiable": True,
                "verifiable_price": float(
                    pending.get("verifiable_price")
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
                    msg_id
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

            except Exception as enqueue_exc:
                release_provider_result_claim(
                    verifiable_key
                )

                print(
                    "RFC_VERIFIABLE_RESULT_"
                    "ENQUEUE_ERROR =",
                    {
                        "request_key": (
                            verifiable_key
                        ),
                        "error": repr(
                            enqueue_exc
                        ),
                    },
                    flush=True,
                )

                raise

            finish_pending(
                verifiable_key,
                provider_message_id=(
                    stored_provider_message_id
                ),
            )

            print(
                "RFC_VERIFIABLE_RESULT_QUEUED =",
                {
                    "job_id": job.id,
                    "request_key": (
                        verifiable_key
                    ),
                    "client_group": (
                        client_group_jid
                    ),
                    "client_instance": (
                        client_instance
                    ),
                    "rfc": provider_rfc,
                    "idcif": provider_idcif,
                },
                flush=True,
            )

            return {
                "ok": True,
                "queued": True,
                "flow": "RFC_VERIFICABLE",
                "job_id": job.id,
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

            if not VERIFIABLE_PROVIDER_GROUP:
                print(
                    "RFC_VERIFIABLE_CONFIG_ERROR =",
                    "provider_group_empty",
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
                        "verifiable_provider_"
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
                ex=VERIFIABLE_TIMEOUT_SEC,
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
            }

            save_pending(
                command_key,
                pending_payload,
            )

            try:
                provider_response = send_text(
                    VERIFIABLE_PROVIDER_GROUP,
                    provider_text,
                    instance_name=(
                        VERIFIABLE_PROVIDER_INSTANCE
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

                stored_provider_message_id = (
                    pending.get(
                        "provider_message_id"
                    )
                    or quoted_message_id
                    or ""
                ).strip()
                
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
                        "provider_group": (
                            VERIFIABLE_PROVIDER_GROUP
                        ),
                        "provider_instance": (
                            VERIFIABLE_PROVIDER_INSTANCE
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
                        "tu RFC verificable fue enviado "
                        "al proveedor.\n"
                        "Se entregará automáticamente "
                        "cuando responda."
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
                    "provider_group": (
                        VERIFIABLE_PROVIDER_GROUP
                    ),
                    "provider_instance": (
                        VERIFIABLE_PROVIDER_INSTANCE
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
