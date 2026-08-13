#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import difflib
import py_compile
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path.cwd()
WEBHOOK = ROOT / "app" / "rfc_webhook.py"
WORKER = ROOT / "worker_jobs.py"


class PatchError(RuntimeError):
    pass


def replace_exact(text, old, new, label, count=1):
    found = text.count(old)
    if found != count:
        raise PatchError(f"{label}: esperaba {count}, encontré {found}")
    return text.replace(old, new, count)


def insert_before_once(text, marker, block, label):
    found = text.count(marker)
    if found != 1:
        raise PatchError(f"{label}: marcador esperado 1 vez, encontrado {found}")
    if block.strip() in text:
        raise PatchError(f"{label}: bloque ya insertado")
    return text.replace(marker, block.rstrip() + "\n\n" + marker, 1)


WEB_HELPERS = r'''
# ============================================================
# MENSAJES CLIENTE RFC
# ============================================================

def _client_request_type_info(query_type: str, count: int = 1) -> dict:
    query_type = str(query_type or "").strip().upper()
    try:
        count = max(int(count or 1), 1)
    except Exception:
        count = 1

    if query_type == "CURP":
        return {"label": "CURP", "received": "recibida" if count == 1 else "recibidas"}
    if query_type == "RFC_ONLY":
        return {"label": "RFC", "received": "recibido" if count == 1 else "recibidos"}
    if query_type == "RFC_IDCIF":
        return {"label": "RFC + IDCIF", "received": "recibido" if count == 1 else "recibidos"}
    if query_type in {"QR", "QR_TEXT", "IMAGE", "DOCUMENT"}:
        return {"label": "QR SAT", "received": "recibido" if count == 1 else "recibidos"}
    if query_type == "RFC_VERIFICABLE":
        return {
            "label": "RFC verificable" if count == 1 else "RFC verificables",
            "received": "recibido" if count == 1 else "recibidos",
        }
    return {
        "label": "solicitud" if count == 1 else "solicitudes",
        "received": "recibida" if count == 1 else "recibidas",
    }


def _client_received_message(*, requester_label: str, query_type: str, count: int = 1) -> str:
    requester_label = str(requester_label or "Usuario").strip()
    try:
        count = max(int(count or 1), 1)
    except Exception:
        count = 1
    info = _client_request_type_info(query_type, count)
    process_text = "⏳ Se procesará individualmente." if count == 1 else "⏳ Se procesarán individualmente."
    return (
        f"📥 {count} {info['label']} {info['received']}\n"
        f"👤 {requester_label}\n"
        f"{process_text}"
    )


def _client_mixed_received_message(*, requester_label: str, total: int, type_counts: dict) -> str:
    requester_label = str(requester_label or "Usuario").strip()
    try:
        total = max(int(total or 1), 1)
    except Exception:
        total = 1
    lines = [f"📥 {total} solicitudes recibidas", f"👤 {requester_label}", ""]
    preferred = ["CURP", "RFC_ONLY", "RFC_IDCIF", "QR", "RFC_VERIFICABLE"]
    normalized = {}
    for key, value in (type_counts or {}).items():
        k = str(key or "").strip().upper()
        if k in {"QR_TEXT", "IMAGE", "DOCUMENT"}:
            k = "QR"
        try:
            value = int(value or 0)
        except Exception:
            value = 0
        if value > 0:
            normalized[k] = normalized.get(k, 0) + value
    for key in preferred:
        qty = normalized.get(key, 0)
        if qty <= 0:
            continue
        info = _client_request_type_info(key, qty)
        lines.append(f"• {qty} {info['label']}")
    lines += ["", "⏳ Se procesarán individualmente."]
    return "\n".join(lines)


def _client_verifiable_received_message(*, requester_label: str, count: int = 1) -> str:
    requester_label = str(requester_label or "Usuario").strip()
    try:
        count = max(int(count or 1), 1)
    except Exception:
        count = 1
    if count == 1:
        return (
            "🔎 1 RFC verificable recibido\n"
            f"👤 {requester_label}\n"
            "⏳ Fue enviado para procesamiento.\n"
            "Te avisaremos cuando tengamos el resultado."
        )
    return (
        f"🔎 {count} RFC verificables recibidos\n"
        f"👤 {requester_label}\n"
        "⏳ Fueron enviados para procesamiento.\n"
        "Los resultados se entregarán individualmente."
    )


def _client_position_line(*, batch_index: int = 1, batch_total: int = 1) -> str:
    try:
        batch_index = max(int(batch_index or 1), 1)
    except Exception:
        batch_index = 1
    try:
        batch_total = max(int(batch_total or 1), 1)
    except Exception:
        batch_total = 1
    if batch_total <= 1:
        return ""
    return f"📌 Solicitud {batch_index} de {batch_total}"


def _client_identity_lines(*, query_type: str, identifier: str = "", rfc: str = "", idcif: str = "", provider_rfc: str = "", provider_idcif: str = "", result_mode: bool = False) -> str:
    query_type = str(query_type or "").strip().upper()
    identifier = str(identifier or "").strip().upper()
    rfc = str(rfc or "").strip().upper()
    idcif = str(idcif or "").strip()
    provider_rfc = str(provider_rfc or "").strip().upper()
    provider_idcif = str(provider_idcif or "").strip()
    lines = []
    if query_type == "CURP":
        if identifier:
            label = "🪪 CURP solicitada:" if result_mode else "🪪 CURP:"
            lines.append(f"{label} {identifier}")
        if result_mode and provider_rfc:
            lines.append(f"🧾 RFC localizado: {provider_rfc}")
        if result_mode and provider_idcif:
            lines.append(f"🔢 IDCIF: {provider_idcif}")
    elif query_type in {"RFC_ONLY", "RFC_VERIFICABLE"}:
        effective_rfc = identifier or rfc or provider_rfc
        if effective_rfc:
            lines.append(f"🧾 RFC: {effective_rfc}")
        if result_mode and provider_idcif:
            lines.append(f"🔢 IDCIF: {provider_idcif}")
    elif query_type == "RFC_IDCIF":
        effective_rfc = rfc or identifier
        if effective_rfc:
            lines.append(f"🧾 RFC: {effective_rfc}")
        if idcif:
            lines.append(f"🔢 IDCIF: {idcif}")
    elif query_type in {"QR", "QR_TEXT", "IMAGE", "DOCUMENT"}:
        lines.append("📷 QR SAT")
    elif identifier:
        lines.append(f"📄 Dato: {identifier}")
    return "\n".join(lines)


def _client_status_message(*, title: str, requester_label: str, query_type: str = "", identifier: str = "", rfc: str = "", idcif: str = "", provider_rfc: str = "", provider_idcif: str = "", body: str = "", batch_index: int = 1, batch_total: int = 1, include_identity: bool = True, result_mode: bool = False) -> str:
    lines = [title]
    position = _client_position_line(batch_index=batch_index, batch_total=batch_total)
    if position:
        lines.append(position)
    requester_label = str(requester_label or "Usuario").strip()
    lines.append(f"👤 {requester_label}")
    if include_identity:
        identity = _client_identity_lines(
            query_type=query_type,
            identifier=identifier,
            rfc=rfc,
            idcif=idcif,
            provider_rfc=provider_rfc,
            provider_idcif=provider_idcif,
            result_mode=result_mode,
        )
        if identity:
            lines.append(identity)
    if body:
        lines += ["", body.strip()]
    return "\n".join(lines)
'''.strip()


WORKER_HELPERS = r'''
# ============================================================
# MENSAJES CLIENTE RFC - WORKER
# ============================================================

def _job_batch_position(job_data: dict) -> str:
    try:
        index = max(int(job_data.get("batch_index") or 1), 1)
    except Exception:
        index = 1
    try:
        total = max(int(job_data.get("batch_total") or 1), 1)
    except Exception:
        total = 1
    if total <= 1:
        return ""
    return f"📌 Solicitud {index} de {total}"


def _job_request_identity(job_data: dict) -> dict:
    if bool(job_data.get("is_verifiable")):
        return {
            "query_type": str(job_data.get("verifiable_original_type") or "").strip().upper(),
            "identifier": str(job_data.get("verifiable_original_identifier") or "").strip().upper(),
            "provider_rfc": str(job_data.get("provider_rfc") or "").strip().upper(),
            "provider_idcif": str(job_data.get("provider_idcif") or "").strip(),
        }
    query_type = str(job_data.get("query_type") or "").strip().upper()
    src = str(job_data.get("query") or job_data.get("original_text") or "").strip().upper()
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


def _job_identity_lines(job_data: dict, *, result_mode: bool = False, result_rfc: str = "", result_idcif: str = "") -> str:
    identity = _job_request_identity(job_data)
    query_type = identity.get("query_type")
    identifier = identity.get("identifier")
    original_rfc = identity.get("rfc")
    original_idcif = identity.get("idcif")
    provider_rfc = str(result_rfc or identity.get("provider_rfc") or "").strip().upper()
    provider_idcif = str(result_idcif or identity.get("provider_idcif") or "").strip()
    lines = []
    if query_type == "CURP":
        if identifier:
            label = "🪪 CURP solicitada:" if result_mode else "🪪 CURP:"
            lines.append(f"{label} {identifier}")
        if result_mode and provider_rfc:
            lines.append(f"🧾 RFC localizado: {provider_rfc}")
        if result_mode and provider_idcif:
            lines.append(f"🔢 IDCIF: {provider_idcif}")
    elif query_type in {"RFC_ONLY", "RFC_VERIFICABLE"}:
        effective_rfc = identifier or provider_rfc
        if effective_rfc:
            lines.append(f"🧾 RFC: {effective_rfc}")
        if result_mode and provider_idcif:
            lines.append(f"🔢 IDCIF: {provider_idcif}")
    elif query_type == "RFC_IDCIF":
        if original_rfc:
            lines.append(f"🧾 RFC: {original_rfc}")
        if original_idcif:
            lines.append(f"🔢 IDCIF: {original_idcif}")
    elif query_type in {"QR", "QR_TEXT", "IMAGE", "DOCUMENT"}:
        lines.append("📷 QR SAT")
    return "\n".join(lines)


def _job_client_message(job_data: dict, *, title: str, requester_label: str = "", body: str = "", include_identity: bool = True, result_mode: bool = False, result_rfc: str = "", result_idcif: str = "") -> str:
    lines = [title]
    position = _job_batch_position(job_data)
    if position:
        lines.append(position)
    requester_label = str(requester_label or job_data.get("requester_label") or job_data.get("requester_name") or "Usuario").strip()
    lines.append(f"👤 {requester_label}")
    if include_identity:
        identity = _job_identity_lines(job_data, result_mode=result_mode, result_rfc=result_rfc, result_idcif=result_idcif)
        if identity:
            lines.append(identity)
    if body:
        lines += ["", body.strip()]
    return "\n".join(lines)
'''.strip()


def patch_webhook(text: str) -> str:
    text = insert_before_once(text, "def _extract_sent_message_id(", WEB_HELPERS, "helpers webhook")

    text = replace_exact(text,
'''    return {
        "ok": False,
        "type": "INVALID_INPUT",
        "error": (
            ""
        ),
    }
''',
'''    return {
        "ok": False,
        "type": "INVALID_INPUT",
        "error": (
            "⚠️ Solicitud no reconocida\\n\\n"
            "Puedes enviar:\\n"
            "• CURP\\n"
            "• RFC\\n"
            "• RFC + IDCIF\\n"
            "• QR SAT\\n"
            "• CURP/RFC VERIFICABLE"
        ),
    }
''', "INVALID_INPUT")

    text = replace_exact(text,
'''        job_data = {
            "requester_number": requester_wa_id,
            "requester_name": push_name,
            "requester_label": requester_label,
            "group_jid": remote_jid,
''',
'''        job_data = {
            "requester_number": requester_wa_id,
            "requester_name": push_name,
            "requester_label": requester_label,
            "batch_index": 1,
            "batch_total": 1,
            "group_jid": remote_jid,
''', "batch normal")

    text = replace_exact(text,
'''        "requester_label": requester_label,
        "group_jid": client_group_jid,
''',
'''        "requester_label": requester_label,
        "batch_index": int(pending.get("batch_index") or 1),
        "batch_total": int(pending.get("batch_total") or 1),
        "group_jid": client_group_jid,
''', "batch verificable job")

    text = replace_exact(text,
'''                "requester_label": (
                    requester_label
                ),
                "client_msg_id": msg_id,
''',
'''                "requester_label": (
                    requester_label
                ),
                "batch_index": 1,
                "batch_total": 1,
                "client_msg_id": msg_id,
''', "batch verificable pending")

    text = replace_exact(text,
'''                    (
                        f"{_bot_label_from_db(instance_name)}\\n"
                        f"Solicitud recibida de {requester_label}.\\n"
                        "Esto puede tardar unos segundos..."
                    ),
''',
'''                    _client_received_message(
                        requester_label=requester_label,
                        query_type=(parsed.get("type") or ""),
                        count=1,
                    ),
''', "ACK normal")

    text = replace_exact(text,
'''                    (
                        f"🔎 {requester_label}, "
                        "tu RFC verificable fue enviado.\\n"
                        "Se entregará cuando el proveedor responda."
                    ),
''',
'''                    _client_verifiable_received_message(
                        requester_label=requester_label,
                        count=1,
                    ),
''', "ACK verificable")

    text = replace_exact(text,
'''                        (
                            f"✅ {requester_label}, "
                            "esta solicitud verificable "
                            "ya fue entregada durante las "
                            "últimas 24 horas.\\n\\n"
                            "Podrás volver a solicitarla "
                            f"aproximadamente en "
                            f"{remaining_hours} h."
                        ),
''',
'''                        _client_status_message(
                            title="✅ Solicitud ya entregada",
                            requester_label=requester_label,
                            query_type=original_query_type,
                            identifier=original_identifier,
                            body=(
                                "Esta solicitud verificable fue "
                                "entregada durante las últimas 24 horas.\\n"
                                "Podrás volver a solicitarla "
                                f"aproximadamente en {remaining_hours} h."
                            ),
                        ),
''', "24h")

    text = replace_exact(text,
'''                            (
                                f"⏳ {requester_label}, "
                                "esta solicitud verificable "
                                "ya está en proceso.\\n\\n"
                                "No es necesario volver "
                                "a enviarla."
                            ),
''',
'''                            _client_status_message(
                                title="⏳ RFC verificable en proceso",
                                requester_label=requester_label,
                                query_type=original_query_type,
                                identifier=original_identifier,
                                body=(
                                    "Esta solicitud ya está siendo procesada.\\n"
                                    "No es necesario enviarla nuevamente."
                                ),
                            ),
''', "duplicado verificable 1")

    text = replace_exact(text,
'''                            (
                                f"⏳ {requester_label}, "
                                "este RFC verificable "
                                "ya está siendo procesado."
                            ),
''',
'''                            _client_status_message(
                                title="⏳ RFC verificable en proceso",
                                requester_label=requester_label,
                                query_type=original_query_type,
                                identifier=original_identifier,
                                body=(
                                    "Esta solicitud ya está siendo procesada.\\n"
                                    "No es necesario enviarla nuevamente."
                                ),
                            ),
''', "duplicado verificable 2")

    text = replace_exact(text,
'''                        (
                            f"⚠️ {requester_label}, "
                            "no hay proveedores de RFC "
                            "verificable activos."
                        ),
''',
'''                        (
                            "⚠️ RFC verificable temporalmente no disponible\\n"
                            f"👤 {requester_label}\\n\\n"
                            "No fue posible iniciar el procesamiento en este momento.\\n"
                            "Intenta nuevamente más tarde."
                        ),
''', "sin proveedor")

    # SIN ID: sustituimos solo el mensaje, sin tocar cierre/contabilidad.
    text = replace_exact(text,
'''            (
                f"⚠️ {requester_label}, "
                "el proveedor informó que no hay "
                "ID disponible para "
                f"{identifier_label}."
            ),
''',
'''            _client_status_message(
                title="⚠️ RFC verificable sin ID disponible",
                requester_label=requester_label,
                query_type=original_type,
                identifier=original_identifier,
                body=(
                    "No hay IDCIF disponible actualmente "
                    "para esta solicitud."
                ),
                batch_index=int(pending.get("batch_index") or 1),
                batch_total=int(pending.get("batch_total") or 1),
            ),
''', "SIN ID")

    message_swaps = [
        ('''                            f"⚠️ {requester_label}, "\n                            "este bot no tiene configurado "\n                            "RFC verificable."''',
         '''                            "⚠️ RFC verificable no disponible\\n"\n                            f"👤 {requester_label}\\n\\n"\n                            "El servicio de RFC verificable no está configurado."''', "bot sin config"),
        ('''                            f"⚠️ {requester_label}, "\n                            "RFC verificable no está activo "\n                            "para este bot."''',
         '''                            "⚠️ RFC verificable no disponible\\n"\n                            f"👤 {requester_label}\\n\\n"\n                            "El servicio de RFC verificable no está activo actualmente."''', "verif off bot"),
        ('''                            f"⚠️ {requester_label}, "\n                            "este grupo no está configurado "\n                            "para RFC verificable."''',
         '''                            "⚠️ RFC verificable no disponible\\n"\n                            f"👤 {requester_label}\\n\\n"\n                            "Este grupo no tiene configurado el servicio de RFC verificable."''', "grupo sin config"),
        ('''                            f"⚠️ {requester_label}, "\n                            "RFC verificable no está activo "\n                            "para este grupo."''',
         '''                            "⚠️ RFC verificable no disponible\\n"\n                            f"👤 {requester_label}\\n\\n"\n                            "Este grupo no tiene activo el servicio de RFC verificable."''', "verif off grupo"),
        ('''                            f"⚠️ {requester_label}, "\n                            "este bot no está activo."''',
         '''                            "⚠️ Servicio no disponible\\n"\n                            f"👤 {requester_label}\\n\\n"\n                            "El servicio está temporalmente inactivo."''', "bot inactive"),
        ('''                            f"⚠️ {requester_label}, "\n                            "este bot está bloqueado."''',
         '''                            "⚠️ Servicio no disponible\\n"\n                            f"👤 {requester_label}\\n\\n"\n                            "El servicio está temporalmente bloqueado."''', "bot blocked"),
        ('''                            f"⚠️ {requester_label}, "\n                            "este bot ya no tiene RFC "\n                            "verificables disponibles."''',
         '''                            "⚠️ RFC verificable no disponible\\n"\n                            f"👤 {requester_label}\\n\\n"\n                            "No hay RFC verificables disponibles en este momento."''', "sin verif bot"),
    ]
    for old, new, label in message_swaps:
        text = replace_exact(text, old, new, label)

    return text


def patch_worker(text: str) -> str:
    text = insert_before_once(text, "def _panel_stats_key(", WORKER_HELPERS, "helpers worker")

    text = replace_exact(text,
'''                (
                    f"⚠️ {requester_label}, "
                    "este servicio fue desactivado "
                    "antes de procesar la solicitud."
                ),
''',
'''                _job_client_message(
                    job_data,
                    title="⚠️ Servicio temporalmente no disponible",
                    requester_label=requester_label,
                    body=(
                        "El servicio fue desactivado antes "
                        "de procesar esta solicitud."
                    ),
                ),
''', "servicio desactivado")

    text = replace_exact(text,
'''        if not result.get("ok"):
            err = result.get("error") or "No fue posible generar el documento."
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} {err}",
                instance_name=instance_name
            )
            return
''',
'''        if not result.get("ok"):
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
                        "Ocurrió una interrupción temporal.\\n"
                        "Intenta nuevamente en 2-3 minutos."
                    ),
                ),
                instance_name=instance_name,
            )
            return
''', "raw result error")

    text = replace_exact(text,
'''            client_text = (
                f"{delivery_text}\\n\\n"
                "⚠️ La página del SAT no permitió "
                "generar la constancia.\\n"
                "Se entrega el RFC e IDCIF "
                "localizados por el proveedor.\\n\\n"
                "⏱️ Tiempo total: "
                f"{_format_total_time(elapsed_seconds)}"
            )
''',
'''            client_text = _job_client_message(
                job_data,
                title="⚠️ Constancia no disponible en SAT",
                requester_label=requester_label,
                body=(
                    "El SAT no permitió generar la constancia en este momento.\\n"
                    "Se entregan los datos localizados.\\n\\n"
                    "⏱️ Tiempo total: "
                    f"{_format_total_time(elapsed_seconds)}"
                ),
                result_mode=True,
                result_rfc=delivered_rfc,
                result_idcif=delivered_idcif,
            )
''', "verifiable text")

    text = replace_exact(text, 'f"❌ {requester_label} no se obtuvo enlace del lote.",',
'''_job_client_message(
                        job_data,
                        title="⚠️ No pudimos entregar el lote",
                        requester_label=requester_label,
                        body=(
                            "El lote fue procesado, pero no se obtuvo "
                            "el enlace de entrega."
                        ),
                        include_identity=False,
                    ),''', "zip no url")

    text = replace_exact(text, 'f"❌ {requester_label} no se obtuvo enlace del PDF.",',
'''_job_client_message(
                    job_data,
                    title="⚠️ No pudimos entregar el documento",
                    requester_label=requester_label,
                    body=(
                        "El procesamiento terminó, pero no se obtuvo el enlace del PDF.\\n"
                        "Intenta nuevamente en unos minutos."
                    ),
                ),''', "pdf no url")

    text = replace_exact(text,
'''        if is_verifiable:
            time_caption += (
                "\\nRFC entregado"
            )
''', '', "caption solo tiempo")

    text = replace_exact(text,
'''                else:
                    evolution_send_text_to_group(
                        group_jid,
                        f"❌ {requester_label} fallo {rfc} {idcif}: {err or 'error desconocido'}",
                        instance_name=instance_name
                    )
''',
'''                else:
                    print(
                        "[RFC BATCH ITEM CLIENT ERROR HIDDEN]",
                        {"rfc": rfc, "idcif": idcif, "internal_error": err},
                        flush=True,
                    )
                    evolution_send_text_to_group(
                        group_jid,
                        (
                            "⚠️ No pudimos completar la solicitud\\n"
                            f"👤 {requester_label}\\n"
                            f"🧾 RFC: {rfc or 'N/D'}"
                            + (f"\\n🔢 IDCIF: {idcif}" if idcif else "")
                            + "\\n\\nOcurrió una interrupción temporal.\\n"
                            "Intenta nuevamente en unos minutos."
                        ),
                        instance_name=instance_name
                    )
''', "batch raw error")

    direct_swaps = [
        ('f"⚠️ {requester_label} el QR no corresponde a un enlace oficial del SAT.",',
'''_job_client_message(
                        job_data,
                        title="⚠️ QR no válido para SAT",
                        requester_label=requester_label,
                        body="El QR no corresponde a un enlace oficial del SAT.",
                    ),''', "QR sat"),
        ('f"⚠️ {requester_label} no pude leer el QR. Envíalo más cerca, más nítido y con buena luz.",',
'''_job_client_message(
                        job_data,
                        title="⚠️ No pudimos leer el QR",
                        requester_label=requester_label,
                        body="Envíalo más cerca, más nítido y con buena luz.",
                    ),''', "QR read"),
        ('f"⚠️ {requester_label} ese tipo de archivo aún no es compatible. Envíalo como imagen.",',
'''_job_client_message(
                        job_data,
                        title="⚠️ Archivo no compatible",
                        requester_label=requester_label,
                        body="Ese tipo de archivo aún no es compatible.\\nEnvíalo como imagen.",
                        include_identity=False,
                    ),''', "MIME"),
    ]
    for old, new, label in direct_swaps:
        text = replace_exact(text, old, new, label)

    text = replace_exact(text,
'''                    (
                        f"⚠️ {requester_label}, "
                        f"{client_reason}. "
                        "No se generó la constancia."
                    ),
''',
'''                    _job_client_message(
                        job_data,
                        title="⚠️ Constancia no generada",
                        requester_label=requester_label,
                        body=(f"{client_reason}.\\nNo se generó la constancia."),
                    ),
''', "SAT reason")

    text = replace_exact(text,
'''                    f"⚠️ {requester_label} la CURP no fue encontrada.\\n\\n"
                    "Verifica que esté escrita correctamente y vuelve a enviarla.",
''',
'''                    _job_client_message(
                        job_data,
                        title="⚠️ CURP no localizada",
                        requester_label=requester_label,
                        body="Verifica que esté escrita correctamente y vuelve a enviarla.",
                    ),
''', "CURP not found")

    text = replace_exact(text,
'''                    f"⚠️ {requester_label} el RFC no fue encontrado.\\n\\n"
                    "Verifica que esté escrito correctamente y vuelve a enviarlo.",
''',
'''                    _job_client_message(
                        job_data,
                        title="⚠️ RFC no localizado",
                        requester_label=requester_label,
                        body="Verifica que esté escrito correctamente y vuelve a enviarlo.",
                    ),
''', "RFC not found")

    text = replace_exact(text,
'''                    f"⚠️ {requester_label} se encontró información, pero está incompleta.\\n\\n"
                    "No se generó el documento para evitar datos incorrectos.",
''',
'''                    _job_client_message(
                        job_data,
                        title="⚠️ Información incompleta",
                        requester_label=requester_label,
                        body=(
                            "Se encontró información, pero está incompleta.\\n"
                            "No se generó el documento para evitar entregar datos incorrectos."
                        ),
                    ),
''', "incomplete 1")

    text = replace_exact(text,
'''                    f"⚠️ {requester_label} se encontró información, pero está incompleta.\\n\\n"
                    "No se generó el documento para evitar datos incorrectos. Verifica la CURP/RFC o intenta más tarde.",
''',
'''                    _job_client_message(
                        job_data,
                        title="⚠️ Información incompleta",
                        requester_label=requester_label,
                        body=(
                            "Se encontró información, pero está incompleta.\\n"
                            "No se generó el documento para evitar entregar datos incorrectos.\\n"
                            "Verifica la CURP/RFC o intenta nuevamente más tarde."
                        ),
                    ),
''', "incomplete 2")

    text = replace_exact(text,
'''                    f"⚠️ {requester_label} no se encontró información para esta CURP.\\n"
                    "Verifica que esté escrita correctamente o que se encuentre certificada.",
''',
'''                    _job_client_message(
                        job_data,
                        title="⚠️ CURP sin información suficiente",
                        requester_label=requester_label,
                        body=(
                            "No se encontró información suficiente para esta CURP.\\n"
                            "Verifica que esté escrita correctamente o que se encuentre certificada."
                        ),
                    ),
''', "CURP insufficient")

    text = replace_exact(text,
'''                    f"⚠️ {requester_label}, el servicio de consulta de CURP "
                    "no respondió a tiempo.\\n"
                    "La CURP no fue marcada como inexistente. "
                    "Intenta nuevamente en unos momentos.",
''',
'''                    _job_client_message(
                        job_data,
                        title="⚠️ Consulta de CURP sin respuesta",
                        requester_label=requester_label,
                        body=(
                            "El servicio de consulta no respondió a tiempo.\\n"
                            "La CURP no fue marcada como inexistente.\\n"
                            "Intenta nuevamente en unos momentos."
                        ),
                    ),
''', "CURP timeout")

    text = replace_exact(text,
'f"⚠️ {requester_label} ocurrió una interrupción procesando la solicitud. Intenta de nuevo en 2-3 minutos",',
'''_job_client_message(
                        job_data,
                        title="⚠️ No pudimos completar la solicitud",
                        requester_label=requester_label,
                        body="Ocurrió una interrupción temporal.\\nIntenta nuevamente en 2-3 minutos.",
                    ),''', "HTTP generic")

    text = replace_exact(text,
'''                    (
                        f"⚠️ {requester_label} ocurrió una "
                        "interrupción procesando la solicitud. "
                        "Intenta de nuevo en 2-3 minutos"
                    ),
''',
'''                    _job_client_message(
                        job_data,
                        title="⚠️ No pudimos completar la solicitud",
                        requester_label=requester_label,
                        body="Ocurrió una interrupción temporal.\\nIntenta nuevamente en 2-3 minutos.",
                    ),
''', "exception generic")

    # Comercial: solo textos exactos críticos acordados.
    commercial = [
        ('f"⚠️ {requester_label} no hay configuración RFC para este bot."', '"⚠️ Servicio no disponible\\n" f"👤 {requester_label}\\n\\n" "No fue posible validar la configuración del servicio RFC."', "cfg"),
        ('f"⚠️ {requester_label} este bot interno no está activo."', '"⚠️ Servicio no disponible\\n" f"👤 {requester_label}\\n\\n" "El servicio está temporalmente inactivo."', "inactive"),
        ('f"⚠️ {requester_label} este bot interno está bloqueado."', '"⚠️ Servicio no disponible\\n" f"👤 {requester_label}\\n\\n" "El servicio está temporalmente bloqueado."', "blocked"),
        ('f"⚠️ {requester_label} este grupo ya no tiene RFC CLON disponibles."', '"⚠️ RFC CLON no disponible\\n" f"👤 {requester_label}\\n\\n" "Este grupo ya no tiene RFC CLON disponibles."', "clon group"),
        ('f"⚠️ {requester_label} el panel ya no tiene RFC CLON disponibles."', '"⚠️ RFC CLON no disponible\\n" f"👤 {requester_label}\\n\\n" "No hay RFC CLON disponibles en este momento."', "clon global"),
        ('f"⚠️ {requester_label} el plan semanal RFC IDCIF no está activo."', '"⚠️ RFC IDCIF no disponible\\n" f"👤 {requester_label}\\n\\n" "El servicio RFC IDCIF no está activo actualmente."', "idcif inactive"),
        ('f"⚠️ {requester_label} el plan semanal RFC IDCIF no tiene vigencia activa."', '"⚠️ RFC IDCIF no disponible\\n" f"👤 {requester_label}\\n\\n" "El servicio RFC IDCIF no tiene una vigencia activa."', "idcif vigencia"),
        ('f"⚠️ {requester_label} el plan semanal RFC IDCIF venció."', '"⚠️ RFC IDCIF no disponible\\n" f"👤 {requester_label}\\n\\n" "La vigencia del servicio RFC IDCIF ha finalizado."', "idcif vencido"),
    ]
    for old, new, label in commercial:
        text = replace_exact(text, old, new, "commercial " + label)

    return text


def compile_text(path: Path, text: str):
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / path.name
        p.write_text(text, encoding="utf-8")
        try:
            py_compile.compile(str(p), doraise=True)
        except Exception as exc:
            raise PatchError(f"py_compile falló para {path}: {exc}") from exc


def produce():
    if not WEBHOOK.exists():
        raise PatchError(f"No existe {WEBHOOK}")
    if not WORKER.exists():
        raise PatchError(f"No existe {WORKER}")
    old_web = WEBHOOK.read_text(encoding="utf-8")
    old_worker = WORKER.read_text(encoding="utf-8")
    new_web = patch_webhook(old_web)
    new_worker = patch_worker(old_worker)
    compile_text(WEBHOOK, new_web)
    compile_text(WORKER, new_worker)
    return old_web, new_web, old_worker, new_worker


def show_diff(path, old, new):
    print("\n".join(difflib.unified_diff(
        old.splitlines(), new.splitlines(),
        fromfile=str(path), tofile=str(path) + " (patched)", lineterm=""
    )))


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--check", action="store_true")
    g.add_argument("--diff", action="store_true")
    g.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    try:
        old_web, new_web, old_worker, new_worker = produce()
    except PatchError as exc:
        print(f"❌ PARCHE ABORTADO: {exc}", file=sys.stderr)
        return 2

    if args.check:
        print("✅ Coincidencias críticas correctas")
        print("✅ app/rfc_webhook.py compila con el parche")
        print("✅ worker_jobs.py compila con el parche")
        print("✅ No se escribió ningún archivo")
        return 0

    if args.diff:
        show_diff(WEBHOOK, old_web, new_web)
        show_diff(WORKER, old_worker, new_worker)
        return 0

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    b1 = WEBHOOK.with_name(WEBHOOK.name + ".bak-" + stamp)
    b2 = WORKER.with_name(WORKER.name + ".bak-" + stamp)
    shutil.copy2(WEBHOOK, b1)
    shutil.copy2(WORKER, b2)
    try:
        WEBHOOK.write_text(new_web, encoding="utf-8")
        WORKER.write_text(new_worker, encoding="utf-8")
        py_compile.compile(str(WEBHOOK), doraise=True)
        py_compile.compile(str(WORKER), doraise=True)
    except Exception as exc:
        shutil.copy2(b1, WEBHOOK)
        shutil.copy2(b2, WORKER)
        print(f"❌ Error al aplicar/compilar: {exc}", file=sys.stderr)
        print("✅ Backups restaurados automáticamente")
        return 3

    print("✅ PARCHE APLICADO Y COMPILADO")
    print(f"Backup webhook: {b1}")
    print(f"Backup worker:  {b2}")
    print("Revisa ahora: git diff -- app/rfc_webhook.py worker_jobs.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
