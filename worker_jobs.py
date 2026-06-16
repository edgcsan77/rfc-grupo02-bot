import os
import re
import traceback
import requests
import base64
import json
import hashlib

from datetime import datetime
from zoneinfo import ZoneInfo
from redis import Redis

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

CURP_RE = re.compile(r"\b[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d\b", re.I)
RFC_RE = re.compile(r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b", re.I)
IDCIF_RE = re.compile(r"\b\d{11}\b", re.I)

def _panel_now():
    return datetime.now(ZoneInfo(PANEL_TZ))

def _panel_day_str():
    return _panel_now().strftime("%Y-%m-%d")

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

    if kind in ("CURP", "RFC_ONLY"):
        add_clon = count
    elif kind in ("RFC_IDCIF", "QR"):
        add_idcif = count
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

    # historial permanente para cortes / cobros / auditorías
    pipe.persist(key)
    pipe.execute()

def _stats_counted_key(job_data: dict, kind: str, item_key: str = "") -> str:
    instance = (job_data.get("evolution_instance") or EVOLUTION_INSTANCE or "").strip()
    group = (job_data.get("group_jid") or "").strip()
    msg_id = (job_data.get("msg_id") or job_data.get("media_id") or "").strip()
    requester = (job_data.get("requester_number") or "").strip()
    day = _panel_day_str()

    if not item_key:
        item_key = (
            job_data.get("query")
            or job_data.get("original_text")
            or job_data.get("media_id")
            or ""
        )

    base = f"{day}|{instance}|{group}|{requester}|{msg_id}|{kind}|{item_key}"
    h = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return f"stats_counted:{day}:{h}"


def record_success_once(job_data: dict, group_jid: str, group_name: str, kind: str, count: int = 1, item_key: str = "") -> bool:
    """
    Cuenta una solicitud exitosa una sola vez.
    Evita inflar panel_stats/cut_stats por reintentos, jobs repetidos o webhooks duplicados.
    """
    if not group_jid or count <= 0:
        return False

    key = _stats_counted_key(job_data, kind, item_key=item_key)

    ok = redis_stats.set(key, "1", nx=True, ex=60 * 60 * 24 * 35)

    if not ok:
        print("[STATS DUPLICATE IGNORED]", key, flush=True)
        return False

    _save_rfc_panel_request_log(
        {
            **job_data,
            "item_key": locals().get("item_key") or locals().get("pdf_filename") or locals().get("filename"),
            "query_type": locals().get("kind") or job_data.get("query_type") or job_data.get("msg_type"),
        },
        (
            locals().get("result")
            or locals().get("media_result")
            or locals().get("bot_result")
            or locals().get("data")
            or locals().get("resp_json")
            or locals().get("response_json")
            or locals().get("internal_result")
            or {
                "pdf_url": locals().get("pdf_url"),
                "filename": locals().get("item_key") or locals().get("filename") or locals().get("pdf_filename"),
                "detected_query": locals().get("detected_query"),
            }
        ),
        status="DONE",
    )

    print("[COUNT_SUCCESS]", {
        "group_jid": group_jid,
        "group_name": group_name,
        "kind": kind,
        "count": count,
        "item_key": item_key,
        "msg_id": job_data.get("msg_id"),
        "media_id": job_data.get("media_id"),
        "query": job_data.get("query"),
        "original_text": job_data.get("original_text"),
    }, flush=True)

    panel_record_success(group_jid=group_jid, group_name=group_name, kind=kind, count=count)
    cut_record_success(group_jid=group_jid, group_name=group_name, kind=kind, count=count)

    try:
        instance_name = (
            job_data.get("evolution_instance")
            or job_data.get("instance_name")
            or EVOLUTION_INSTANCE
            or "grupo02"
        ).strip()

        _rfc_commercial_after_success(
            job_data=job_data,
            group_jid=group_jid,
            group_name=group_name,
            instance_name=instance_name,
            kind=kind,
            count=count
        )
    except Exception as e:
        print("[RFC_COMMERCIAL_AFTER_SUCCESS_ERROR]", repr(e), flush=True)

    return True

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

def evolution_send_media_to_group(group_jid: str, media_url: str, file_name: str, instance_name=None):
    instance_name = (instance_name or EVOLUTION_INSTANCE).strip()

    url = f"{EVOLUTION_BASE_URL}/message/sendMedia/{instance_name}"
    payload = {
        "number": group_jid,
        "mediatype": "document",
        "media": media_url,
        "fileName": file_name,
    }

    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=240)
    print("worker sendMedia instance:", instance_name, flush=True)
    print("worker sendMedia payload:", payload, flush=True)
    print("worker sendMedia resp:", r.status_code, r.text, flush=True)
    r.raise_for_status()
    return r.json()

def call_bot_internal_text(
    requester_number: str,
    requester_name: str,
    group_jid: str,
    original_text: str,
    query: str,
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
        "query": query,
        "evolution_instance": instance_name,
    }
    url = f"{BOT_INTERNAL_URL.rstrip('/')}/internal/generate-pdf"
    r = requests.post(url, json=payload, headers=headers, timeout=420)
    print("worker call_bot_internal_text instance:", instance_name, flush=True)
    print("worker call_bot_internal_text status:", r.status_code, flush=True)
    print("worker call_bot_internal_text resp:", r.text, flush=True)
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

    instance_name = (job_data.get("evolution_instance") or EVOLUTION_INSTANCE).strip()
    print("[WORKER EVOLUTION INSTANCE]", repr(instance_name), flush=True)

    print("[WORKER GROUP NAME]", repr(group_name), flush=True)

    try:
        requested_kind = _classify_success_kind(
            query=query or "",
            original_text=original_text or "",
            msg_type=msg_type or ""
        )

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

        if not result.get("ok"):
            err = result.get("error") or "No fue posible generar el documento."
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} {err}",
                instance_name=instance_name
            )
            return

        kind = _classify_success_kind(query=query or "", original_text=original_text or "", msg_type=msg_type)
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

            try:
                evolution_send_media_to_group(
                    group_jid=group_jid,
                    media_url=zip_url,
                    file_name=file_name,
                    instance_name=instance_name,
                )
                kind = _classify_success_kind(query=query or "", original_text=original_text or "", msg_type=msg_type)
            except Exception as media_err:
                print("group batch zip media send fail:", repr(media_err), flush=True)
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el lote se generó, pero no pude adjuntarlo.\n{zip_url}",
                    instance_name=instance_name
                )
            return

        if mode == "batch_multi":
            items = result.get("items") or []

            for item in items:
                pdf_url = (item.get("pdf_url") or "").strip()
                file_name = (item.get("filename") or "documento.pdf").strip()
                err = (item.get("error") or "").strip()
                rfc = (item.get("rfc") or "").strip()
                idcif = (item.get("idcif") or "").strip()

                if pdf_url and not err:
                    try:
                        evolution_send_media_to_group(
                            group_jid=group_jid,
                            media_url=pdf_url,
                            file_name=file_name,
                            instance_name=instance_name,
                        )
                
                        item_key = rfc or idcif or file_name or pdf_url
                
                        record_success_once(
                            job_data=job_data,
                            group_jid=group_jid,
                            group_name=group_name,
                            kind=kind,
                            count=1,
                            item_key=item_key
                        )
                    except Exception as media_err:
                        print("group batch multi media send fail:", repr(media_err), flush=True)
                        evolution_send_text_to_group(
                            group_jid,
                            f"⚠️ {requester_label} no pude adjuntar {file_name}.\n{pdf_url}",
                            instance_name=instance_name
                        )
                else:
                    evolution_send_text_to_group(
                        group_jid,
                        f"❌ {requester_label} fallo {rfc} {idcif}: {err or 'error desconocido'}",
                        instance_name=instance_name
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

        try:
            evolution_send_media_to_group(
                group_jid=group_jid,
                media_url=pdf_url,
                file_name=file_name,
                instance_name=instance_name,
            )
            
            record_success_once(
                job_data=job_data,
                group_jid=group_jid,
                group_name=group_name,
                kind=kind,
                count=1,
                item_key=query or original_text or file_name or pdf_url
            )
        except Exception as media_err:
            print("group media send fail:", repr(media_err), flush=True)
            evolution_send_text_to_group(
                group_jid,
                f"⚠️ {requester_label} el documento se generó, pero no pude adjuntarlo.\n{pdf_url}",
                instance_name=instance_name
            )

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
            elif "SIN_DATOS_SAT" in resp_text or err_code == "SIN_DATOS_SAT":
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el IDCIF/QR se leyó, pero no arrojó información en SAT.",
                    instance_name=instance_name
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
            elif "CLIENT_RFC_SUSPENDED" in resp_text or err_code == "CLIENT_RFC_SUSPENDED":
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el RFC aparece como suspendido.\n\n"
                    "Verifica la situación fiscal o envía otro RFC.",
                    instance_name=instance_name
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
                "provider_name": "RFC",
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


def _rfc_plan_family(kind: str) -> str:
    kind = (kind or "").upper().strip()
    if kind in ("CURP", "RFC_ONLY"):
        return "CLON"
    if kind in ("QR", "RFC_IDCIF"):
        return "IDCIF"
    return "UNKNOWN"


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


def _rfc_plan_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    """
    Valida saldo antes de generar.
    CLON: necesita clon_balance > 0.
    IDCIF: necesita plan semanal activo.
    """
    try:
        from datetime import datetime, timezone

        family = _rfc_plan_family(kind)

        if family == "UNKNOWN":
            return True

        plan = _rfc_plan_get(group_jid, instance_name)

        requester_label = (
            job_data.get("requester_label")
            or job_data.get("requester_name")
            or ""
        )

        if not plan:
            evolution_send_text_to_group(
                group_jid,
                f"⚠️ {requester_label} este grupo no tiene bolsa RFC configurada. Contacta al administrador.",
                instance_name=instance_name
            )
            return False

        if family == "CLON":
            if not plan.get("clon_enabled"):
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} la bolsa RFC CLON está desactivada.",
                    instance_name=instance_name
                )
                return False

            balance = int(plan.get("clon_balance") or 0)

            if balance <= 0:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} la bolsa RFC CLON se terminó.\n\n"
                    "Contacta al administrador para recargar.",
                    instance_name=instance_name
                )
                return False

            return True

        if family == "IDCIF":
            if not plan.get("idcif_enabled"):
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el plan semanal RFC IDCIF no está activo.",
                    instance_name=instance_name
                )
                return False

            expires_at = plan.get("idcif_expires_at")

            if not expires_at:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el plan semanal RFC IDCIF no tiene fecha activa.",
                    instance_name=instance_name
                )
                return False

            now = datetime.now(timezone.utc)

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            if expires_at <= now:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el plan semanal RFC IDCIF venció.\n\n"
                    "Contacta al administrador para renovar.",
                    instance_name=instance_name
                )
                return False

            return True

        return True

    except Exception as e:
        print("[RFC_PLAN_CHECK_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
        }, flush=True)
        return True


def _rfc_plan_deduct_success(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    """
    Descuenta SOLO cuando ya fue éxito y record_success_once aceptó contar.
    CLON: descuenta piezas.
    IDCIF: solo suma uso semanal.
    """
    try:
        from sqlalchemy import text

        family = _rfc_plan_family(kind)

        if family == "UNKNOWN":
            return

        engine = _rfc_plan_engine()

        with engine.begin() as conn:
            if family == "CLON":
                conn.execute(text("""
                    UPDATE rfc_group_plans
                    SET
                        clon_balance = GREATEST(clon_balance - :count, 0),
                        clon_used = clon_used + :count,
                        updated_at = now()
                    WHERE group_jid = :group_jid
                      AND instance_name = :instance_name
                """), {
                    "count": int(count or 1),
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                })

                print("[RFC_CLON_DEDUCTED]", {
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                    "kind": kind,
                    "count": count,
                }, flush=True)

            elif family == "IDCIF":
                conn.execute(text("""
                    UPDATE rfc_group_plans
                    SET
                        idcif_used = idcif_used + :count,
                        updated_at = now()
                    WHERE group_jid = :group_jid
                      AND instance_name = :instance_name
                """), {
                    "count": int(count or 1),
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                })

                print("[RFC_IDCIF_USED_INC]", {
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                    "kind": kind,
                    "count": count,
                }, flush=True)

    except Exception as e:
        print("[RFC_PLAN_DEDUCT_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
            "count": count,
        }, flush=True)


def _rfc_plan_family(kind: str) -> str:
    kind = (kind or "").upper().strip()
    if kind in ("CURP", "RFC_ONLY"):
        return "CLON"
    if kind in ("QR", "RFC_IDCIF"):
        return "IDCIF"
    return "UNKNOWN"


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


def _rfc_plan_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    """
    Valida saldo antes de generar.
    CLON: necesita clon_balance > 0.
    IDCIF: necesita plan semanal activo.
    """
    try:
        from datetime import datetime, timezone

        family = _rfc_plan_family(kind)

        if family == "UNKNOWN":
            return True

        plan = _rfc_plan_get(group_jid, instance_name)

        requester_label = (
            job_data.get("requester_label")
            or job_data.get("requester_name")
            or ""
        )

        if not plan:
            evolution_send_text_to_group(
                group_jid,
                f"⚠️ {requester_label} este grupo no tiene bolsa RFC configurada. Contacta al administrador.",
                instance_name=instance_name
            )
            return False

        if family == "CLON":
            if not plan.get("clon_enabled"):
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} la bolsa RFC CLON está desactivada.",
                    instance_name=instance_name
                )
                return False

            balance = int(plan.get("clon_balance") or 0)

            if balance <= 0:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} la bolsa RFC CLON se terminó.\n\n"
                    "Contacta al administrador para recargar.",
                    instance_name=instance_name
                )
                return False

            return True

        if family == "IDCIF":
            if not plan.get("idcif_enabled"):
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el plan semanal RFC IDCIF no está activo.",
                    instance_name=instance_name
                )
                return False

            expires_at = plan.get("idcif_expires_at")

            if not expires_at:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el plan semanal RFC IDCIF no tiene fecha activa.",
                    instance_name=instance_name
                )
                return False

            now = datetime.now(timezone.utc)

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            if expires_at <= now:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el plan semanal RFC IDCIF venció.\n\n"
                    "Contacta al administrador para renovar.",
                    instance_name=instance_name
                )
                return False

            return True

        return True

    except Exception as e:
        print("[RFC_PLAN_CHECK_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
        }, flush=True)
        return True


def _rfc_plan_deduct_success(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    """
    Descuenta SOLO cuando ya fue éxito y record_success_once aceptó contar.
    CLON: descuenta piezas.
    IDCIF: solo suma uso semanal.
    """
    try:
        from sqlalchemy import text

        family = _rfc_plan_family(kind)

        if family == "UNKNOWN":
            return

        engine = _rfc_plan_engine()

        with engine.begin() as conn:
            if family == "CLON":
                conn.execute(text("""
                    UPDATE rfc_group_plans
                    SET
                        clon_balance = GREATEST(clon_balance - :count, 0),
                        clon_used = clon_used + :count,
                        updated_at = now()
                    WHERE group_jid = :group_jid
                      AND instance_name = :instance_name
                """), {
                    "count": int(count or 1),
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                })

                print("[RFC_CLON_DEDUCTED]", {
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                    "kind": kind,
                    "count": count,
                }, flush=True)

            elif family == "IDCIF":
                conn.execute(text("""
                    UPDATE rfc_group_plans
                    SET
                        idcif_used = idcif_used + :count,
                        updated_at = now()
                    WHERE group_jid = :group_jid
                      AND instance_name = :instance_name
                """), {
                    "count": int(count or 1),
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                })

                print("[RFC_IDCIF_USED_INC]", {
                    "group_jid": group_jid,
                    "instance_name": instance_name,
                    "kind": kind,
                    "count": count,
                }, flush=True)

    except Exception as e:
        print("[RFC_PLAN_DEDUCT_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
            "count": count,
        }, flush=True)


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


def _rfc_commercial_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    """
    Antes de generar PDF:
    - CURP/RFC_ONLY valida saldo/límite del bot interno en bot_control.
    - QR/RFC_IDCIF valida plan semanal IDCIF del owner grupo02.
    """
    try:
        from datetime import datetime, timezone
        from sqlalchemy import text

        family = _rfc_kind_family(kind)

        if family == "UNKNOWN":
            return True

        requester_label = (
            job_data.get("requester_label")
            or job_data.get("requester_name")
            or ""
        )

        engine = _rfc_commercial_engine()

        # =========================
        # CLON: por bot_control
        # =========================
        if family == "CLON":
            with engine.begin() as conn:
                row = _rfc_ensure_bot_control(conn, instance_name)

                limit_value = int(row.get("limit") or 0)
                used_value = int(row.get("used") or 0)
                is_blocked = bool(row.get("is_blocked"))
                is_active = bool(row.get("is_active"))

                if not is_active:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} este bot interno no está activo.",
                        instance_name=instance_name
                    )
                    return False

                if is_blocked:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} este bot interno está bloqueado o sin saldo RFC CLON.",
                        instance_name=instance_name
                    )
                    return False

                # limit=0 significa ilimitado, igual que el panel actual muestra ∞
                if limit_value > 0 and used_value >= limit_value:
                    conn.execute(text("""
                        UPDATE bot_control
                        SET
                            is_blocked = TRUE,
                            updated_at = now()
                        WHERE instance_name = :instance_name
                    """), {
                        "instance_name": instance_name,
                    })

                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} este bot interno ya no tiene RFC CLON disponibles.\n\n"
                        "Contacta al administrador para recarga.",
                        instance_name=instance_name
                    )
                    return False

            return True

        # =========================
        # IDCIF: plan semanal owner grupo02
        # =========================
        if family == "IDCIF":
            owner = _rfc_owner_instance()

            with engine.begin() as conn:
                wallet = conn.execute(text("""
                    SELECT
                        owner_instance,
                        idcif_enabled,
                        idcif_expires_at
                    FROM rfc_owner_wallets
                    WHERE owner_instance = :owner
                    LIMIT 1
                """), {
                    "owner": owner,
                }).mappings().first()

                if not wallet:
                    conn.execute(text("""
                        INSERT INTO rfc_owner_wallets (
                            owner_instance,
                            owner_name,
                            idcif_enabled,
                            idcif_weekly_price,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            :owner,
                            :owner_name,
                            FALSE,
                            500.00,
                            now(),
                            now()
                        )
                        ON CONFLICT (owner_instance)
                        DO NOTHING
                    """), {
                        "owner": owner,
                        "owner_name": owner.upper(),
                    })

                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} el plan semanal RFC IDCIF no está activo.",
                        instance_name=instance_name
                    )
                    return False

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
                        f"⚠️ {requester_label} el plan semanal RFC IDCIF venció.\n\n"
                        "Contacta al administrador para renovar.",
                        instance_name=instance_name
                    )
                    return False

            return True

        return True

    except Exception as e:
        print("[RFC_COMMERCIAL_CHECK_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
        }, flush=True)

        # Si falla la validación por error interno, NO generamos para evitar consumo sin control.
        try:
            evolution_send_text_to_group(
                group_jid,
                "⚠️ Ocurrió un error validando saldo/plan RFC. Intenta de nuevo en unos minutos.",
                instance_name=instance_name
            )
        except Exception:
            pass

        return False


def _rfc_commercial_after_success(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    """
    Después de DONE:
    - CURP/RFC_ONLY incrementa used del bot interno.
    - QR/RFC_IDCIF incrementa uso IDCIF semanal del owner grupo02.
    """
    try:
        from sqlalchemy import text

        family = _rfc_kind_family(kind)

        if family == "UNKNOWN":
            return

        engine = _rfc_commercial_engine()
        count = int(count or 1)

        if family == "CLON":
            with engine.begin() as conn:
                _rfc_ensure_bot_control(conn, instance_name)

                conn.execute(text("""
                    UPDATE bot_control
                    SET
                        used = used + :count,
                        is_blocked = CASE
                            WHEN "limit" > 0 AND (used + :count) >= "limit"
                            THEN TRUE
                            ELSE is_blocked
                        END,
                        updated_at = now()
                    WHERE instance_name = :instance_name
                """), {
                    "count": count,
                    "instance_name": instance_name,
                })

            print("[RFC_CLON_BOT_USED_INC]", {
                "instance_name": instance_name,
                "group_jid": group_jid,
                "kind": kind,
                "count": count,
            }, flush=True)

            return

        if family == "IDCIF":
            owner = _rfc_owner_instance()

            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE rfc_owner_wallets
                    SET
                        idcif_used = idcif_used + :count,
                        updated_at = now()
                    WHERE owner_instance = :owner
                """), {
                    "count": count,
                    "owner": owner,
                })

            print("[RFC_IDCIF_OWNER_USED_INC]", {
                "owner": owner,
                "instance_name": instance_name,
                "group_jid": group_jid,
                "kind": kind,
                "count": count,
            }, flush=True)

            return

    except Exception as e:
        print("[RFC_COMMERCIAL_AFTER_SUCCESS_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
            "count": count,
        }, flush=True)


# =========================================================
# OVERRIDE FINAL RFC GRUPO02
# Modelo correcto:
# - RFC CLON global de grupo02: CURP/RFC_ONLY descuentan clon_balance.
# - IDCIF semanal global de grupo02: QR/RFC_IDCIF validan vigencia y suman idcif_used.
# - NO usar rfc_group_plans.
# =========================================================

def _rfc_plan_family(kind: str) -> str:
    k = (kind or "").strip().upper()

    if k in ("CURP", "RFC_ONLY", "RFC"):
        return "CLON"

    if k in ("QR", "RFC_IDCIF", "IDCIF"):
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


def _rfc_plan_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    """
    OVERRIDE FINAL.
    Antes de procesar:
    - CLON revisa saldo global rfc_owner_wallets.clon_balance.
    - IDCIF revisa plan semanal global rfc_owner_wallets.idcif_expires_at.
    """
    try:
        from datetime import datetime, timezone
        from sqlalchemy import text

        family = _rfc_plan_family(kind)

        if family == "UNKNOWN":
            return True

        owner = _rfc_global_wallet_owner()
        engine = _rfc_global_wallet_engine()

        requester_label = (
            job_data.get("requester_label")
            or job_data.get("requester_name")
            or ""
        )

        with engine.begin() as conn:
            _rfc_global_wallet_ensure(conn, owner)

            wallet = conn.execute(text("""
                SELECT
                    owner_instance,
                    clon_balance,
                    clon_used,
                    idcif_enabled,
                    idcif_expires_at,
                    idcif_used
                FROM rfc_owner_wallets
                WHERE owner_instance = :owner
                LIMIT 1
            """), {
                "owner": owner,
            }).mappings().first()

            if not wallet:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} no hay saldo RFC configurado.",
                    instance_name=instance_name
                )
                return False

            if family == "CLON":
                balance = int(wallet.get("clon_balance") or 0)

                if balance <= 0:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} el panel ya no tiene RFC CLON disponibles.\n\n"
                        "Contacta al administrador para recargar.",
                        instance_name=instance_name
                    )
                    return False

                print("[RFC_CLON_GLOBAL_CHECK_OK]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "balance": balance,
                }, flush=True)

                return True

            if family == "IDCIF":
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
                        f"⚠️ {requester_label} el plan semanal RFC IDCIF venció.\n\n"
                        "Contacta al administrador para renovar.",
                        instance_name=instance_name
                    )
                    return False

                print("[RFC_IDCIF_GLOBAL_CHECK_OK]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "expires_at": str(expires_at),
                }, flush=True)

                return True

        return True

    except Exception as e:
        print("[RFC_GLOBAL_CHECK_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
        }, flush=True)

        try:
            evolution_send_text_to_group(
                group_jid,
                "⚠️ Error validando saldo RFC. Intenta de nuevo.",
                instance_name=instance_name
            )
        except Exception:
            pass

        return False


def _rfc_plan_deduct_success(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    """
    OVERRIDE FINAL.
    Después de DONE:
    - CLON descuenta saldo global grupo02.
    - IDCIF suma uso global IDCIF.
    - También suma bot_control.used para auditoría por bot interno.
    """
    try:
        from sqlalchemy import text

        family = _rfc_plan_family(kind)

        if family == "UNKNOWN":
            print("[RFC_GLOBAL_DEDUCT_SKIP_UNKNOWN]", {
                "kind": kind,
                "instance_name": instance_name,
                "group_jid": group_jid,
            }, flush=True)
            return

        owner = _rfc_global_wallet_owner()
        engine = _rfc_global_wallet_engine()
        count = int(count or 1)

        with engine.begin() as conn:
            _rfc_global_wallet_ensure(conn, owner)

            if family == "CLON":
                conn.execute(text("""
                    UPDATE rfc_owner_wallets
                    SET
                        clon_balance = GREATEST(clon_balance - :count, 0),
                        clon_used = clon_used + :count,
                        updated_at = now()
                    WHERE owner_instance = :owner
                """), {
                    "count": count,
                    "owner": owner,
                })

                print("[RFC_CLON_GLOBAL_DEDUCTED]", {
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
                """), {
                    "count": count,
                    "owner": owner,
                })

                print("[RFC_IDCIF_GLOBAL_USED_INC]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "count": count,
                }, flush=True)

            # Auditoría por bot interno en el panel.
            # Esto NO controla saldo real; solo deja ver qué bot consumió.
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
                    :instance_name,
                    NULL,
                    0,
                    :count,
                    0,
                    FALSE,
                    TRUE,
                    now(),
                    now()
                )
                ON CONFLICT (instance_name)
                DO UPDATE SET
                    used = bot_control.used + :count,
                    updated_at = now()
            """), {
                "instance_name": instance_name,
                "count": count,
            })

    except Exception as e:
        print("[RFC_GLOBAL_DEDUCT_ERROR]", repr(e), {
            "group_jid": group_jid,
            "instance_name": instance_name,
            "kind": kind,
            "count": count,
        }, flush=True)


# =========================================================
# OVERRIDE DEFINITIVO RFC GRUPO02
# CLON = saldo global rfc_owner_wallets.clon_balance
# IDCIF = plan semanal global rfc_owner_wallets.idcif_expires_at
# NO usar rfc_group_plans ni saldo por bot.
# =========================================================

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


def _rfc_final_family(kind: str) -> str:
    k = (kind or "").strip().upper()

    if k in ("CURP", "RFC", "RFC_ONLY"):
        return "CLON"

    if k in ("QR", "IDCIF", "RFC_IDCIF"):
        return "IDCIF"

    return "UNKNOWN"


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


def _rfc_final_check_global(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    try:
        from datetime import datetime, timezone
        from sqlalchemy import text

        family = _rfc_final_family(kind)

        if family == "UNKNOWN":
            return True

        owner = _rfc_final_owner_instance()
        engine = _rfc_final_engine()

        requester_label = (
            job_data.get("requester_label")
            or job_data.get("requester_name")
            or ""
        )

        with engine.begin() as conn:
            _rfc_final_ensure_wallet(conn, owner)

            wallet = conn.execute(text("""
                SELECT
                    owner_instance,
                    clon_balance,
                    clon_used,
                    idcif_enabled,
                    idcif_expires_at,
                    idcif_used
                FROM rfc_owner_wallets
                WHERE owner_instance = :owner
                LIMIT 1
            """), {
                "owner": owner,
            }).mappings().first()

            if not wallet:
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} no hay saldo RFC configurado.",
                    instance_name=instance_name
                )
                return False

            if family == "CLON":
                balance = int(wallet.get("clon_balance") or 0)

                if balance <= 0:
                    evolution_send_text_to_group(
                        group_jid,
                        f"⚠️ {requester_label} el panel ya no tiene RFC CLON disponibles.\n\n"
                        "Contacta al administrador para recargar.",
                        instance_name=instance_name
                    )
                    return False

                print("[RFC_CLON_GLOBAL_CHECK_OK]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "balance": balance,
                }, flush=True)

                return True

            if family == "IDCIF":
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
                        f"⚠️ {requester_label} el plan semanal RFC IDCIF venció.\n\n"
                        "Contacta al administrador para renovar.",
                        instance_name=instance_name
                    )
                    return False

                print("[RFC_IDCIF_GLOBAL_CHECK_OK]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "expires_at": str(expires_at),
                }, flush=True)

                return True

        return True

    except Exception as e:
        print("[RFC_FINAL_GLOBAL_CHECK_ERROR]", repr(e), {
            "kind": kind,
            "group_jid": group_jid,
            "instance_name": instance_name,
        }, flush=True)

        try:
            evolution_send_text_to_group(
                group_jid,
                "⚠️ Error validando saldo RFC. Intenta de nuevo.",
                instance_name=instance_name
            )
        except Exception:
            pass

        return False


def _rfc_final_after_success_global(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    try:
        from sqlalchemy import text

        family = _rfc_final_family(kind)

        if family == "UNKNOWN":
            print("[RFC_FINAL_GLOBAL_SKIP_UNKNOWN]", {
                "kind": kind,
                "instance_name": instance_name,
                "group_jid": group_jid,
            }, flush=True)
            return

        owner = _rfc_final_owner_instance()
        engine = _rfc_final_engine()
        count = int(count or 1)

        with engine.begin() as conn:
            _rfc_final_ensure_wallet(conn, owner)

            if family == "CLON":
                conn.execute(text("""
                    UPDATE rfc_owner_wallets
                    SET
                        clon_balance = GREATEST(clon_balance - :count, 0),
                        clon_used = clon_used + :count,
                        updated_at = now()
                    WHERE owner_instance = :owner
                """), {
                    "count": count,
                    "owner": owner,
                })

                print("[RFC_CLON_GLOBAL_DEDUCTED]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "count": count,
                }, flush=True)
                _rfc_clear_panel_cache_v2()

            elif family == "IDCIF":
                conn.execute(text("""
                    UPDATE rfc_owner_wallets
                    SET
                        idcif_used = idcif_used + :count,
                        updated_at = now()
                    WHERE owner_instance = :owner
                """), {
                    "count": count,
                    "owner": owner,
                })

                print("[RFC_IDCIF_GLOBAL_USED_INC]", {
                    "owner": owner,
                    "instance_name": instance_name,
                    "group_jid": group_jid,
                    "kind": kind,
                    "count": count,
                }, flush=True)
                _rfc_clear_panel_cache_v2()

            # Solo auditoría por bot; no controla saldo real.
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
                    :instance_name,
                    NULL,
                    0,
                    :count,
                    0,
                    FALSE,
                    TRUE,
                    now(),
                    now()
                )
                ON CONFLICT (instance_name)
                DO UPDATE SET
                    used = bot_control.used + :count,
                    updated_at = now()
            """), {
                "instance_name": instance_name,
                "count": count,
            })

    except Exception as e:
        print("[RFC_FINAL_GLOBAL_AFTER_ERROR]", repr(e), {
            "kind": kind,
            "group_jid": group_jid,
            "instance_name": instance_name,
            "count": count,
        }, flush=True)


# Alias para pisar todas las funciones viejas que el código ya llama.
def _rfc_plan_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    return _rfc_final_check_global(job_data, group_jid, group_name, instance_name, kind)


def _rfc_commercial_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    return _rfc_final_check_global(job_data, group_jid, group_name, instance_name, kind)


def _rfc_plan_deduct_success(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    return _rfc_final_after_success_global(job_data, group_jid, group_name, instance_name, kind, count)


def _rfc_commercial_after_success(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    return _rfc_final_after_success_global(job_data, group_jid, group_name, instance_name, kind, count)


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
    if k in ("CURP", "RFC", "RFC_ONLY"):
        return "CLON"
    if k in ("QR", "IDCIF", "RFC_IDCIF"):
        return "IDCIF"
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

    conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS clon_limit INTEGER NOT NULL DEFAULT 0'))
    conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS clon_used INTEGER NOT NULL DEFAULT 0'))
    conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS clon_recharges INTEGER NOT NULL DEFAULT 0'))
    conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS idcif_limit INTEGER NOT NULL DEFAULT 0'))
    conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS idcif_used INTEGER NOT NULL DEFAULT 0'))
    conn.execute(text('ALTER TABLE bot_control ADD COLUMN IF NOT EXISTS idcif_recharges INTEGER NOT NULL DEFAULT 0'))

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


def _rfc_final_check_global(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    try:
        from datetime import datetime, timezone
        from sqlalchemy import text

        family = _rfc_final_family(kind)
        if family == "UNKNOWN":
            return True

        owner = _rfc_final_owner_instance()
        engine = _rfc_final_engine()

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
                    clon_limit,
                    clon_used,
                    idcif_limit,
                    idcif_used
                FROM bot_control
                WHERE instance_name = :instance_name
                LIMIT 1
            """), {"instance_name": instance_name}).mappings().first()

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


def _rfc_final_after_success_global(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
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

            try:
                _rfc_clear_panel_cache_v2()
            except Exception:
                pass

    except Exception as e:
        print("[RFC_FINAL_BOT_LIMIT_AFTER_ERROR]", repr(e), {
            "kind": kind,
            "group_jid": group_jid,
            "instance_name": instance_name,
            "count": count,
        }, flush=True)


# Aliases finales para pisar cualquier lógica anterior.
def _rfc_plan_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    return _rfc_final_check_global(job_data, group_jid, group_name, instance_name, kind)


def _rfc_commercial_check_or_notify(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str) -> bool:
    return _rfc_final_check_global(job_data, group_jid, group_name, instance_name, kind)


def _rfc_plan_deduct_success(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    return _rfc_final_after_success_global(job_data, group_jid, group_name, instance_name, kind, count)


def _rfc_commercial_after_success(job_data: dict, group_jid: str, group_name: str, instance_name: str, kind: str, count: int = 1):
    return _rfc_final_after_success_global(job_data, group_jid, group_name, instance_name, kind, count)
