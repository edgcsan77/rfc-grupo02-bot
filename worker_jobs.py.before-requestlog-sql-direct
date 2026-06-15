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
