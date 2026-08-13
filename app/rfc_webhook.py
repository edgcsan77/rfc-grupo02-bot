import os
import re
import copy
import json
import hashlib
import time

from datetime import timedelta

from rq import Retry
from fastapi import APIRouter, Request

from app.queue import request_queue
from app.services.evolution import send_text
from app.db import SessionLocal
from app.models import (
    AuthorizedGroup,
    BotControl,
    GroupPromotion,
    ProviderSetting,
    AppSetting,
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
    _normalize_verifiable_text,
    _verifiable_word_distance,
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

VERIFIABLE_PROVIDER_INPUT_MODE_KEY_PREFIX = (
    "VERIFIABLE_PROVIDER_INPUT_MODE:"
)


def _verifiable_provider_uses_rfc_converter(
    db,
    provider_code: str,
) -> bool:
    code = (
        provider_code
        or ""
    ).strip().upper()

    if code not in {
        "VERIF1",
        "VERIF2",
        "VERIF3",
        "VERIF4",
    }:
        return False

    key = (
        VERIFIABLE_PROVIDER_INPUT_MODE_KEY_PREFIX
        + code
    )

    row = (
        db.query(AppSetting)
        .filter(
            AppSetting.key == key
        )
        .first()
    )

    if not row:
        # DEFAULT:
        # mandar exactamente lo recibido.
        return False

    mode = (
        str(
            row.value
            or ""
        )
        .strip()
        .upper()
    )

    return mode == "RFC"

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
        nl_error_text = str(
            nl_error
            or ""
        ).strip()

        print(
            "[VERIFIABLE_NL_CURP_FAIL]",
            {
                "curp": curp,
                "error": repr(nl_error),
            },
            flush=True,
        )

        # ==========================================
        # CURP CONFIRMADA COMO NO LOCALIZADA
        # ==========================================
        #
        # Nuevo León respondió expresamente que
        # la CURP no existe en su base.
        #
        # NO intentar gob.mx.
        # NO intentar Moffin.
        # ==========================================
        if (
            "NL_CURP_NOT_FOUND"
            in nl_error_text
        ):
            raise RuntimeError(
                "VERIFIABLE_CURP_NOT_FOUND:"
                f"{curp}"
            ) from nl_error

        # ==========================================
        # FALLO TÉCNICO DE NUEVO LEÓN
        # ==========================================
        #
        # Ejemplos:
        # - HTTP 500
        # - HTTP 503
        # - timeout
        # - error de red
        #
        # En esos casos SÍ intentamos gob.mx.
        # ==========================================
        try:
            datos_curp = (
                consultar_curp_bot(
                    curp
                )
                or {}
            )

            print(
                "[VERIFIABLE_GOB_CURP_FALLBACK_OK]",
                {
                    "curp": curp,
                    "nl_error": (
                        nl_error_text
                    ),
                },
                flush=True,
            )

        except Exception as gob_error:
            print(
                "[VERIFIABLE_GOB_CURP_FALLBACK_FAIL]",
                {
                    "curp": curp,
                    "nl_error": (
                        nl_error_text
                    ),
                    "gob_error": repr(
                        gob_error
                    ),
                },
                flush=True,
            )

            raise RuntimeError(
                "VERIFIABLE_CURP_SERVICE_UNAVAILABLE:"
                f"curp={curp}:"
                f"nl={nl_error_text}:"
                f"gob={str(gob_error)}"
            ) from gob_error

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
    
        "provider_code": (
            getattr(
                row,
                "verifiable_provider_code",
                None,
            )
        ),
    }


def _verifiable_group_balance(
    db,
    group_jid: str,
) -> dict:
    """
    La bolsa RFC es opcional para verificables.

    - Sin bolsa activa: permitido; aplica únicamente
      el límite general del bot.
    - Bolsa activa con verifiable_total <= 0:
      permitido; no existe límite verificable por grupo.
    - Bolsa activa con verifiable_total > 0:
      se respeta el saldo verificable de la bolsa.
    """

    group_jid = str(
        group_jid or ""
    ).strip()

    if not group_jid:
        return {
            "found": False,
            "allowed": False,
            "limited_by_promotion": False,
            "reason": "group_jid_empty",
            "total": 0,
            "used": 0,
            "available": None,
            "shared_key": "",
        }

    promo = (
        db.query(GroupPromotion)
        .filter(
            GroupPromotion.group_jid
            == group_jid,
            GroupPromotion.is_active
            == True,
        )
        .order_by(
            GroupPromotion.updated_at.desc(),
            GroupPromotion.id.desc(),
        )
        .first()
    )

    # No tener bolsa NO bloquea verificables.
    if not promo:
        return {
            "found": False,
            "allowed": True,
            "limited_by_promotion": False,
            "reason": (
                "verifiable_without_group_promotion"
            ),
            "total": 0,
            "used": 0,
            "available": None,
            "shared_key": "",
            "shared_limit": 0,
            "shared_used": 0,
        }

    shared_key = str(
        getattr(
            promo,
            "shared_key",
            "",
        )
        or ""
    ).strip()

    shared_limit = int(
        getattr(
            promo,
            "shared_group_limit_verifiable",
            0,
        )
        or 0
    )

    shared_used = int(
        getattr(
            promo,
            "shared_group_used_verifiable",
            0,
        )
        or 0
    )

    # ==========================================
    # BOLSA INDIVIDUAL
    # ==========================================
    if not shared_key:
        total = int(
            getattr(
                promo,
                "verifiable_total",
                0,
            )
            or 0
        )

        used = int(
            getattr(
                promo,
                "verifiable_used",
                0,
            )
            or 0
        )

    # ==========================================
    # BOLSA COMPARTIDA
    # ==========================================
    else:
        shared_rows = (
            db.query(GroupPromotion)
            .filter(
                GroupPromotion.shared_key
                == shared_key,
                GroupPromotion.is_active
                == True,
            )
            .order_by(
                GroupPromotion.id.asc()
            )
            .all()
        )

        if not shared_rows:
            return {
                "found": True,
                "allowed": False,
                "limited_by_promotion": True,
                "reason": (
                    "verifiable_shared_promotion_not_found"
                ),
                "total": 0,
                "used": 0,
                "available": 0,
                "shared_key": shared_key,
                "shared_limit": shared_limit,
                "shared_used": shared_used,
            }

        totals = {
            int(
                getattr(
                    row,
                    "verifiable_total",
                    0,
                )
                or 0
            )
            for row in shared_rows
        }

        used_values = {
            int(
                getattr(
                    row,
                    "verifiable_used",
                    0,
                )
                or 0
            )
            for row in shared_rows
        }

        if len(totals) != 1:
            return {
                "found": True,
                "allowed": False,
                "limited_by_promotion": True,
                "reason": (
                    "verifiable_shared_total_mismatch"
                ),
                "total": 0,
                "used": 0,
                "available": 0,
                "shared_key": shared_key,
                "shared_limit": shared_limit,
                "shared_used": shared_used,
            }

        if len(used_values) != 1:
            return {
                "found": True,
                "allowed": False,
                "limited_by_promotion": True,
                "reason": (
                    "verifiable_shared_used_mismatch"
                ),
                "total": 0,
                "used": 0,
                "available": 0,
                "shared_key": shared_key,
                "shared_limit": shared_limit,
                "shared_used": shared_used,
            }

        total = next(iter(totals))
        used = next(iter(used_values))

    # Una bolsa puede existir únicamente para CLON/IDCIF.
    # Si no asignó verificables, NO debe bloquearlos.
    if total <= 0:
        return {
            "found": True,
            "allowed": True,
            "limited_by_promotion": False,
            "reason": (
                "verifiable_without_group_limit"
            ),
            "total": total,
            "used": used,
            "available": None,
            "shared_key": shared_key,
            "shared_limit": shared_limit,
            "shared_used": shared_used,
        }

    available = max(
        total - used,
        0,
    )

    if used >= total:
        return {
            "found": True,
            "allowed": False,
            "limited_by_promotion": True,
            "reason": (
                "verifiable_group_limit_reached"
            ),
            "total": total,
            "used": used,
            "available": available,
            "shared_key": shared_key,
            "shared_limit": shared_limit,
            "shared_used": shared_used,
        }

    if (
        shared_limit > 0
        and shared_used >= shared_limit
    ):
        return {
            "found": True,
            "allowed": False,
            "limited_by_promotion": True,
            "reason": (
                "verifiable_shared_group_limit_reached"
            ),
            "total": total,
            "used": used,
            "available": available,
            "shared_key": shared_key,
            "shared_limit": shared_limit,
            "shared_used": shared_used,
        }

    return {
        "found": True,
        "allowed": True,
        "limited_by_promotion": True,
        "reason": "ok",
        "total": total,
        "used": used,
        "available": available,
        "shared_key": shared_key,
        "shared_limit": shared_limit,
        "shared_used": shared_used,
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
            "⚠️ Solicitud no reconocida\n\n"
            "Puedes enviar:\n"
            "• CURP\n"
            "• RFC\n"
            "• RFC + IDCIF\n"
            "• QR SAT\n"
            "• CURP/RFC VERIFICABLE"
        ),
    }


def _parse_rfc_batch_requests(
    text: str,
    msg_type: str = "",
) -> list[dict]:
    """
    Extrae TODAS las solicitudes reconocibles de un mensaje,
    ignorando texto libre y conservando el orden de los datos.

    Reglas:
    - RFC + IDCIF e IDCIF + RFC se emparejan cuando son datos
      reconocidos consecutivos, aunque haya texto libre entre ellos.
    - RFC/CURP sin IDCIF quedan como solicitudes individuales.
    - Si existe un indicador VERIFICABLE en cualquier parte del
      mensaje, todos los RFC/CURP individuales del mensaje se tratan
      como verificables. Los pares RFC+IDCIF conservan su flujo IDCIF.
    - Cada línea que contenga un QR SAT textual se trata como un QR.
    - Texto que no sea RFC, CURP, IDCIF, QR SAT o indicador
      verificable se ignora por completo.
    - Una imagen/documento sigue siendo una única solicitud porque
      Evolution entrega cada archivo multimedia en su propio webhook.
    """
    raw = str(text or "")

    if msg_type in {"image", "document"}:
        parsed = _parse_rfc_query(
            raw,
            msg_type=msg_type,
        )
        return [{
            "ok": bool(parsed.get("ok")),
            "type": parsed.get("type") or "INVALID_INPUT",
            "text": raw,
            "query": parsed.get("query") or "",
            "error": parsed.get("error") or "",
        }]

    normalized = (
        raw.replace("\r\n", "\n")
        .replace("\r", "\n")
    )

    # ----------------------------------------------------------
    # Indicador GLOBAL VERIFICABLE para listas.
    #
    # IMPORTANTE:
    # Aquí NO usamos las abreviaciones cortas del flujo individual
    # (V, VE, VER, VERI, VERIF), porque palabras normales como
    # "ver" podrían convertir accidentalmente toda una lista en
    # solicitudes verificables.
    #
    # Para activar el modo global se exige una palabra claramente
    # equivalente a VERIFICABLE/VERIFICABLES. Se toleran errores
    # tipográficos razonables únicamente en palabras largas.
    # ----------------------------------------------------------
    global_verifiable = False

    for word in re.findall(
        r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+",
        normalized,
    ):
        normalized_word = _normalize_verifiable_text(
            word
        )

        normalized_word = re.sub(
            r"[^A-Z]",
            "",
            normalized_word,
        )

        if normalized_word in {
            "VERIFICABLE",
            "VERIFICABLES",
        }:
            global_verifiable = True
            break

        # Permitir errores tipográficos solamente cuando la palabra
        # sigue siendo suficientemente larga para ser inequívoca.
        #
        # Ejemplos admitidos:
        # VERIFIACBLE
        # VERIFCABLE
        # VEIIFICABLE
        if len(normalized_word) >= 8:
            distance_to_singular = (
                _verifiable_word_distance(
                    normalized_word,
                    "VERIFICABLE",
                )
            )

            distance_to_plural = (
                _verifiable_word_distance(
                    normalized_word,
                    "VERIFICABLES",
                )
            )

            if min(
                distance_to_singular,
                distance_to_plural,
            ) <= 2:
                global_verifiable = True
                break

    # ----------------------------------------------------------
    # Tokens reconocidos, en orden físico dentro del mensaje.
    # El texto libre jamás se convierte en solicitud inválida.
    # ----------------------------------------------------------
    token_re = re.compile(
        r"\b[A-Z][AEIOUX][A-Z]{2}"
        r"\d{6}[HM][A-Z]{5}[A-Z0-9]\d\b"
        r"|"
        r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b"
        r"|"
        r"\b\d{11}\b",
        re.I,
    )

    data_tokens: list[dict] = []
    qr_items: list[dict] = []
    offset = 0

    for raw_line in normalized.split("\n"):
        line = str(raw_line or "")

        # Un QR SAT textual conserva la línea completa para que el
        # worker reciba todos sus parámetros. No extraemos RFC/IDCIF
        # de esa misma línea para evitar falsos positivos del URL.
        if SAT_QR_RE.search(line):
            qr_items.append({
                "position": offset,
                "ok": True,
                "type": "QR_TEXT",
                "is_verifiable": False,
                "text": line.strip(),
                "query": line.strip(),
                "error": "",
            })
            offset += len(line) + 1
            continue

        upper_line = line.upper()

        for match in token_re.finditer(upper_line):
            token = match.group(0).upper()

            if CURP_RE.fullmatch(token):
                token_type = "CURP"
            elif RFC_RE.fullmatch(token):
                token_type = "RFC"
            elif IDCIF_RE.fullmatch(token):
                token_type = "IDCIF"
            else:
                continue

            data_tokens.append({
                "position": offset + match.start(),
                "kind": token_type,
                "value": token,
            })

        offset += len(line) + 1

    results_with_position: list[tuple[int, dict]] = []
    index = 0

    while index < len(data_tokens):
        current = data_tokens[index]
        following = (
            data_tokens[index + 1]
            if index + 1 < len(data_tokens)
            else None
        )

        # RFC + IDCIF o IDCIF + RFC. "Consecutivos" significa que
        # no existe otro RFC/CURP/IDCIF reconocido entre ambos;
        # cualquier texto libre intermedio se ignora.
        if (
            following
            and {current["kind"], following["kind"]}
            == {"RFC", "IDCIF"}
        ):
            rfc = (
                current["value"]
                if current["kind"] == "RFC"
                else following["value"]
            )
            idcif = (
                current["value"]
                if current["kind"] == "IDCIF"
                else following["value"]
            )

            results_with_position.append((
                min(current["position"], following["position"]),
                {
                    "ok": True,
                    "type": "RFC_IDCIF",
                    "is_verifiable": False,
                    "text": f"RFC: {rfc}\nIDCIF: {idcif}",
                    "query": f"RFC: {rfc}\nIDCIF: {idcif}",
                    "error": "",
                },
            ))
            index += 2
            continue

        if current["kind"] == "CURP":
            identifier = current["value"]
            if global_verifiable:
                item = {
                    "ok": True,
                    "type": "RFC_VERIFICABLE",
                    "is_verifiable": True,
                    "text": f"VERIFICABLE {identifier}",
                    "query": "",
                    "error": "",
                }
            else:
                item = {
                    "ok": True,
                    "type": "CURP",
                    "is_verifiable": False,
                    "text": identifier,
                    "query": identifier,
                    "error": "",
                }

            results_with_position.append((
                current["position"],
                item,
            ))
            index += 1
            continue

        if current["kind"] == "RFC":
            identifier = current["value"]
            if global_verifiable:
                item = {
                    "ok": True,
                    "type": "RFC_VERIFICABLE",
                    "is_verifiable": True,
                    "text": f"VERIFICABLE {identifier}",
                    "query": "",
                    "error": "",
                }
            else:
                item = {
                    "ok": True,
                    "type": "RFC_ONLY",
                    "is_verifiable": False,
                    "text": identifier,
                    "query": identifier,
                    "error": "",
                }

            results_with_position.append((
                current["position"],
                item,
            ))
            index += 1
            continue

        # IDCIF sin RFC vecino no es una solicitud autónoma.
        # Se ignora igual que el texto libre.
        index += 1

    for qr_item in qr_items:
        position = int(qr_item.pop("position"))
        results_with_position.append((position, qr_item))

    results_with_position.sort(key=lambda pair: pair[0])
    results = [item for _, item in results_with_position]

    total = len(results)
    for item_index, item in enumerate(results, start=1):
        item["batch_index"] = item_index
        item["batch_total"] = total

    return results


class _RFCBatchSyntheticRequest:
    def __init__(self, payload: dict):
        self._payload = payload

    async def json(self):
        return self._payload


def _build_rfc_batch_child_payload(
    *,
    payload: dict,
    item: dict,
    parent_msg_id: str,
    batch_started_at_epoch: float,
) -> dict:
    child_payload = copy.deepcopy(payload)
    child_data = child_payload.setdefault("data", {})
    child_key = child_data.setdefault("key", {})

    batch_index = int(item.get("batch_index") or 1)
    batch_total = int(item.get("batch_total") or 1)

    child_msg_id = (
        f"{parent_msg_id}:batch:{batch_index}"
    )

    child_key["id"] = child_msg_id
    child_data["id"] = child_msg_id
    child_data["messageId"] = child_msg_id

    child_text = str(item.get("text") or "")
    child_data["message"] = {
        "conversation": child_text,
    }
    child_data["text"] = child_text
    child_data["body"] = child_text

    child_data["_rfc_batch_child"] = True
    child_data["_rfc_batch_index"] = batch_index
    child_data["_rfc_batch_total"] = batch_total
    child_data["_rfc_batch_parent_msg_id"] = parent_msg_id
    child_data["_rfc_batch_suppress_ack"] = True
    child_data["_rfc_batch_started_at_epoch"] = float(
        batch_started_at_epoch
    )

    return child_payload


def _claim_batch_global_notice(
    *,
    batch_child: bool,
    batch_parent_msg_id: str,
    instance_name: str,
    remote_jid: str,
    notice_code: str,
    ttl_sec: int = 600,
) -> bool:
    """
    Evita repetir un aviso GLOBAL dentro del mismo mensaje multilínea.

    - Solicitud individual: siempre permite el aviso.
    - Batch: sólo el primer hijo que encuentra el mismo notice_code
      envía el mensaje.
    - Si Redis falla, permite el aviso (fail-open) para no ocultarlo.
    """
    if not batch_child:
        return True

    parent_msg_id = str(
        batch_parent_msg_id or ""
    ).strip()

    notice_code = str(
        notice_code or ""
    ).strip().lower()

    if not parent_msg_id or not notice_code:
        return True

    base = "|".join(
        [
            str(instance_name or "").strip(),
            str(remote_jid or "").strip(),
            parent_msg_id,
            notice_code,
        ]
    )

    digest = hashlib.sha1(
        base.encode("utf-8")
    ).hexdigest()

    redis_key = (
        "rfc:batch:global_notice:"
        f"{digest}"
    )

    try:
        claimed = bool(
            request_queue.connection.set(
                redis_key,
                "1",
                nx=True,
                ex=max(int(ttl_sec or 600), 60),
            )
        )

        print(
            "RFC_BATCH_GLOBAL_NOTICE_CLAIM =",
            {
                "parent_msg_id": parent_msg_id,
                "notice_code": notice_code,
                "redis_key": redis_key,
                "claimed": claimed,
            },
            flush=True,
        )

        return claimed

    except Exception as claim_exc:
        print(
            "RFC_BATCH_GLOBAL_NOTICE_CLAIM_ERROR =",
            {
                "parent_msg_id": parent_msg_id,
                "notice_code": notice_code,
                "error": repr(claim_exc),
            },
            flush=True,
        )

        return True


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


def _group_service_config(
    db,
    group_jid: str,
    instance_name: str,
) -> dict:
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
            "clon_enabled": False,
            "idcif_enabled": False,
            "verifiable_enabled": False,
        }

    owner_instance = (
        row.owner_instance or ""
    ).strip()

    owned = bool(
        not owner_instance
        or owner_instance
        == (instance_name or "").strip()
    )

    return {
        "exists": True,
        "owned": owned,

        "clon_enabled": bool(
            getattr(
                row,
                "clon_enabled",
                True,
            )
        ),

        "idcif_enabled": bool(
            getattr(
                row,
                "idcif_enabled",
                True,
            )
        ),

        "verifiable_enabled": bool(
            getattr(
                row,
                "verifiable_enabled",
                False,
            )
        ),

        "owner_instance":
            owner_instance,
    }


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


def _verifiable_identity_key(
    instance: str,
    remote_jid: str,
    requester: str,
    query_type: str,
    identifier: str,
) -> str:
    """
    Identifica la misma solicitud verificable aunque
    WhatsApp genere otro msg_id.
    
    Bloquea la misma CURP/RFC para:
    - la misma instancia;
    - el mismo grupo;
    - el mismo tipo de consulta.
    """
    normalized_identifier = re.sub(
        r"\s+",
        "",
        str(identifier or "").strip().upper(),
    )

    normalized_type = (
        str(query_type or "")
        .strip()
        .upper()
    )

    base = "|".join(
        [
            str(instance or "").strip(),
            str(remote_jid or "").strip(),
            normalized_type,
            normalized_identifier,
        ]
    )

    return hashlib.sha256(
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
        "⏳ Se procesarán individualmente.\n"
        "Te avisaremos cuando tengamos cada resultado."
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

        item["routing_mode"] = str(
            setting.value
            or ""
        ).strip().upper()

        result.append(item)

    return result


def _choose_verifiable_provider(
    db,
) -> dict:
    """
    Selección automática normal.

    Excluye proveedores marcados GROUP_ONLY,
    porque esos únicamente pueden seleccionarse
    expresamente para un grupo de grupo02.
    """
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
        and str(
            provider.get("routing_mode")
            or ""
        ).strip().upper()
        != "GROUP_ONLY"
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


BOT_VERIFIABLE_PROVIDER_KEY_PREFIX = (
    "BOT_VERIFIABLE_PROVIDER:"
)


def _verifiable_provider_code_for_instance(
    db,
    instance_name: str,
) -> str:
    inst = str(
        instance_name
        or ""
    ).strip().lower()

    if not inst:
        return ""

    key = (
        BOT_VERIFIABLE_PROVIDER_KEY_PREFIX
        + inst
    )

    row = (
        db.query(AppSetting)
        .filter(
            AppSetting.key == key
        )
        .first()
    )

    if not row:
        return ""

    code = str(
        row.value
        or ""
    ).strip().upper()

    if code in {
        "",
        "AUTO",
        "AUTOMATICO",
        "AUTOMÁTICO",
    }:
        return ""

    if code not in {
        "VERIF1",
        "VERIF2",
        "VERIF3",
        "VERIF4",
    }:
        return ""

    return code


def _choose_verifiable_provider_for_group(
    db,
    *,
    group_jid: str,
    instance_name: str,
) -> dict:
    """
    Prioridad para RFC verificable:

    1. Si es grupo02 y el grupo tiene proveedor específico:
       usar ese proveedor.

    2. Si no hay proveedor específico de grupo,
       revisar proveedor configurado para la instancia/bot.

    3. Si tampoco existe configuración por bot:
       usar selección automática normal.
    """

    group_jid = str(
        group_jid
        or ""
    ).strip()

    instance_name = str(
        instance_name
        or ""
    ).strip()

    forced_code = ""
    selection_mode = "AUTO"

    # ==================================================
    # 1. PROVEEDOR ESPECÍFICO DEL GRUPO
    #    SOLO PARA EL PANEL PRINCIPAL / grupo02
    # ==================================================
    if (
        instance_name
        == MAIN_PANEL_INSTANCE
    ):
        group_row = (
            db.query(AuthorizedGroup)
            .filter(
                AuthorizedGroup.group_jid
                == group_jid
            )
            .first()
        )

        if group_row:
            owner_instance = str(
                getattr(
                    group_row,
                    "owner_instance",
                    "",
                )
                or ""
            ).strip()

            group_provider_code = str(
                getattr(
                    group_row,
                    "verifiable_provider_code",
                    "",
                )
                or ""
            ).strip().upper()

            if (
                owner_instance
                == MAIN_PANEL_INSTANCE
                and group_provider_code
                not in {
                    "",
                    "AUTO",
                    "AUTOMATICO",
                    "AUTOMÁTICO",
                }
            ):
                forced_code = (
                    group_provider_code
                )

                selection_mode = (
                    "GROUP_FORCED"
                )

    # ==================================================
    # 2. PROVEEDOR FIJADO PARA TODO EL BOT
    # ==================================================
    if not forced_code:
        bot_provider_code = (
            _verifiable_provider_code_for_instance(
                db,
                instance_name,
            )
        )

        if bot_provider_code:
            forced_code = (
                bot_provider_code
            )

            selection_mode = (
                "BOT_FORCED"
            )

    # ==================================================
    # 3. SIN CONFIGURACIÓN MANUAL = AUTOMÁTICO
    # ==================================================
    if not forced_code:
        selected = (
            _choose_verifiable_provider(
                db
            )
        )

        if not selected:
            return {}

        selected = dict(
            selected
        )

        selected[
            "selection_mode"
        ] = "AUTO"

        return selected

    allowed_codes = {
        "VERIF1",
        "VERIF2",
        "VERIF3",
        "VERIF4",
    }

    if forced_code not in allowed_codes:
        print(
            "RFC_VERIFIABLE_FORCED_PROVIDER_INVALID =",
            {
                "group_jid":
                    group_jid,
                "instance":
                    instance_name,
                "forced_code":
                    forced_code,
                "selection_mode":
                    selection_mode,
            },
            flush=True,
        )

        return _choose_verifiable_provider(
            db
        )

    providers = (
        _verifiable_providers_runtime(
            db
        )
    )

    selected = next(
        (
            provider
            for provider in providers
            if str(
                provider.get(
                    "code"
                )
                or ""
            ).strip().upper()
            == forced_code
        ),
        None,
    )

    if not selected:
        print(
            "RFC_VERIFIABLE_FORCED_PROVIDER_NOT_FOUND =",
            {
                "group_jid":
                    group_jid,
                "instance":
                    instance_name,
                "forced_code":
                    forced_code,
                "selection_mode":
                    selection_mode,
            },
            flush=True,
        )

        return {}

    # Si está seleccionado manualmente,
    # DE TODAS FORMAS debe estar encendido.
    if not selected.get(
        "enabled"
    ):
        print(
            "RFC_VERIFIABLE_FORCED_PROVIDER_DISABLED =",
            {
                "group_jid":
                    group_jid,
                "instance":
                    instance_name,
                "forced_code":
                    forced_code,
                "selection_mode":
                    selection_mode,
                "provider_db_name":
                    selected.get(
                        "db_name"
                    ),
            },
            flush=True,
        )

        return {}

    selected = dict(
        selected
    )

    selected[
        "selection_mode"
    ] = selection_mode

    if (
        selection_mode
        == "GROUP_FORCED"
    ):
        selected[
            "forced_group_jid"
        ] = group_jid

    elif (
        selection_mode
        == "BOT_FORCED"
    ):
        selected[
            "forced_instance"
        ] = instance_name

    print(
        "RFC_VERIFIABLE_FORCED_PROVIDER_SELECTED =",
        {
            "group_jid":
                group_jid,
            "instance":
                instance_name,
            "provider_code":
                selected.get(
                    "code"
                ),
            "provider_name":
                selected.get(
                    "name"
                ),
            "provider_db_name":
                selected.get(
                    "db_name"
                ),
            "selection_mode":
                selection_mode,
            "routing_mode":
                selected.get(
                    "routing_mode"
                ),
        },
        flush=True,
    )

    return selected
    

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
    
    # Vuelve a consultar Redis para no procesar una copia
    # vieja de un pendiente que otro worker ya terminó.
    current_pending = (
        load_pending(verifiable_key)
        or {}
    )
    
    if not current_pending:
        return {
            "ok": False,
            "queued": False,
            "request_key": verifiable_key,
            "rfc": provider_rfc,
            "idcif": provider_idcif,
            "reason": "pending_already_finished",
        }
    
    pending = current_pending
    
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
        "batch_index": int(pending.get("batch_index") or 1),
        "batch_total": int(pending.get("batch_total") or 1),
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
        "verifiable_identity": (
            pending.get(
                "verifiable_identity"
            )
            or ""
        ),
        "verifiable_processing_key": (
            pending.get(
                "verifiable_processing_key"
            )
            or ""
        ),
        "verifiable_completed_key": (
            pending.get(
                "verifiable_completed_key"
            )
            or ""
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
        # ID del mensaje que el bot envió al proveedor.
        # Se usa para borrar la asociación al finalizar.
        "provider_request_msg_id": (
            pending.get(
                "provider_message_id"
            )
            or ""
        ),
        
        # ID de la respuesta enviada por el proveedor.
        # Se conserva para auditoría y deduplicación del job.
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

    provider_job_message_id = (
        provider_response_msg_id
        or quoted_message_id
        or "without-provider-message"
    ).strip()
    
    final_rq_job_id = (
        "rfc-verifiable-result:"
        f"{verifiable_key}:"
        f"{provider_job_message_id}"
    )

    try:
        request_queue.enqueue(
            "worker_jobs."
            "process_group_request_job",
            job_data,
            job_id=final_rq_job_id,
            job_timeout=900,
            result_ttl=86400,
            failure_ttl=86400,
            retry=Retry(
                max=3,
                interval=[15, 45, 120],
            ),
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

    Respuestas admitidas:
    - NO ID
    - SIN ID
    - S/ID
    - NO HAY ID
    - NO SALE

    Formatos admitidos:
    - DATO RESPUESTA
    - RESPUESTA DATO
    - RESPUESTA + salto + DATO
    - DATO + salto + RESPUESTA
    - RESPUESTA sola, cuando el mensaje está citado
      o contiene un único resultado.
    """

    results: list[dict] = []

    normalized_text = (
        str(provider_text or "")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\u00a0", " ")
        .replace("\u2007", " ")
        .replace("\u202f", " ")
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
        .replace("\ufeff", "")
    )

    identifier_pattern = (
        r"(?:"
        r"[A-ZÑ&]{3,4}\d{6}[A-Z0-9Ñ]{3}"
        r"|"
        r"[A-Z][AEIOUX][A-Z]{2}\d{6}"
        r"[HM][A-Z]{5}[A-Z0-9]\d"
        r")"
    )

    status_pattern = (
        r"(?:"
        r"S\s*/\s*ID"
        r"|SIN\s+ID"
        r"|NO\s+ID"
        r"|NO\s+HAY\s+ID"
        r"|NO\s+SALE"
        r"|NO\s+RESULTADO"
        r"|SIN\s+RESULTADO"
        r")"
    )

    identifier_only_re = re.compile(
        rf"^(?P<identifier>{identifier_pattern})$",
        re.I,
    )

    status_only_re = re.compile(
        rf"^(?P<status>{status_pattern})$",
        re.I,
    )

    # DATO RESPUESTA
    identifier_status_re = re.compile(
        rf"^(?P<identifier>{identifier_pattern})"
        r"\s*(?:[-:|]\s*)?"
        rf"(?P<status>{status_pattern})$",
        re.I,
    )

    # RESPUESTA DATO
    status_identifier_re = re.compile(
        rf"^(?P<status>{status_pattern})"
        r"\s*(?:[-:|]\s*)?"
        rf"(?P<identifier>{identifier_pattern})$",
        re.I,
    )

    lines: list[dict] = []

    for line_number, raw_line in enumerate(
        normalized_text.split("\n"),
        start=1,
    ):
        normalized_line = re.sub(
            r"\s+",
            " ",
            str(raw_line or "")
            .strip()
            .upper(),
        )

        if not normalized_line:
            continue

        lines.append({
            "line_number": line_number,
            "raw_line": raw_line,
            "text": normalized_line,
        })

    consumed_indexes: set[int] = set()
    seen_results: set[tuple] = set()

    def add_result(
        *,
        identifier: str,
        status: str,
        line_number: int,
        raw_line: str,
        reason: str,
    ):
        identifier = str(
            identifier or ""
        ).strip().upper()

        status = re.sub(
            r"\s+",
            " ",
            str(status or "")
            .strip()
            .upper(),
        )

        dedupe_key = (
            identifier,
            status,
            int(line_number or 0),
        )

        if dedupe_key in seen_results:
            return

        seen_results.add(dedupe_key)

        results.append({
            "identifier": identifier,
            "status": status,
            "line_number": line_number,
            "raw_line": raw_line,
            "reason": reason,
        })

    # ==================================================
    # 1. RESPUESTAS EN LA MISMA LÍNEA
    # ==================================================
    for index, line_data in enumerate(lines):
        line_text = line_data["text"]

        match = identifier_status_re.fullmatch(
            line_text
        )

        if match:
            add_result(
                identifier=match.group(
                    "identifier"
                ),
                status=match.group(
                    "status"
                ),
                line_number=line_data[
                    "line_number"
                ],
                raw_line=line_data[
                    "raw_line"
                ],
                reason=(
                    "identifier_status_same_line"
                ),
            )

            consumed_indexes.add(index)
            continue

        match = status_identifier_re.fullmatch(
            line_text
        )

        if match:
            add_result(
                identifier=match.group(
                    "identifier"
                ),
                status=match.group(
                    "status"
                ),
                line_number=line_data[
                    "line_number"
                ],
                raw_line=line_data[
                    "raw_line"
                ],
                reason=(
                    "status_identifier_same_line"
                ),
            )

            consumed_indexes.add(index)

    # ==================================================
    # 2. RESPUESTA Y DATO EN LÍNEAS CONSECUTIVAS
    # ==================================================
    index = 0

    while index < len(lines) - 1:
        if index in consumed_indexes:
            index += 1
            continue

        next_index = index + 1

        if next_index in consumed_indexes:
            index += 1
            continue

        current = lines[index]
        following = lines[next_index]

        current_status = status_only_re.fullmatch(
            current["text"]
        )

        current_identifier = (
            identifier_only_re.fullmatch(
                current["text"]
            )
        )

        following_status = (
            status_only_re.fullmatch(
                following["text"]
            )
        )

        following_identifier = (
            identifier_only_re.fullmatch(
                following["text"]
            )
        )

        # RESPUESTA
        # DATO
        if (
            current_status
            and following_identifier
        ):
            add_result(
                identifier=(
                    following_identifier.group(
                        "identifier"
                    )
                ),
                status=current_status.group(
                    "status"
                ),
                line_number=current[
                    "line_number"
                ],
                raw_line=(
                    f"{current['raw_line']}\n"
                    f"{following['raw_line']}"
                ),
                reason=(
                    "status_then_identifier_lines"
                ),
            )

            consumed_indexes.add(index)
            consumed_indexes.add(next_index)

            index += 2
            continue

        # DATO
        # RESPUESTA
        if (
            current_identifier
            and following_status
        ):
            add_result(
                identifier=(
                    current_identifier.group(
                        "identifier"
                    )
                ),
                status=following_status.group(
                    "status"
                ),
                line_number=current[
                    "line_number"
                ],
                raw_line=(
                    f"{current['raw_line']}\n"
                    f"{following['raw_line']}"
                ),
                reason=(
                    "identifier_then_status_lines"
                ),
            )

            consumed_indexes.add(index)
            consumed_indexes.add(next_index)

            index += 2
            continue

        index += 1

    # ==================================================
    # 3. RESPUESTAS SOLAS
    # ==================================================
    for index, line_data in enumerate(lines):
        if index in consumed_indexes:
            continue

        status_match = status_only_re.fullmatch(
            line_data["text"]
        )

        if not status_match:
            continue

        add_result(
            identifier="",
            status=status_match.group(
                "status"
            ),
            line_number=line_data[
                "line_number"
            ],
            raw_line=line_data[
                "raw_line"
            ],
            reason="bare_negative_status",
        )

        consumed_indexes.add(index)

    return results
    

def _extract_verif4_blank_id_items(
    text: str,
) -> list[dict]:
    """
    Para ID ROBERTO / VERIF4:

    Interpreta una línea que contiene únicamente un RFC
    como resultado sin IDCIF.

    Ejemplos aceptados:

        JACJ0407211Z6
        JACJ0407211Z6<TAB>
        JACJ0407211Z6    <espacios>

    No toma líneas que sí contienen un IDCIF.
    No toma texto libre.
    """
    results: list[dict] = []

    normalized_text = (
        str(text or "")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\u00a0", " ")
        .replace("\u2007", " ")
        .replace("\u202f", " ")
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
        .replace("\ufeff", "")
    )

    seen: set[str] = set()

    for line_number, raw_line in enumerate(
        normalized_text.split("\n"),
        start=1,
    ):
        line = str(raw_line or "").strip().upper()

        if not line:
            continue

        # Conserva solamente el contenido útil de la línea.
        compact_line = re.sub(
            r"[ \t]+$",
            "",
            line,
        ).strip()

        # Debe ser exactamente un RFC.
        # No se acepta texto antes ni después.
        if not re.fullmatch(
            r"[A-ZÑ&]{3,4}\d{6}[A-Z0-9Ñ]{3}",
            compact_line,
            flags=re.IGNORECASE,
        ):
            continue

        identifier = compact_line.upper()

        if identifier in seen:
            continue

        seen.add(identifier)

        results.append({
            "identifier": identifier,
            "line_number": line_number,
            "reason": "verif4_blank_idcif",
            "status": "SIN ID",
            "raw_line": raw_line,
        })

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
        
        elif (
            fallback_match.get("reason")
            == "ambiguous_pending_match"
        ):
            matches = (
                fallback_match.get("matches")
                or []
            )
        
            return {
                "ok": False,
                "ambiguous": True,
                "identifier": identifier,
                "reason": "ambiguous_pending_match",
                "matches": matches,
            }

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

    verifiable_processing_key = (
        pending.get(
            "verifiable_processing_key"
        )
        or ""
    ).strip()
    
    verifiable_completed_key = (
        pending.get(
            "verifiable_completed_key"
        )
        or ""
    ).strip()

    if not verifiable_completed_key:
        release_provider_result_claim(
            request_key
        )
    
        print(
            "RFC_VERIFIABLE_NO_ID_"
            "COMPLETED_KEY_EMPTY =",
            {
                "request_key": request_key,
                "original_identifier": (
                    pending.get(
                        "original_identifier"
                    )
                    or matched_identifier
                    or ""
                ),
            },
            flush=True,
        )
    
        return {
            "ok": False,
            "sent": False,
            "request_key": request_key,
            "reason": (
                "verifiable_completed_key_empty"
            ),
        }

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
            _client_status_message(
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

    try:
        pipe = (
            request_queue.connection
            .pipeline()
        )
    
        # Un resultado SIN ID también fue una respuesta
        # definitiva enviada al cliente.
        if verifiable_completed_key:
            pipe.set(
                verifiable_completed_key,
                str(time.time()),
                ex=24 * 60 * 60,
            )
    
        if verifiable_processing_key:
            pipe.delete(
                verifiable_processing_key
            )
    
        if inflight_key:
            pipe.delete(
                inflight_key
            )
    
        pipe.execute()
    
        print(
            "RFC_VERIFIABLE_NO_ID_"
            "COMPLETED_24H_MARKED =",
            {
                "request_key": request_key,
                "completed_key": (
                    verifiable_completed_key
                ),
                "processing_key": (
                    verifiable_processing_key
                ),
                "inflight_key": inflight_key,
                "ttl_seconds": 86400,
            },
            flush=True,
        )
    
    except Exception as completed_exc:
        print(
            "RFC_VERIFIABLE_NO_ID_"
            "COMPLETED_MARK_ERROR =",
            {
                "request_key": request_key,
                "error": repr(
                    completed_exc
                ),
            },
            flush=True,
        )
    
        # No borres el pendiente si no se pudo crear
        # correctamente la protección de completado.
        release_provider_result_claim(
            request_key
        )
    
        return {
            "ok": False,
            "sent": True,
            "request_key": request_key,
            "reason": (
                "no_id_completion_mark_failed"
            ),
            "error": repr(
                completed_exc
            ),
        }
    
    finish_pending(
        request_key,
        provider_message_id=(
            stored_provider_message_id
        ),
    )
    
    release_provider_result_claim(
        request_key
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

        try:
            batch_index = max(
                int(data.get("_rfc_batch_index") or 1),
                1,
            )
        except Exception:
            batch_index = 1

        try:
            batch_total = max(
                int(data.get("_rfc_batch_total") or 1),
                1,
            )
        except Exception:
            batch_total = 1

        batch_child = bool(
            data.get("_rfc_batch_child")
        )

        batch_suppress_ack = bool(
            data.get("_rfc_batch_suppress_ack")
        )

        batch_parent_msg_id = str(
            data.get("_rfc_batch_parent_msg_id")
            or ""
        ).strip()

        try:
            batch_started_at_epoch = float(
                data.get("_rfc_batch_started_at_epoch")
                or time.time()
            )
        except Exception:
            batch_started_at_epoch = time.time()

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
            
            # --------------------------------------------------
            # ID ROBERTO / VERIF4:
            # una línea con puro RFC significa "sin IDCIF".
            #
            # Se restringe por grupo + instancia para no cambiar
            # el comportamiento de los demás proveedores.
            # --------------------------------------------------
            is_verif4_roberto = (
                remote_jid
                == "120363409752881042@g.us"
                and instance_name == "grupo02"
            )
            
            if is_verif4_roberto:
                verif4_blank_items = (
                    _extract_verif4_blank_id_items(
                        text
                    )
                )
            
                # RFC que ya tiene IDCIF dentro de provider_pairs.
                paired_rfcs = {
                    str(pair[0] or "").strip().upper()
                    for pair in provider_pairs
                    if pair
                }
            
                # RFC ya detectados por frases explícitas:
                # "NO ID", "SIN ID", "S/ID", etc.
                explicit_no_id_rfcs = {
                    str(item.get("identifier") or "")
                    .strip()
                    .upper()
                    for item in provider_no_id_items
                }
            
                for blank_item in verif4_blank_items:
                    identifier = (
                        blank_item.get("identifier")
                        or ""
                    ).strip().upper()
            
                    if not identifier:
                        continue
            
                    # Si esa línea o RFC sí quedó emparejado con IDCIF,
                    # no debe marcarse como no-id.
                    if identifier in paired_rfcs:
                        continue
            
                    # Evita duplicar un "sin id" que ya fue detectado
                    # mediante texto explícito.
                    if identifier in explicit_no_id_rfcs:
                        continue
            
                    provider_no_id_items.append(
                        blank_item
                    )
            
                    explicit_no_id_rfcs.add(
                        identifier
                    )
            
                print(
                    "RFC_VERIFIABLE_VERIF4_BLANK_ID_PARSE =",
                    {
                        "provider_code": "VERIF4",
                        "provider_group": remote_jid,
                        "provider_instance": instance_name,
                        "pairs": provider_pairs,
                        "blank_no_id_items": (
                            verif4_blank_items
                        ),
                        "final_no_id_items": (
                            provider_no_id_items
                        ),
                    },
                    flush=True,
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

                if (
                    pending_match.get("ambiguous")
                    and pending_match.get("matches")
                ):
                    fanout_results = []
                
                    seen_request_keys: set[str] = set()
                
                    for match in (
                        pending_match.get("matches")
                        or []
                    ):
                        match_request_key = (
                            match.get("request_key")
                            or ""
                        ).strip()
                
                        match_pending = (
                            match.get("pending")
                            or {}
                        )
                
                        if (
                            not match_request_key
                            or not match_pending
                            or match_request_key
                            in seen_request_keys
                        ):
                            continue
                
                        seen_request_keys.add(
                            match_request_key
                        )
                
                        fanout_result = (
                            _send_verifiable_no_id_to_client(
                                request_key=match_request_key,
                                pending=match_pending,
                                remote_jid=remote_jid,
                                instance_name=instance_name,
                                quoted_message_id="",
                                provider_response_msg_id=msg_id,
                                matched_identifier=identifier,
                                matched_by=(
                                    match.get("matched_by")
                                    or "ambiguous_no_id_fanout"
                                ),
                            )
                        )
                
                        fanout_result["line_number"] = (
                            no_id_item.get("line_number")
                        )
                
                        fanout_result["no_id_index"] = (
                            no_id_index
                        )
                
                        fanout_result["fanout"] = True
                
                        fanout_results.append(
                            fanout_result
                        )
                
                    no_id_results.extend(
                        fanout_results
                    )
                
                    print(
                        "RFC_VERIFIABLE_NO_ID_FANOUT =",
                        {
                            "identifier": identifier,
                            "matches": len(
                                pending_match.get("matches")
                                or []
                            ),
                            "sent": len([
                                item
                                for item in fanout_results
                                if item.get("sent")
                            ]),
                            "results": fanout_results,
                        },
                        flush=True,
                    )
                
                    continue

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
                        "• CURP/RFC\n"
                        "• RFC + IDCIF\n"
                        "• QR SAT\n"
                        "• CURP/RFC VERIFICABLE",
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


        # ======================================================
        # MULTILÍNEA
        # ======================================================
        if (
            not batch_child
            and not msg_type
        ):
            batch_items = _parse_rfc_batch_requests(
                text,
                msg_type=msg_type,
            )

            if len(batch_items) > 1:
                batch_started_at_epoch = time.time()
                batch_total = len(batch_items)
                requester_label = push_name or "Usuario"

                valid_items = [
                    item
                    for item in batch_items
                    if item.get("ok")
                ]

                type_counts: dict[str, int] = {}

                for item in valid_items:
                    item_type = str(
                        item.get("type") or ""
                    ).strip().upper()

                    if item_type:
                        type_counts[item_type] = (
                            type_counts.get(item_type, 0)
                            + 1
                        )

                redis_conn = request_queue.connection
                batch_ack_key = (
                    "rfc:ack:batch:"
                    f"{instance_name}:{msg_id}"
                )

                if redis_conn.set(
                    batch_ack_key,
                    "1",
                    nx=True,
                    ex=300,
                ):
                    try:
                        if (
                            len(type_counts) == 1
                            and len(valid_items) == batch_total
                        ):
                            only_type = next(iter(type_counts))
                            only_count = type_counts[only_type]

                            if only_type == "RFC_VERIFICABLE":
                                ack_text = (
                                    _client_verifiable_received_message(
                                        requester_label=requester_label,
                                        count=only_count,
                                    )
                                )
                            else:
                                ack_text = _client_received_message(
                                    requester_label=requester_label,
                                    query_type=only_type,
                                    count=only_count,
                                )
                        else:
                            ack_text = (
                                _client_mixed_received_message(
                                    requester_label=requester_label,
                                    total=batch_total,
                                    type_counts=type_counts,
                                )
                            )

                        send_text(
                            remote_jid,
                            ack_text,
                            instance_name=instance_name,
                            fast=True,
                        )
                    except Exception as ack_exc:
                        print(
                            "RFC_BATCH_ACK_SEND_ERROR =",
                            repr(ack_exc),
                            flush=True,
                        )

                batch_results = []

                for item in batch_items:
                    item_index = int(
                        item.get("batch_index") or 1
                    )

                    if not item.get("ok"):
                        try:
                            send_text(
                                remote_jid,
                                _client_status_message(
                                    title="⚠️ Solicitud no reconocida",
                                    requester_label=requester_label,
                                    body=(
                                        "Puedes enviar:\n"
                                        "• CURP\n"
                                        "• RFC\n"
                                        "• RFC + IDCIF\n"
                                        "• QR SAT\n"
                                        "• CURP/RFC VERIFICABLE"
                                    ),
                                    batch_index=item_index,
                                    batch_total=batch_total,
                                    include_identity=False,
                                ),
                                instance_name=instance_name,
                                fast=True,
                            )
                        except Exception as invalid_exc:
                            print(
                                "RFC_BATCH_INVALID_NOTICE_ERROR =",
                                repr(invalid_exc),
                                flush=True,
                            )

                        batch_results.append({
                            "ok": True,
                            "ignored": "invalid_batch_item",
                            "batch_index": item_index,
                            "batch_total": batch_total,
                        })
                        continue

                    child_payload = (
                        _build_rfc_batch_child_payload(
                            payload=payload,
                            item=item,
                            parent_msg_id=msg_id,
                            batch_started_at_epoch=(
                                batch_started_at_epoch
                            ),
                        )
                    )

                    child_result = await (
                        evolution_rfc_webhook(
                            _RFCBatchSyntheticRequest(
                                child_payload
                            )
                        )
                    )

                    batch_results.append({
                        "batch_index": item_index,
                        "batch_total": batch_total,
                        "type": item.get("type"),
                        "result": child_result,
                    })

                print(
                    "RFC_BATCH_PROCESSED =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                        "parent_msg_id": msg_id,
                        "batch_total": batch_total,
                        "type_counts": type_counts,
                        "results": batch_results,
                    },
                    flush=True,
                )

                return {
                    "ok": True,
                    "batch": True,
                    "batch_total": batch_total,
                    "results": batch_results,
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
            
                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_providers_not_configured",
                ):
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

                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_bot_not_configured",
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                "⚠️ RFC verificable no disponible\n"
                                f"👤 {requester_label}\n\n"
                                "El servicio de RFC verificable no está configurado."
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

                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_disabled",
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                "⚠️ RFC verificable no disponible\n"
                                f"👤 {requester_label}\n\n"
                                "El servicio de RFC verificable no está activo actualmente."
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

                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_group_not_found",
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                "⚠️ RFC verificable no disponible\n"
                                f"👤 {requester_label}\n\n"
                                "Este grupo no tiene configurado el servicio de RFC verificable."
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

                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_group_disabled",
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                "⚠️ RFC verificable no disponible\n"
                                f"👤 {requester_label}\n\n"
                                "Este grupo no tiene activo el servicio de RFC verificable."
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

                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_bot_inactive",
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                "⚠️ Servicio no disponible\n"
                                f"👤 {requester_label}\n\n"
                                "El servicio está temporalmente inactivo."
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

                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_bot_blocked",
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                "⚠️ Servicio no disponible\n"
                                f"👤 {requester_label}\n\n"
                                "El servicio está temporalmente bloqueado."
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

                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_limit_reached",
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                "⚠️ RFC verificable no disponible\n"
                                f"👤 {requester_label}\n\n"
                                "No hay RFC verificables disponibles en este momento."
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

            group_balance = (
                _verifiable_group_balance(
                    db,
                    remote_jid,
                )
            )
            
            if not group_balance.get(
                "allowed",
                False,
            ):
                reason = (
                    group_balance.get("reason")
                    or "verifiable_group_balance_denied"
                )
            
                print(
                    "RFC_VERIFIABLE_GROUP_BALANCE_DENIED =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                        "identifier": (
                            original_identifier
                        ),
                        "query_type": (
                            original_query_type
                        ),
                        "reason": reason,
                        "verifiable_total": (
                            group_balance.get("total")
                        ),
                        "verifiable_used": (
                            group_balance.get("used")
                        ),
                        "verifiable_available": (
                            group_balance.get(
                                "available"
                            )
                        ),
                        "shared_key": (
                            group_balance.get(
                                "shared_key"
                            )
                        ),
                        "shared_limit": (
                            group_balance.get(
                                "shared_limit"
                            )
                        ),
                        "shared_used": (
                            group_balance.get(
                                "shared_used"
                            )
                        ),
                    },
                    flush=True,
                )
            
                if reason == (
                    "verifiable_group_promotion_not_found"
                ):
                    client_message = (
                        "⚠️ RFC verificable no disponible\n"
                        f"👤 {requester_label}\n\n"
                        "Este grupo no tiene una bolsa RFC activa."
                    )
            
                elif reason == (
                    "verifiable_group_not_assigned"
                ):
                    client_message = (
                        "⚠️ RFC verificable no disponible\n"
                        f"👤 {requester_label}\n\n"
                        "Este grupo no tiene RFC verificables asignados."
                    )
            
                elif reason == (
                    "verifiable_group_limit_reached"
                ):
                    client_message = (
                        "⚠️ RFC verificable no disponible\n"
                        f"👤 {requester_label}\n\n"
                        "Este grupo ya no tiene RFC verificables disponibles."
                    )
            
                elif reason == (
                    "verifiable_shared_group_limit_reached"
                ):
                    client_message = (
                        "⚠️ RFC verificable no disponible\n"
                        f"👤 {requester_label}\n\n"
                        "Este grupo alcanzó su límite de RFC verificables."
                    )
            
                else:
                    client_message = (
                        "⚠️ No pudimos validar la disponibilidad\n"
                        f"👤 {requester_label}\n\n"
                        "No fue posible validar la disponibilidad de RFC verificables.\n"
                        "Intenta nuevamente en unos momentos."
                    )
            
                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code=("verifiable_group_balance:" + str(reason)),
                ):
                    try:
                        send_text(
                            remote_jid,
                            client_message,
                            instance_name=instance_name,
                            fast=True,
                        )
                    except Exception as send_exc:
                        print(
                            "RFC_VERIFIABLE_GROUP_BALANCE_"
                            "NOTICE_ERROR =",
                            {
                                "group_jid": remote_jid,
                                "reason": reason,
                                "error": repr(send_exc),
                            },
                            flush=True,
                        )
            
                return {
                    "ok": True,
                    "ignored": reason,
                    "group_jid": remote_jid,
                    "verifiable_total": (
                        group_balance.get("total")
                    ),
                    "verifiable_used": (
                        group_balance.get("used")
                    ),
                    "verifiable_available": (
                        group_balance.get("available")
                    ),
                }
            
            print(
                "RFC_VERIFIABLE_GROUP_BALANCE_OK =",
                {
                    "instance": instance_name,
                    "group_jid": remote_jid,
                    "identifier": (
                        original_identifier
                    ),
                    "query_type": (
                        original_query_type
                    ),
                    "reason": group_balance.get("reason"),
                    "limited_by_promotion": (
                        group_balance.get(
                            "limited_by_promotion",
                            False,
                        )
                    ),
                    "verifiable_total": (
                        group_balance.get("total")
                    ),
                    "verifiable_used": (
                        group_balance.get("used")
                    ),
                    "verifiable_available": (
                        group_balance.get(
                            "available"
                        )
                    ),
                    "shared_key": (
                        group_balance.get(
                            "shared_key"
                        )
                    ),
                },
                flush=True,
            )

            normalized_query = (
                "VERIFICABLE:"
                f"{original_query_type}:"
                f"{original_identifier}"
            )
            
            # Dedupe técnico del mismo webhook.
            # Conserva msg_id para bloquear el mismo evento repetido.
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
            
            # Identidad lógica de la solicitud.
            # No incluye msg_id, por lo que también detecta
            # cuando el usuario vuelve a escribir la misma CURP/RFC.
            verifiable_identity = (
                _verifiable_identity_key(
                    instance=instance_name,
                    remote_jid=remote_jid,
                    requester=requester_wa_id,
                    query_type=original_query_type,
                    identifier=original_identifier,
                )
            )
            
            verifiable_processing_key = (
                "rfc:verifiable:processing:"
                f"{verifiable_identity}"
            )
            
            verifiable_completed_key = (
                "rfc:verifiable:completed:"
                f"{verifiable_identity}"
            )

            if redis_conn.exists(
                verifiable_completed_key
            ):
                completed_ttl = redis_conn.ttl(
                    verifiable_completed_key
                )
            
                try:
                    remaining_seconds = max(
                        int(completed_ttl or 0),
                        0,
                    )
                except Exception:
                    remaining_seconds = 0
            
                remaining_hours = max(
                    1,
                    int(
                        (
                            remaining_seconds
                            + 3599
                        )
                        // 3600
                    ),
                )
            
                print(
                    "RFC_VERIFIABLE_COMPLETED_24H_BLOCKED =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                        "requester": requester_wa_id,
                        "query_type": original_query_type,
                        "identifier": original_identifier,
                        "completed_key": (
                            verifiable_completed_key
                        ),
                        "ttl": completed_ttl,
                    },
                    flush=True,
                )
            
                try:
                    send_text(
                        remote_jid,
                        _client_status_message(
                            title="✅ Solicitud ya entregada",
                            requester_label=requester_label,
                            query_type=original_query_type,
                            identifier=original_identifier,
                            body=(
                                "Esta solicitud verificable fue "
                                "entregada durante las últimas 24 horas.\n"
                                "Podrás volver a solicitarla "
                                f"aproximadamente en {remaining_hours} h."
                            ),
                            batch_index=batch_index,
                            batch_total=batch_total,
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception:
                    pass
            
                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_completed_"
                        "within_24h"
                    ),
                    "identifier": original_identifier,
                    "remaining_seconds": (
                        remaining_seconds
                    ),
                }

            verifiable_processing_ttl = max(
                int(
                    VERIFIABLE_TIMEOUT_SEC
                    or 3600
                ) + 300,
                900,
            )
            
            processing_created = redis_conn.set(
                verifiable_processing_key,
                command_key,
                nx=True,
                ex=verifiable_processing_ttl,
            )
            
            if not processing_created:
                processing_ttl = redis_conn.ttl(
                    verifiable_processing_key
                )
            
                print(
                    "RFC_VERIFIABLE_ALREADY_PROCESSING_BLOCKED =",
                    {
                        "instance": instance_name,
                        "group_jid": remote_jid,
                        "requester": requester_wa_id,
                        "query_type": original_query_type,
                        "identifier": original_identifier,
                        "processing_key": (
                            verifiable_processing_key
                        ),
                        "ttl": processing_ttl,
                        "configured_ttl": (
                            verifiable_processing_ttl
                        ),
                    },
                    flush=True,
                )
            
                duplicate_notice_key = (
                    "rfc:verifiable:"
                    "duplicate_processing_notice:"
                    f"{verifiable_identity}"
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
                            _client_status_message(
                                title="⏳ RFC verificable en proceso",
                                requester_label=requester_label,
                                query_type=original_query_type,
                                identifier=original_identifier,
                                body=(
                                    "Esta solicitud ya está siendo procesada.\n"
                                    "No es necesario enviarla nuevamente."
                                ),
                                batch_index=batch_index,
                                batch_total=batch_total,
                            ),
                            instance_name=instance_name,
                            fast=True,
                        )
                    except Exception:
                        pass
            
                return {
                    "ok": True,
                    "ignored": (
                        "verifiable_same_request_"
                        "already_processing"
                    ),
                    "identifier": original_identifier,
                }

            if not redis_conn.set(
                inflight_key,
                "1",
                nx=True,
                ex=86400,
            ):
                # Esta ejecución no continuará, por lo que no debe
                # conservar la llave lógica que acaba de reclamar.
                redis_conn.delete(
                    verifiable_processing_key
                )
            
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
                            _client_status_message(
                                title="⏳ RFC verificable en proceso",
                                requester_label=requester_label,
                                query_type=original_query_type,
                                identifier=original_identifier,
                                body=(
                                    "Esta solicitud ya está siendo procesada.\n"
                                    "No es necesario enviarla nuevamente."
                                ),
                                batch_index=batch_index,
                                batch_total=batch_total,
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
                _choose_verifiable_provider_for_group(
                    db,
                    group_jid=remote_jid,
                    instance_name=instance_name,
                )
            )
            
            if not selected_provider:
                redis_conn.delete(
                    inflight_key
                )
            
                redis_conn.delete(
                    verifiable_processing_key
                )
            
                if _claim_batch_global_notice(
                    batch_child=batch_child,
                    batch_parent_msg_id=batch_parent_msg_id,
                    instance_name=instance_name,
                    remote_jid=remote_jid,
                    notice_code="verifiable_provider_not_available",
                ):
                    try:
                        send_text(
                            remote_jid,
                            (
                                "⚠️ RFC verificable temporalmente no disponible\n"
                                f"👤 {requester_label}\n\n"
                                "No fue posible iniciar el procesamiento en este momento.\n"
                                "Intenta nuevamente más tarde."
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

            provider_selection_mode = (
                selected_provider.get(
                    "selection_mode"
                )
                or "AUTO"
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
            
            provider_uses_rfc_converter = (
                _verifiable_provider_uses_rfc_converter(
                    db,
                    provider_code,
                )
            )

            must_convert_curp_to_rfc = (
                original_query_type == "CURP"
                and provider_uses_rfc_converter
            )

            print(
                "RFC_VERIFIABLE_PROVIDER_"
                "INPUT_MODE =",
                {
                    "provider_code":
                        provider_code,
                    "provider_name":
                        provider_name,
                    "provider_group":
                        provider_group_jid,
                    "original_query_type":
                        original_query_type,
                    "original_identifier":
                        original_identifier,
                    "uses_rfc_converter":
                        provider_uses_rfc_converter,
                    "will_convert":
                        must_convert_curp_to_rfc,
                },
                flush=True,
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
                    # Esta solicitud NO llegó al proveedor.
                    # Por lo tanto debe poder reintentarse
                    # inmediatamente.
                    redis_conn.delete(
                        inflight_key
                    )

                    redis_conn.delete(
                        verifiable_processing_key
                    )

                    conversion_error_text = str(
                        conversion_exc
                        or ""
                    ).strip()

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
                            "inflight_released": (
                                inflight_key
                            ),
                            "processing_released": (
                                verifiable_processing_key
                            ),
                        },
                        flush=True,
                    )

                    # ======================================
                    # 1. CURP NO LOCALIZADA
                    # ======================================
                    if (
                        "VERIFIABLE_CURP_NOT_FOUND"
                        in conversion_error_text
                    ):
                        client_message = (
                            "⚠️ CURP no localizada\n"
                            f"👤 {requester_label}\n"
                            f"🪪 CURP: {original_identifier}\n\n"
                            "No fue localizada en RENAPO.\n"
                            "Verifica que esté escrita correctamente."
                        )

                        error_code = (
                            "verifiable_curp_not_found"
                        )

                    # ======================================
                    # 2. SERVICIOS CURP NO DISPONIBLES
                    # ======================================
                    elif (
                        "VERIFIABLE_CURP_"
                        "SERVICE_UNAVAILABLE"
                        in conversion_error_text
                    ):
                        client_message = (
                            "⚠️ Consulta temporalmente no disponible\n"
                            f"👤 {requester_label}\n"
                            f"🪪 CURP: {original_identifier}\n\n"
                            "No fue posible consultar la CURP en este momento.\n"
                            "Intenta nuevamente más tarde."
                        )

                        error_code = (
                            "verifiable_curp_"
                            "service_unavailable"
                        )

                    # ======================================
                    # 3. SÍ OBTUVIMOS DATOS DE LA CURP,
                    #    PERO FALLÓ MOFFIN / CONVERSIÓN
                    # ======================================
                    else:
                        client_message = (
                            "⚠️ No pudimos completar la solicitud\n"
                            f"👤 {requester_label}\n"
                            f"🪪 CURP: {original_identifier}\n\n"
                            "La CURP fue localizada, pero no fue posible continuar "
                            "con la solicitud verificable.\n"
                            "Intenta nuevamente."
                        )

                        error_code = (
                            "verifiable_curp_to_"
                            "rfc_failed"
                        )

                    try:
                        send_text(
                            remote_jid,
                            client_message,
                            instance_name=(
                                instance_name
                            ),
                            fast=True,
                        )

                    except Exception as notice_exc:
                        print(
                            "[VERIFIABLE_CURP_ERROR_"
                            "NOTICE_FAIL]",
                            {
                                "curp": (
                                    original_identifier
                                ),
                                "error_code": (
                                    error_code
                                ),
                                "error": repr(
                                    notice_exc
                                ),
                            },
                            flush=True,
                        )

                    return {
                        "ok": False,
                        "error": error_code,
                    }
                
            pending_payload = {
                "normal_request_key": (
                    command_key
                ),
                "inflight_key": (
                    inflight_key
                ),
                "verifiable_identity": (
                    verifiable_identity
                ),
                "verifiable_processing_key": (
                    verifiable_processing_key
                ),
                "verifiable_completed_key": (
                    verifiable_completed_key
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
                "batch_index": batch_index,
                "batch_total": batch_total,
                "client_msg_id": msg_id,
                "request_started_at_epoch": (
                    batch_started_at_epoch
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
                "provider_selection_mode": (
                    provider_selection_mode
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

            try:
                save_pending(
                    command_key,
                    pending_payload,
                )
            
            except Exception as pending_exc:
                try:
                    redis_conn.delete(
                        inflight_key
                    )
            
                    redis_conn.delete(
                        verifiable_processing_key
                    )
            
                except Exception as cleanup_exc:
                    print(
                        "RFC_VERIFIABLE_PENDING_"
                        "SAVE_CLEANUP_ERROR =",
                        {
                            "request_key": command_key,
                            "inflight_key": inflight_key,
                            "processing_key": (
                                verifiable_processing_key
                            ),
                            "error": repr(
                                cleanup_exc
                            ),
                        },
                        flush=True,
                    )
            
                print(
                    "RFC_VERIFIABLE_PENDING_"
                    "SAVE_ERROR =",
                    {
                        "request_key": command_key,
                        "identifier": (
                            original_identifier
                        ),
                        "query_type": (
                            original_query_type
                        ),
                        "inflight_key": inflight_key,
                        "processing_key": (
                            verifiable_processing_key
                        ),
                        "error": repr(
                            pending_exc
                        ),
                    },
                    flush=True,
                )
            
                try:
                    send_text(
                        remote_jid,
                        _client_status_message(
                            title="⚠️ No pudimos registrar la solicitud",
                            requester_label=requester_label,
                            query_type=original_query_type,
                            identifier=original_identifier,
                            body=(
                                "Ocurrió una interrupción temporal.\n"
                                "Intenta nuevamente."
                            ),
                            batch_index=batch_index,
                            batch_total=batch_total,
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
            
                except Exception:
                    pass
            
                return {
                    "ok": False,
                    "error": (
                        "verifiable_pending_save_failed"
                    ),
                }

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
            
            except Exception as provider_send_exc:
                redis_conn.delete(
                    inflight_key
                )
            
                redis_conn.delete(
                    verifiable_processing_key
                )
            
                finish_pending(
                    command_key
                )
            
                print(
                    "RFC_VERIFIABLE_PROVIDER_"
                    "SEND_ERROR =",
                    {
                        "request_key": command_key,
                        "provider_code": provider_code,
                        "provider_db_name": (
                            provider_db_name
                        ),
                        "provider_name": provider_name,
                        "provider_group": (
                            provider_group_jid
                        ),
                        "provider_instance": (
                            provider_instance_name
                        ),
                        "error": repr(
                            provider_send_exc
                        ),
                    },
                    flush=True,
                )
            
                try:
                    send_text(
                        remote_jid,
                        _client_status_message(
                            title="⚠️ No pudimos iniciar la solicitud",
                            requester_label=requester_label,
                            query_type=original_query_type,
                            identifier=original_identifier,
                            body=(
                                "No fue posible iniciar el procesamiento.\n"
                                "Intenta nuevamente."
                            ),
                            batch_index=batch_index,
                            batch_total=batch_total,
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
                        "send_failed"
                    ),
                }

            pending_payload[
                "provider_message_id"
            ] = provider_message_id
            
            try:
                associate_provider_message(
                    command_key,
                    provider_message_id,
                )
            
                save_pending(
                    command_key,
                    pending_payload,
                )
            
            except Exception as provider_state_exc:
                print(
                    "RFC_VERIFIABLE_PROVIDER_"
                    "STATE_SAVE_ERROR =",
                    {
                        "request_key": command_key,
                        "provider_message_id": (
                            provider_message_id
                        ),
                        "provider_group": (
                            provider_group_jid
                        ),
                        "provider_instance": (
                            provider_instance_name
                        ),
                        "error": repr(
                            provider_state_exc
                        ),
                    },
                    flush=True,
                )
            
                # No eliminar el pendiente.
                # El proveedor sí recibió la solicitud.

            timeout_job_id = (
                "rfc-verifiable-timeout:"
                f"{command_key}"
            )
            
            try:
                request_queue.enqueue_in(
                    timedelta(
                        seconds=int(
                            VERIFIABLE_TIMEOUT_SEC
                        )
                    ),
                    "worker_jobs."
                    "process_verifiable_timeout_job",
                    command_key,
                    job_id=timeout_job_id,
                    job_timeout=300,
                    result_ttl=86400,
                    failure_ttl=86400,
                )
            
                print(
                    "RFC_VERIFIABLE_TIMEOUT_QUEUED =",
                    {
                        "request_key": command_key,
                        "job_id": timeout_job_id,
                        "timeout_seconds": int(
                            VERIFIABLE_TIMEOUT_SEC
                        ),
                    },
                    flush=True,
                )
            
            except Exception as timeout_enqueue_exc:
                print(
                    "RFC_VERIFIABLE_TIMEOUT_"
                    "ENQUEUE_ERROR =",
                    {
                        "request_key": command_key,
                        "job_id": timeout_job_id,
                        "provider_message_id": (
                            provider_message_id
                        ),
                        "error": repr(
                            timeout_enqueue_exc
                        ),
                    },
                    flush=True,
                )
            
                # El mensaje ya llegó al proveedor.
                # No eliminar el pendiente ni mentir al cliente.
                #
                # El pendiente y processing tienen TTL propio,
                # por lo que eventualmente se liberarán.

            if not batch_suppress_ack:
                try:
                    send_text(
                        remote_jid,
                        _client_verifiable_received_message(
                            requester_label=requester_label,
                            count=1,
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
                    "provider_selection_mode": (
                        selected_provider.get(
                            "selection_mode"
                        )
                        or "AUTO"
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

        group_service = (
            _group_service_config(
                db,
                remote_jid,
                instance_name,
            )
        )
        
        parsed_type = (
            parsed.get("type")
            or ""
        ).strip().upper()
        
        is_clon_request = (
            parsed_type
            in {
                "CURP",
                "RFC_ONLY",
            }
        )
        
        is_idcif_request = (
            parsed_type
            in {
                "RFC_IDCIF",
                "QR_TEXT",
                "IMAGE",
                "DOCUMENT",
            }
        )
        
        if (
            is_clon_request
            and not group_service[
                "clon_enabled"
            ]
        ):
            print(
                "RFC_GROUP_SERVICE_DISABLED =",
                {
                    "group_jid": remote_jid,
                    "instance": instance_name,
                    "service": "CLON",
                    "query_type": parsed_type,
                },
                flush=True,
            )
        
            if _claim_batch_global_notice(
                batch_child=batch_child,
                batch_parent_msg_id=batch_parent_msg_id,
                instance_name=instance_name,
                remote_jid=remote_jid,
                notice_code="group_clon_disabled",
            ):
                try:
                    send_text(
                        remote_jid,
                        (
                            "⚠️ El servicio CLON está "
                            "desactivado temporalmente "
                            "para este grupo."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception as send_exc:
                    print(
                        "RFC_GROUP_SERVICE_DISABLED_"
                        "NOTICE_ERROR =",
                        repr(send_exc),
                        flush=True,
                    )
        
            return {
                "ok": True,
                "ignored":
                    "group_clon_disabled",
            }
        
        if (
            is_idcif_request
            and not group_service[
                "idcif_enabled"
            ]
        ):
            print(
                "RFC_GROUP_SERVICE_DISABLED =",
                {
                    "group_jid": remote_jid,
                    "instance": instance_name,
                    "service": "IDCIF",
                    "query_type": parsed_type,
                },
                flush=True,
            )
        
            if _claim_batch_global_notice(
                batch_child=batch_child,
                batch_parent_msg_id=batch_parent_msg_id,
                instance_name=instance_name,
                remote_jid=remote_jid,
                notice_code="group_idcif_disabled",
            ):
                try:
                    send_text(
                        remote_jid,
                        (
                            "⚠️ El servicio IDCIF/QR "
                            "está desactivado temporalmente "
                            "para este grupo."
                        ),
                        instance_name=instance_name,
                        fast=True,
                    )
                except Exception as send_exc:
                    print(
                        "RFC_GROUP_SERVICE_DISABLED_"
                        "NOTICE_ERROR =",
                        repr(send_exc),
                        flush=True,
                    )
        
            return {
                "ok": True,
                "ignored":
                    "group_idcif_disabled",
            }

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
                        _client_status_message(
                            title="⏳ Solicitud en proceso",
                            requester_label=requester_label,
                            query_type=(parsed.get("type") or ""),
                            identifier=(
                                CURP_RE.search(normalized_query).group(0).upper()
                                if (
                                    parsed.get("type") == "CURP"
                                    and CURP_RE.search(normalized_query)
                                )
                                else (
                                    RFC_RE.search(normalized_query).group(0).upper()
                                    if (
                                        parsed.get("type") == "RFC_ONLY"
                                        and RFC_RE.search(normalized_query)
                                    )
                                    else ""
                                )
                            ),
                            rfc=(
                                RFC_RE.search(normalized_query).group(0).upper()
                                if (
                                    parsed.get("type") == "RFC_IDCIF"
                                    and RFC_RE.search(normalized_query)
                                )
                                else ""
                            ),
                            idcif=(
                                IDCIF_RE.search(normalized_query).group(0)
                                if (
                                    parsed.get("type") == "RFC_IDCIF"
                                    and IDCIF_RE.search(normalized_query)
                                )
                                else ""
                            ),
                            body=(
                                "Esta solicitud ya está siendo procesada.\n"
                                "No es necesario enviarla nuevamente."
                            ),
                            batch_index=batch_index,
                            batch_total=batch_total,
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
            "batch_index": batch_index,
            "batch_total": batch_total,
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
            "request_started_at_epoch": (
                batch_started_at_epoch
            ),
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

        if not batch_suppress_ack:
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
                        _client_received_message(
                            requester_label=requester_label,
                            query_type=(parsed.get("type") or ""),
                            count=1,
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
