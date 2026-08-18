import json
import os
import re
import unicodedata
import hashlib
from typing import Any

from app.queue import request_queue


VERIFIABLE_PROVIDER_GROUP = (
    os.getenv("RFC_VERIFIABLE_PROVIDER_GROUP", "")
    .strip()
)

VERIFIABLE_PROVIDER_INSTANCE = (
    os.getenv("RFC_VERIFIABLE_PROVIDER_INSTANCE", "grupo02")
    .strip()
)

VERIFIABLE_TIMEOUT_SEC = int(
    os.getenv(
        "RFC_VERIFIABLE_TIMEOUT_SEC",
        "86400",
    )
    or "86400"
)


def _env_bool(
    value: str | None,
    default: bool = False,
) -> bool:
    raw = (
        str(value).strip().lower()
        if value is not None
        else ""
    )

    if not raw:
        return default

    return raw in {
        "1",
        "true",
        "yes",
        "on",
        "si",
        "sí",
    }


def load_verifiable_providers() -> list[dict]:
    """
    Metadatos de proveedores desde .env.

    Activo/inactivo y prioridad definitiva se guardan
    posteriormente en provider_settings.
    """

    raw_codes = (
        os.getenv(
            "RFC_VERIFIABLE_PROVIDERS",
            "",
        )
        or ""
    ).strip()

    codes = [
        code.strip().upper()
        for code in raw_codes.split(",")
        if code.strip()
    ]

    providers: list[dict] = []

    for code in codes:
        name = (
            os.getenv(
                f"RFC_{code}_NAME",
                code,
            )
            or code
        ).strip()

        group_jid = (
            os.getenv(
                f"RFC_{code}_GROUP",
                "",
            )
            or ""
        ).strip()

        instance_name = (
            os.getenv(
                f"RFC_{code}_INSTANCE",
                VERIFIABLE_PROVIDER_INSTANCE,
            )
            or VERIFIABLE_PROVIDER_INSTANCE
        ).strip()

        try:
            default_weight = float(
                os.getenv(
                    f"RFC_{code}_DEFAULT_WEIGHT",
                    "1",
                )
                or "1"
            )
        except Exception:
            default_weight = 1.0

        default_enabled = _env_bool(
            os.getenv(
                f"RFC_{code}_DEFAULT_ENABLED",
                "1",
            ),
            default=True,
        )

        if not group_jid:
            print(
                "RFC_VERIFIABLE_PROVIDER_SKIPPED =",
                {
                    "code": code,
                    "reason": "empty_group_jid",
                },
                flush=True,
            )
            continue

        providers.append(
            {
                "code": code,
                "db_name": (
                    f"RFC_VERIFIABLE_{code}"
                ),
                "name": name,
                "group_jid": group_jid,
                "instance_name": instance_name,
                "default_weight": max(
                    default_weight,
                    0.0,
                ),
                "default_enabled": (
                    default_enabled
                ),
            }
        )

    # Compatibilidad temporal con el proveedor anterior.
    if (
        not providers
        and VERIFIABLE_PROVIDER_GROUP
    ):
        providers.append(
            {
                "code": "LEGACY",
                "db_name": (
                    "RFC_VERIFIABLE_LEGACY"
                ),
                "name": "RFC VERIFICABLE",
                "group_jid": (
                    VERIFIABLE_PROVIDER_GROUP
                ),
                "instance_name": (
                    VERIFIABLE_PROVIDER_INSTANCE
                ),
                "default_weight": 1.0,
                "default_enabled": True,
            }
        )

    return providers


def verifiable_provider_by_group(
    group_jid: str,
    instance_name: str,
) -> dict:
    group_jid = (
        group_jid or ""
    ).strip()

    instance_name = (
        instance_name or ""
    ).strip()

    for provider in (
        load_verifiable_providers()
    ):
        if (
            provider["group_jid"]
            == group_jid
            and provider["instance_name"]
            == instance_name
        ):
            return provider

    return {}


def verifiable_provider_by_db_name(
    db_name: str,
) -> dict:
    db_name = (
        db_name or ""
    ).strip().upper()

    for provider in (
        load_verifiable_providers()
    ):
        if (
            provider["db_name"].upper()
            == db_name
        ):
            return provider

    return {}


CURP_FULL_RE = re.compile(
    r"^[A-Z][AEIOUX][A-Z]{2}"
    r"\d{6}[HM][A-Z]{5}[A-Z0-9]\d$",
    re.I,
)

RFC_FULL_RE = re.compile(
    r"^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}$",
    re.I,
)

RFC_SEARCH_RE = re.compile(
    r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b",
    re.I,
)

IDCIF_SEARCH_RE = re.compile(
    r"\b\d{11}\b",
)


def redis_connection():
    return request_queue.connection


def normalize_token(value: str) -> str:
    return re.sub(
        r"\s+",
        "",
        (value or "").strip().upper(),
    )


def _hamming_distance(
    left: str,
    right: str,
) -> int:
    left = normalize_token(left)
    right = normalize_token(right)

    if len(left) != len(right):
        return max(len(left), len(right))

    return sum(
        1
        for a, b in zip(left, right)
        if a != b
    )


def _near_curp_rfc_match(
    original_curp: str,
    provider_rfc: str,
) -> bool:
    """
    Permite una corrección limitada cuando el cliente
    escribió mal letras de la CURP y el proveedor regresó
    el RFC correcto.

    Requisitos:
    - CURP de 18 caracteres.
    - RFC de persona física de 13 caracteres.
    - Misma fecha YYMMDD.
    - Máximo 2 diferencias en las primeras 4 letras.
    """
    original_curp = normalize_token(
        original_curp
    )

    provider_rfc = normalize_token(
        provider_rfc
    )

    if not CURP_FULL_RE.fullmatch(
        original_curp
    ):
        return False

    if not re.fullmatch(
        r"[A-ZÑ&]{4}\d{6}[A-Z0-9]{3}",
        provider_rfc,
        re.I,
    ):
        return False

    # CURP y RFC deben conservar la misma fecha.
    if original_curp[4:10] != provider_rfc[4:10]:
        return False

    return (
        _hamming_distance(
            original_curp[:4],
            provider_rfc[:4],
        )
        <= 2
    )


def _near_rfc_match(
    original_rfc: str,
    provider_rfc: str,
) -> bool:
    """
    Corrección limitada para una solicitud RFC_ONLY.

    Solo permite:
    - misma longitud;
    - misma fecha YYMMDD;
    - máximo 2 caracteres distintos en todo el RFC.
    """
    original_rfc = normalize_token(
        original_rfc
    )

    provider_rfc = normalize_token(
        provider_rfc
    )

    if not RFC_FULL_RE.fullmatch(
        original_rfc
    ):
        return False

    if not RFC_FULL_RE.fullmatch(
        provider_rfc
    ):
        return False

    if len(original_rfc) != len(provider_rfc):
        return False

    prefix_len = 4 if len(original_rfc) == 13 else 3

    if (
        original_rfc[
            prefix_len:prefix_len + 6
        ]
        != provider_rfc[
            prefix_len:prefix_len + 6
        ]
    ):
        return False

    return (
        _hamming_distance(
            original_rfc,
            provider_rfc,
        )
        <= 2
    )


VERIFIABLE_EXACT_ALIASES = {
    "V",
    "VE",
    "VER",
    "VERI",
    "VERIF",
    "VERIFIC",
    "VERIFICA",
    "VERIFICAB",
    "VERIFICABL",
    "VERIFICABLE",
    "VERIFICABLES",
}


def _verifiable_word_distance(
    left: str,
    right: str,
) -> int:
    """
    Distancia de edición simple.

    Permite detectar errores como:
    - VERIFIACBLE
    - VEIIFICABLE
    - VEIFABLE
    - VERIFCABLE
    """
    left = str(
        left or ""
    ).strip().upper()

    right = str(
        right or ""
    ).strip().upper()

    if left == right:
        return 0

    if not left:
        return len(right)

    if not right:
        return len(left)

    previous = list(
        range(
            len(right) + 1
        )
    )

    for left_index, left_char in enumerate(
        left,
        start=1,
    ):
        current = [left_index]

        for right_index, right_char in enumerate(
            right,
            start=1,
        ):
            insert_cost = (
                current[right_index - 1]
                + 1
            )

            delete_cost = (
                previous[right_index]
                + 1
            )

            replace_cost = (
                previous[right_index - 1]
                + (
                    0
                    if left_char == right_char
                    else 1
                )
            )

            current.append(
                min(
                    insert_cost,
                    delete_cost,
                    replace_cost,
                )
            )

        previous = current

    return previous[-1]


def _normalize_verifiable_text(
    value: str,
) -> str:
    """
    Convierte a mayúsculas y elimina acentos.

    Ejemplos:
    - verificáble  -> VERIFICABLE
    - verifícable  -> VERIFICABLE
    - vérificable  -> VERIFICABLE
    - verificablé  -> VERIFICABLE
    """
    value = str(
        value or ""
    ).strip().upper()

    value = unicodedata.normalize(
        "NFD",
        value,
    )

    value = "".join(
        char
        for char in value
        if unicodedata.category(char)
        != "Mn"
    )

    return value


def _is_verifiable_keyword(
    token: str,
) -> bool:
    """
    Reconoce abreviaciones exactas y errores comunes.

    Las abreviaciones muy cortas solamente se admiten
    cuando coinciden exactamente, para evitar que una
    palabra cualquiera sea tomada como verificable.
    """
    token = _normalize_verifiable_text(
        token
    )
    
    token = re.sub(
        r"[^A-Z]",
        "",
        token,
    )

    if not token:
        return False

    if token in VERIFIABLE_EXACT_ALIASES:
        return True

    # Evitar fuzzy matching sobre palabras demasiado cortas.
    # VE, VER, VERI y VERIF ya están cubiertas arriba.
    if len(token) < 5:
        return False

    reference = "VERIFICABLE"

    distance = (
        _verifiable_word_distance(
            token,
            reference,
        )
    )

    # Palabras de 5 a 7 caracteres:
    # máximo 2 errores.
    if len(token) <= 7:
        return distance <= 2

    # Palabras de 8 o más caracteres:
    # máximo 3 errores.
    return distance <= 3


def parse_verifiable_request(
    text: str,
) -> dict[str, Any]:
    """
    Detecta una solicitud RFC verificable con una sola
    CURP o un solo RFC.

    Admite el indicador antes, después, en la misma línea
    o en líneas separadas.

    Ejemplos:

        VERIFICABLE
        RAMC801125MDGMRR05

        RAMC801125MDGMRR05
        VERIFICABLE

        RAMC801125MDGMRR05 VERIFICABLE

        VERIFICABLE RAMC801125MDGMRR05

        VERI ROSA060919RA1
        ROSA060919RA1 VERIF

        VERIFIACBLE ROSA060919RA1
        VEIFABLE ROSA060919RA1

    No admite:
    - más de una CURP/RFC;
    - RFC + IDCIF;
    - QR;
    - una palabra verificable sin dato.
    """
    raw = str(
        text or ""
    ).strip()

    if not raw:
        return {
            "is_verifiable": False,
        }

    upper = _normalize_verifiable_text(
        raw
    )
    
    normalized_text = re.sub(
        r"[^A-Z&0-9]+",
        " ",
        upper,
    ).strip()

    tokens = [
        token.strip()
        for token in normalized_text.split()
        if token.strip()
    ]

    if not tokens:
        return {
            "is_verifiable": False,
        }

    keyword_indexes: set[int] = set()

    for index, token in enumerate(tokens):
        if _is_verifiable_keyword(
            token
        ):
            keyword_indexes.add(index)

    # Si no hay indicador verificable, el mensaje debe
    # continuar por el flujo normal de CURP/RFC.
    if not keyword_indexes:
        return {
            "is_verifiable": False,
        }

    valid_tokens: list[
        tuple[str, str]
    ] = []

    idcif_tokens: list[str] = []

    unknown_tokens: list[str] = []

    for index, token in enumerate(tokens):
        if index in keyword_indexes:
            continue

        normalized = normalize_token(
            token
        )

        if CURP_FULL_RE.fullmatch(
            normalized
        ):
            valid_tokens.append(
                (
                    "CURP",
                    normalized,
                )
            )

            continue

        if RFC_FULL_RE.fullmatch(
            normalized
        ):
            valid_tokens.append(
                (
                    "RFC_ONLY",
                    normalized,
                )
            )

            continue

        if re.fullmatch(
            r"\d{11}",
            normalized,
        ):
            idcif_tokens.append(
                normalized
            )

            continue

        unknown_tokens.append(
            normalized
        )

    # ============================================================
    # VERIFICABLE ESTRICTO
    # ============================================================
    #
    # La sola presencia de palabras como VERIF / VERIFICABLE
    # dentro de un texto administrativo NO debe convertir todo
    # el mensaje en una solicitud verificable.
    #
    # Solo permitimos como texto auxiliar:
    #   RFC
    #   CURP
    #
    # Ejemplos válidos:
    #   VERIFICABLE RAMC801125MDGMRR05
    #   RAMC801125MDGMRR05 VERIF
    #   RFC VERIFICABLE ROSA060919RA1
    #   CURP VERIFICABLE RAMC801125MDGMRR05
    #
    # Cualquier otro texto adicional hace que el mensaje se
    # reconozca como relacionado con "verificable", pero se
    # ignore silenciosamente para evitar:
    #   - responder a cortes;
    #   - interpretar mensajes de otros bots;
    #   - enviarlo después al parser RFC normal.
    # ============================================================
    allowed_auxiliary_tokens = {
        "RFC",
        "CURP",
    }

    unexpected_tokens = [
        token
        for token in unknown_tokens
        if token
        and token not in allowed_auxiliary_tokens
    ]

    if unexpected_tokens:
        return {
            "is_verifiable": True,
            "ok": False,
            "silent_ignore": True,
            "reason": "verifiable_extra_text",
            "unexpected_tokens": unexpected_tokens,
        }

    # RFC + IDCIF no debe entrar como verificable.
    if idcif_tokens:
        return {
            "is_verifiable": True,
            "ok": False,
            "error": (
                "⚠️ Para RFC verificable envía "
                "solamente una CURP o un RFC, "
                "sin IDCIF."
            ),
        }

    # Exactamente un dato válido.
    if len(valid_tokens) != 1:
        return {
            "is_verifiable": True,
            "ok": False,
            "error": (
                "⚠️ Para RFC verificable envía "
                "solamente una CURP o un RFC."
            ),
        }

    query_type, identifier = (
        valid_tokens[0]
    )

    matched_keywords = [
        tokens[index]
        for index in sorted(
            keyword_indexes
        )
    ]

    return {
        "is_verifiable": True,
        "ok": True,
        "query_type": query_type,
        "identifier": identifier,
        "verifiable_keywords": (
            matched_keywords
        ),
        "unknown_tokens": (
            unknown_tokens
        ),
    }


def extract_rfc_idcif_pairs(
    text: str,
) -> list[tuple[str, str]]:
    """
    Extrae todos los pares RFC + IDCIF respetando
    el orden lógico de los resultados.

    Acepta:

        RFC IDCIF

        IDCIF RFC

        RFC
        IDCIF

        IDCIF
        RFC

    También acepta listas mixtas con varios resultados.
    """

    upper = (
        text or ""
    ).strip().upper()

    if not upper:
        return []

    combined_re = re.compile(
        r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b"
        r"|"
        r"\b\d{11}\b",
        re.I,
    )

    tokens = [
        match.group(0).upper()
        for match in combined_re.finditer(
            upper
        )
    ]

    pairs: list[tuple[str, str]] = []

    pending_rfc = ""
    pending_idcif = ""

    for token in tokens:

        # ==============================
        # RFC
        # ==============================
        if RFC_FULL_RE.fullmatch(token):

            # Caso:
            #
            # IDCIF
            # RFC
            #
            # o:
            #
            # IDCIF RFC
            if pending_idcif:
                pairs.append(
                    (
                        token,
                        pending_idcif,
                    )
                )

                pending_idcif = ""
                pending_rfc = ""
                continue

            # Caso normal:
            #
            # RFC
            # IDCIF
            pending_rfc = token
            continue

        # ==============================
        # IDCIF
        # ==============================
        if IDCIF_SEARCH_RE.fullmatch(token):

            # Caso:
            #
            # RFC
            # IDCIF
            #
            # o:
            #
            # RFC IDCIF
            if pending_rfc:
                pairs.append(
                    (
                        pending_rfc,
                        token,
                    )
                )

                pending_rfc = ""
                pending_idcif = ""
                continue

            # Todavía no apareció RFC.
            # Lo conservamos por si viene después.
            pending_idcif = token

    # Evitar procesar dos veces exactamente
    # la misma pareja dentro del mismo mensaje.
    unique_pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for pair in pairs:
        if pair in seen:
            continue

        seen.add(pair)
        unique_pairs.append(pair)

    return unique_pairs


def extract_rfc_idcif(
    text: str,
) -> tuple[str, str]:
    """
    Compatibilidad con código anterior:
    devuelve únicamente la primera pareja.
    """

    pairs = extract_rfc_idcif_pairs(
        text
    )

    if not pairs:
        return "", ""

    return pairs[0]


def extract_quoted_message_id(
    message: dict,
    data: dict,
) -> str:
    """
    Extrae el ID del mensaje citado desde las distintas
    estructuras que puede enviar Evolution/Baileys.
    """

    id_fields = (
        "stanzaId",
        "quotedStanzaId",
        "quotedStanzaID",
        "quotedMessageId",
        "quotedMessageID",
    )

    def _direct_id(node) -> str:
        if not isinstance(node, dict):
            return ""

        for field in id_fields:
            value = node.get(field)

            if value:
                return str(value).strip()

        return ""

    def _search(
        node,
        depth: int = 0,
    ) -> str:
        if depth > 15:
            return ""

        if isinstance(node, dict):
            # El nodo actual puede ser directamente contextInfo.
            found = _direct_id(node)

            if found:
                return found

            # O puede contener contextInfo.
            context = node.get("contextInfo")

            found = _direct_id(context)

            if found:
                return found

            # Buscar recursivamente en toda la estructura.
            for value in node.values():
                found = _search(
                    value,
                    depth + 1,
                )

                if found:
                    return found

        elif isinstance(node, list):
            for item in node:
                found = _search(
                    item,
                    depth + 1,
                )

                if found:
                    return found

        return ""

    try:
        found = _search(message)

        if found:
            return found

        found = _search(data)

        if found:
            return found

    except Exception as exc:
        print(
            "RFC_VERIFIABLE_QUOTE_EXTRACT_ERROR =",
            repr(exc),
            flush=True,
        )

    return ""


def verifiable_request_key(
    instance_name: str,
    group_jid: str,
    requester: str,
    identifier: str,
) -> str:
    base = "|".join(
        [
            (instance_name or "").strip(),
            (group_jid or "").strip(),
            (requester or "").strip(),
            normalize_token(identifier),
        ]
    )

    digest = hashlib.sha256(
        base.encode("utf-8")
    ).hexdigest()

    return digest


def pending_key(request_key: str) -> str:
    return (
        f"rfc:verifiable:pending:{request_key}"
    )


def provider_message_key(
    provider_message_id: str,
) -> str:
    return (
        "rfc:verifiable:provider_message:"
        f"{provider_message_id}"
    )


def result_claim_key(request_key: str) -> str:
    return (
        f"rfc:verifiable:result_claim:{request_key}"
    )


def save_pending(
    request_key: str,
    payload: dict,
):
    redis_conn = redis_connection()

    redis_conn.setex(
        pending_key(request_key),
        int(
            VERIFIABLE_TIMEOUT_SEC
        ) + 300,
        json.dumps(
            payload,
            ensure_ascii=False,
        ),
    )


def load_pending(
    request_key: str,
) -> dict:
    redis_conn = redis_connection()

    raw = redis_conn.get(
        pending_key(request_key)
    )

    if not raw:
        return {}

    if isinstance(raw, bytes):
        raw = raw.decode(
            "utf-8",
            errors="ignore",
        )

    try:
        result = json.loads(raw)
    except Exception:
        return {}

    return result if isinstance(result, dict) else {}


def associate_provider_message(
    request_key: str,
    provider_message_id: str,
):
    redis_conn = redis_connection()

    redis_conn.setex(
        provider_message_key(
            provider_message_id
        ),
        int(
            VERIFIABLE_TIMEOUT_SEC
        ) + 300,
        request_key,
    )


def request_key_from_provider_message(
    provider_message_id: str,
) -> str:
    redis_conn = redis_connection()

    value = redis_conn.get(
        provider_message_key(
            provider_message_id
        )
    )

    if isinstance(value, bytes):
        value = value.decode(
            "utf-8",
            errors="ignore",
        )

    return str(value or "").strip()


def find_pending_request_by_provider_rfc(
    provider_rfc: str,
    provider_group_jid: str = "",
    provider_instance: str = "",
) -> dict:
    """
    Resuelve una respuesta del proveedor sin cita.

    Reglas:
    - Si la solicitud original era RFC_ONLY:
      el RFC recibido debe coincidir exactamente.
    - Si la solicitud original era CURP:
      los primeros 10 caracteres de la CURP deben
      coincidir con los primeros 10 del RFC recibido.
    - Solo se acepta si existe exactamente una
      solicitud pendiente compatible.
    """

    redis_conn = redis_connection()

    provider_rfc = normalize_token(
        provider_rfc
    )

    provider_group_jid = (
        provider_group_jid or ""
    ).strip()

    provider_instance = (
        provider_instance or ""
    ).strip()

    if not RFC_FULL_RE.fullmatch(
        provider_rfc
    ):
        return {
            "ok": False,
            "reason": "invalid_provider_rfc",
            "matches": [],
        }

    # Persona física: 4 letras + 6 dígitos.
    # Un RFC moral tiene 3 letras y no permite
    # relacionarlo con una CURP.
    physical_rfc_prefix = ""

    if re.fullmatch(
        r"[A-ZÑ&]{4}\d{6}[A-Z0-9]{3}",
        provider_rfc,
        re.I,
    ):
        physical_rfc_prefix = (
            provider_rfc[:10]
        )

    matches: list[dict] = []

    pattern = "rfc:verifiable:pending:*"

    for redis_key in redis_conn.scan_iter(
        match=pattern,
        count=200,
    ):
        try:
            key_text = (
                redis_key.decode(
                    "utf-8",
                    errors="ignore",
                )
                if isinstance(redis_key, bytes)
                else str(redis_key)
            )

            request_key = key_text.split(
                "rfc:verifiable:pending:",
                1,
            )[-1].strip()

            if not request_key:
                continue

            pending = load_pending(
                request_key
            )

            if not pending:
                continue

            pending_provider_group = (
                pending.get(
                    "provider_group_jid"
                )
                or ""
            ).strip()

            pending_provider_instance = (
                pending.get(
                    "provider_instance"
                )
                or ""
            ).strip()

            if (
                provider_group_jid
                and pending_provider_group
                != provider_group_jid
            ):
                continue

            if (
                provider_instance
                and pending_provider_instance
                != provider_instance
            ):
                continue

            original_type = str(
                pending.get(
                    "original_query_type"
                )
                or ""
            ).strip().upper()

            original_identifier = (
                normalize_token(
                    pending.get(
                        "original_identifier"
                    )
                    or ""
                )
            )

            provider_identifier = (
                normalize_token(
                    pending.get(
                        "provider_identifier"
                    )
                    or ""
                )
            )

            matched_by = ""

            if (
                provider_identifier
                and provider_identifier
                == provider_rfc
            ):
                matched_by = (
                    "exact_provider_identifier"
                )
            
            elif (
                original_type == "RFC_ONLY"
                and original_identifier
                == provider_rfc
            ):
                matched_by = "exact_rfc"

            elif (
                original_type == "CURP"
                and physical_rfc_prefix
                and len(original_identifier) >= 10
                and original_identifier[:10]
                == physical_rfc_prefix
            ):
                matched_by = "curp_rfc_prefix"

            elif (
                original_type == "CURP"
                and _near_curp_rfc_match(
                    original_identifier,
                    provider_rfc,
                )
            ):
                matched_by = (
                    "curp_rfc_near_correction"
                )
            
            elif (
                original_type == "RFC_ONLY"
                and _near_rfc_match(
                    original_identifier,
                    provider_rfc,
                )
            ):
                matched_by = (
                    "rfc_near_correction"
                )

            if not matched_by:
                continue

            matches.append({
                "request_key": request_key,
                "pending": pending,
                "matched_by": matched_by,
                "original_type": original_type,
                "original_identifier": (
                    original_identifier
                ),
            })

        except Exception as scan_exc:
            print(
                "RFC_VERIFIABLE_PENDING_SCAN_ERROR =",
                {
                    "redis_key": str(
                        redis_key
                    ),
                    "error": repr(
                        scan_exc
                    ),
                },
                flush=True,
            )

    # RFC_MATCH_PRIORITY_V1
    # Si existe una coincidencia exacta del dato
    # enviado al proveedor, ésta siempre tiene
    # prioridad sobre matches aproximados.
    exact_provider_matches = [
        item
        for item in matches
        if (
            item.get("matched_by")
            == "exact_provider_identifier"
        )
    ]

    if exact_provider_matches:
        matches = exact_provider_matches

    else:
        exact_rfc_matches = [
            item
            for item in matches
            if (
                item.get("matched_by")
                == "exact_rfc"
            )
        ]

        if exact_rfc_matches:
            matches = exact_rfc_matches

    if len(matches) == 1:
        return {
            "ok": True,
            "unique": True,
            **matches[0],
            "matches": matches,
        }

    if not matches:
        return {
            "ok": False,
            "unique": False,
            "reason": "no_pending_match",
            "matches": [],
        }

    normalized_identifiers = {
        (
            item.get(
                "original_type"
            )
            or ""
        ).strip().upper()
        + ":"
        + normalize_token(
            item.get(
                "original_identifier"
            )
            or ""
        )
        for item in matches
    }
    
    return {
        "ok": False,
        "unique": False,
        "reason": "ambiguous_pending_match",
        "same_original_request": (
            len(normalized_identifiers) == 1
        ),
        "matches": matches,
    }


def claim_provider_result(
    request_key: str,
) -> bool:
    """
    Impide que dos respuestas del proveedor
    generen dos PDFs.
    """
    redis_conn = redis_connection()

    return bool(
        redis_conn.set(
            result_claim_key(request_key),
            "1",
            nx=True,
            ex=(
                int(
                    VERIFIABLE_TIMEOUT_SEC
                )
                + 300
            ),
        )
    )


def release_provider_result_claim(
    request_key: str,
):
    redis_connection().delete(
        result_claim_key(request_key)
    )


def finish_pending(
    request_key: str,
    provider_message_id: str = "",
):
    redis_conn = redis_connection()

    keys = [
        pending_key(request_key),
    ]

    if provider_message_id:
        keys.append(
            provider_message_key(
                provider_message_id
            )
        )

    redis_conn.delete(*keys)
