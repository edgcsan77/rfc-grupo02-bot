import json
import os
import re
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
        "1200",
    )
    or "1200"
)


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


def parse_verifiable_request(text: str) -> dict[str, Any]:
    """
    Formatos admitidos:

        VERIFICABLE RAMC801125MDGMRR05
        RAMC801125MDGMRR05 VERIFICABLE

        VERIFICABLE ROSA060919RA1
        ROSA060919RA1 VERIFICABLE

    Solo admite una CURP o un RFC.
    No admite QR ni RFC + IDCIF.
    """
    raw = (text or "").strip()
    upper = raw.upper()

    if not re.search(
        r"\bVERIFICABLE(?:S)?\b",
        upper,
    ):
        return {
            "is_verifiable": False,
        }

    clean = re.sub(
        r"\bVERIFICABLE(?:S)?\b",
        " ",
        upper,
    )

    clean = re.sub(
        r"[^A-ZÑ&0-9]+",
        " ",
        clean,
    ).strip()

    tokens = [
        token.strip()
        for token in clean.split()
        if token.strip()
    ]

    valid_tokens: list[tuple[str, str]] = []

    for token in tokens:
        normalized = normalize_token(token)

        if CURP_FULL_RE.fullmatch(normalized):
            valid_tokens.append(
                ("CURP", normalized)
            )

        elif RFC_FULL_RE.fullmatch(normalized):
            valid_tokens.append(
                ("RFC_ONLY", normalized)
            )

    if len(valid_tokens) != 1:
        return {
            "is_verifiable": True,
            "ok": False,
            "error": (
                "⚠️ Para RFC verificable envía únicamente:\n\n"
                "VERIFICABLE + CURP\n"
                "o\n"
                "VERIFICABLE + RFC"
            ),
        }

    query_type, identifier = valid_tokens[0]

    return {
        "is_verifiable": True,
        "ok": True,
        "query_type": query_type,
        "identifier": identifier,
    }


def extract_rfc_idcif(text: str) -> tuple[str, str]:
    upper = (text or "").strip().upper()

    rfc_match = RFC_SEARCH_RE.search(upper)
    idcif_match = IDCIF_SEARCH_RE.search(upper)

    rfc = (
        rfc_match.group(0).upper()
        if rfc_match
        else ""
    )

    idcif = (
        idcif_match.group(0)
        if idcif_match
        else ""
    )

    return rfc, idcif


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
        VERIFIABLE_TIMEOUT_SEC,
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
        VERIFIABLE_TIMEOUT_SEC,
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
            ex=VERIFIABLE_TIMEOUT_SEC,
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
