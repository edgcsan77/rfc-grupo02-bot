from __future__ import annotations

import re
from datetime import date

CURP_VALUE_TABLE = "0123456789ABCDEFGHIJKLMNÑOPQRSTUVWXYZ"
CURP_VALUE = {ch: idx for idx, ch in enumerate(CURP_VALUE_TABLE)}

CURP_STRICT_RE = re.compile(
    r"^[A-Z][AEIOUX][A-Z]{2}"
    r"\d{6}"
    r"[HM]"
    r"[A-Z]{2}"
    r"[A-Z]{3}"
    r"[A-Z0-9]"
    r"\d$"
)

VALID_ENTITIES = {
    "AS", "BC", "BS", "CC", "CL", "CM", "CS", "CH", "DF", "DG",
    "GT", "GR", "HG", "JC", "MC", "MN", "MS", "NT", "NL", "OC",
    "PL", "QT", "QR", "SP", "SL", "SR", "TC", "TS", "TL", "VZ",
    "YN", "ZS", "NE",
}

# Correcciones conservadoras. Sólo se aplican cuando el carácter está en una
# posición donde su tipo es imposible (letra en posición numérica o viceversa).
_NUMERIC_CONFUSIONS = {
    "O": "0",
    "I": "1",
    "L": "1",
}

_ALPHA_CONFUSIONS = {
    "0": "O",
    "1": "I",
}


def _clean(value: str) -> str:
    return re.sub(r"[^A-Z0-9Ñ]", "", str(value or "").upper())


def calculate_check_digit(first17: str) -> str:
    first17 = _clean(first17)
    if len(first17) != 17:
        raise ValueError("CURP_FIRST17_LENGTH")

    total = 0
    for idx, ch in enumerate(first17):
        if ch not in CURP_VALUE:
            raise ValueError(f"CURP_INVALID_CHAR:{ch}")
        weight = 18 - idx
        total += CURP_VALUE[ch] * weight

    return str((10 - (total % 10)) % 10)


def _date_is_possible(curp: str) -> bool:
    yy = int(curp[4:6])
    mm = int(curp[6:8])
    dd = int(curp[8:10])

    # El carácter 17 orienta el siglo (dígito = 1900s, letra = 2000s), pero
    # aceptamos cualquiera de ambos siglos para no invalidar registros atípicos.
    years = [1900 + yy, 2000 + yy]
    for year in years:
        try:
            date(year, mm, dd)
            return True
        except ValueError:
            continue
    return False


def _structural_error(curp: str) -> str | None:
    if len(curp) != 18:
        return "INVALID_LENGTH"
    if not CURP_STRICT_RE.fullmatch(curp):
        return "INVALID_STRUCTURE"
    if curp[11:13] not in VALID_ENTITIES:
        return "INVALID_ENTITY"
    if not _date_is_possible(curp):
        return "INVALID_DATE"
    return None


def _repair_position_types(curp: str) -> tuple[str, list[dict]]:
    chars = list(curp)
    corrections: list[dict] = []

    numeric_positions = set(range(4, 10)) | {17}
    alpha_positions = {0, 1, 2, 3, 10, 11, 12, 13, 14, 15}

    for idx, ch in enumerate(chars):
        new_ch = None
        if idx in numeric_positions and not ch.isdigit():
            new_ch = _NUMERIC_CONFUSIONS.get(ch)
        elif idx in alpha_positions and not ("A" <= ch <= "Z"):
            new_ch = _ALPHA_CONFUSIONS.get(ch)

        if new_ch and new_ch != ch:
            corrections.append({
                "position": idx + 1,
                "from": ch,
                "to": new_ch,
            })
            chars[idx] = new_ch

    return "".join(chars), corrections


def analyze_curp(value: str, *, allow_repair: bool = True) -> dict:
    original = str(value or "")
    cleaned = _clean(original)

    result = {
        "original": original,
        "normalized": cleaned,
        "status": "INVALID",
        "valid": False,
        "corrected": False,
        "corrections": [],
        "error": "",
        "expected_check_digit": "",
        "suggested": "",
    }

    if len(cleaned) != 18:
        result["error"] = "INVALID_LENGTH"
        return result

    candidate = cleaned
    corrections: list[dict] = []

    if allow_repair:
        candidate, corrections = _repair_position_types(candidate)

    result["normalized"] = candidate
    result["corrections"] = corrections

    structural_error = _structural_error(candidate)
    if structural_error:
        result["error"] = structural_error
        return result

    expected = calculate_check_digit(candidate[:17])
    result["expected_check_digit"] = expected

    if candidate[17] != expected:
        result["error"] = "INVALID_CHECK_DIGIT"
        result["suggested"] = candidate[:17] + expected
        return result

    result["valid"] = True
    result["corrected"] = bool(corrections)
    result["status"] = "CORRECTED" if corrections else "VALID"
    result["error"] = ""
    return result


def normalize_valid_curp(value: str, *, allow_repair: bool = True) -> str:
    result = analyze_curp(value, allow_repair=allow_repair)
    if not result["valid"]:
        raise ValueError(result["error"] or "INVALID_CURP")
    return str(result["normalized"])


def looks_like_curp_token(value: str) -> bool:
    token = _clean(value)
    if len(token) != 18:
        return False

    # Evita clasificar cadenas arbitrarias como CURP: exigimos que conserve
    # varias señales posicionales fuertes aun cuando tenga O/0 o I/1 mal.
    score = 0
    if token[10] in {"H", "M"}:
        score += 2
    if token[0].isalpha() and token[2].isalpha() and token[3].isalpha():
        score += 2
    if token[11:13].isalpha():
        score += 1
    if token[13:16].isalpha():
        score += 1
    if sum(ch.isdigit() for ch in token[4:10]) >= 4:
        score += 2

    return score >= 5
