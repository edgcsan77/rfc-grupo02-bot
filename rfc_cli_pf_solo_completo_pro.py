# rfc_cli_pf_solo_completo_pro.py
# -*- coding: utf-8 -*-

import re
import unicodedata
from datetime import datetime

# ============================================================
#  CONFIG
# ============================================================

NOMBRES_COMUNES = {
    "JOSE", "JOSÉ", "J", "J.",
    "MARIA", "MARÍA", "MA", "MA."
}

PARTICULAS = {
    "DE", "DEL", "LA", "LAS", "LOS", "Y", "MC", "MAC", "VON", "VAN"
}

# Palabras inconvenientes (4 letras) -> sustitución (clásica SAT)
ALTISONANTES_MAP = {
    "BUEI": "BUEX",
    "BUEY": "BUEX",
    "CACA": "CACX",
    "CACO": "CACX",
    "CAGA": "CAGX",
    "CAGO": "CAGX",
    "CAKA": "CAKX",
    "CAKO": "CAKX",
    "COGE": "COGX",
    "COJA": "COJX",
    "COJE": "COJX",
    "COJI": "COJX",
    "COJO": "COJX",
    "CULO": "CULX",
    "FETO": "FETX",
    "GUEY": "GUEX",
    "JOTO": "JOTX",
    "KACA": "KACX",
    "KACO": "KACX",
    "KAGA": "KAGX",
    "KAGO": "KAGX",
    "KOGE": "KOGX",
    "KOJO": "KOJX",
    "KAKA": "KAKX",
    "KULO": "KULX",
    "MAME": "MAMX",
    "MAMO": "MAMX",
    "MEAR": "MEAX",
    "MEAS": "MEAX",
    "MEON": "MEOX",
    "MION": "MIOX",
    "MOCO": "MOCX",
    "MULA": "MULX",
    "PEDA": "PEDX",
    "PEDO": "PEDX",
    "PENE": "PENX",
    "PUTA": "PUTX",
    "PUTO": "PUTX",
    "QULO": "QULX",
    "RATA": "RATX",
    "RUIN": "RUIX",
}

# Tabla homoclave (Anexo II)
HOMO_TABLE = {
    " ": "00",
    "0": "00","1": "01","2": "02","3": "03","4": "04","5": "05","6": "06","7": "07","8": "08","9": "09",
    "&": "10",
    "A": "11","B": "12","C": "13","D": "14","E": "15","F": "16","G": "17","H": "18","I": "19",
    "J": "21","K": "22","L": "23","M": "24","N": "25","O": "26","P": "27","Q": "28","R": "29",
    "S": "32","T": "33","U": "34","V": "35","W": "36","X": "37","Y": "38","Z": "39",
    "Ñ": "40",
}

HOMO_CHARS = "123456789ABCDEFGHIJKLMNPQRSTUVWXYZ"

DV_WEIGHTS = [13,12,11,10,9,8,7,6,5,4,3,2]

# Tabla DV (Anexo III)
DV_TABLE = {}
for i, ch in enumerate("0123456789"):
    DV_TABLE[ch] = i
for val, ch in enumerate("ABCDEFGHIJKLMN", start=10):
    DV_TABLE[ch] = val
DV_TABLE["&"] = 24
for val, ch in enumerate("OPQRSTUVWXYZ", start=25):
    DV_TABLE[ch] = val
DV_TABLE[" "] = 37
DV_TABLE["Ñ"] = 38

# ============================================================
#  INPUT / HELPERS
# ============================================================

def pedir(msg, requerido=True):
    while True:
        v = input(msg).strip()
        if v or not requerido:
            return v
        print("❌ Este campo es obligatorio")

def fecha_ddmmaaaa_a_iso(fecha_user: str) -> str:
    """Usuario: DD-MM-AAAA  ->  Interno: AAAA-MM-DD"""
    try:
        d = datetime.strptime(fecha_user.strip(), "%d-%m-%Y")
        return d.strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError("Fecha inválida. Usa formato DD-MM-AAAA (ej: 09-02-1988)")

def _strip_accents_keep_enye(s: str) -> str:
    # Preserva Ñ antes de quitar acentos
    s = (s or "").replace("Ñ", "__ENYE__").replace("ñ", "__ENYE__")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("__ENYE__", "Ñ")
    return s

def _clean_non_letters(s: str) -> str:
    """Deja A-Z, 0-9, Ñ, &, espacio. Lo demás -> espacio."""
    out = []
    for ch in (s or ""):
        if ch in ("&", "Ñ"):
            out.append(ch)
        elif "A" <= ch <= "Z" or "0" <= ch <= "9":
            out.append(ch)
        else:
            out.append(" ")
    return "".join(out)

def _norm_name(s: str) -> str:
    s = (s or "").strip().upper()
    s = _strip_accents_keep_enye(s)
    s = s.replace("Ü", "U")
    s = s.replace(".", " ").replace("-", " ").replace("'", " ").replace("’", " ")
    s = _clean_non_letters(s)
    s = re.sub(r"\s+", " ", s).strip()

    parts = [p for p in s.split(" ") if p and p not in PARTICULAS]
    return " ".join(parts).strip()

def _choose_given_name(nombres: str) -> str:
    parts = [p for p in (nombres or "").split(" ") if p]
    if not parts:
        return "X"

    p0 = parts[0]

    # ✅ Caso crítico: inicial como primer nombre -> se usa la inicial (SAT suele hacer DISJ...)
    # Ej: "J LUZ" -> usa "J"
    if len(p0) == 1 and "A" <= p0 <= "Z":
        return p0

    # ✅ Si es nombre común (JOSE/MARIA/MA/J...) -> usa el siguiente que NO sea partícula
    if len(parts) >= 2 and p0 in NOMBRES_COMUNES:
        SKIP = {"DE", "DEL", "LA", "LAS", "LOS"}
        for p in parts[1:]:
            if p not in SKIP:
                return p
        # fallback si todos fueron partículas (raro)
        return parts[1]

    return p0

def _first_internal_vowel(word: str) -> str:
    for ch in (word or "")[1:]:
        if ch in "AEIOU":
            return ch
    return "X"

def _first_letter(word: str) -> str:
    return word[0] if word else "X"

# ============================================================
#  RFC BASE4+FECHA (10) INTERNAMENTE (NO IMPRIMIMOS)
# ============================================================

def _rfc_base10_interno(nombres: str, ap_paterno: str, ap_materno: str, fecha_yyyy_mm_dd: str) -> str:
    nombres = _norm_name(nombres)
    ap_paterno = _norm_name(ap_paterno)
    ap_materno = _norm_name(ap_materno)

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", (fecha_yyyy_mm_dd or "").strip())
    if not m:
        raise ValueError("fecha_yyyy_mm_dd debe venir como AAAA-MM-DD")

    yy = m.group(1)[2:]
    mm = m.group(2)
    dd = m.group(3)

    # Si no hay paterno pero sí materno: usar materno como paterno
    if not ap_paterno and ap_materno:
        ap_paterno, ap_materno = ap_materno, ""

    if not ap_paterno:
        raise ValueError("Falta apellido paterno (o al menos un apellido)")

    nombre_usable = _choose_given_name(nombres)

    p1 = _first_letter(ap_paterno)
    p2 = _first_internal_vowel(ap_paterno)
    p3 = _first_letter(ap_materno) if ap_materno else "X"
    p4 = _first_letter(nombre_usable)

    base4 = f"{p1}{p2}{p3}{p4}"
    base4 = ALTISONANTES_MAP.get(base4, base4)

    return f"{base4}{yy}{mm}{dd}"

# ============================================================
#  HOMOCLAVE (2)
# ============================================================

def _homoclave_2(nombres: str, ap_paterno: str, ap_materno: str) -> str:
    nombres = _norm_name(nombres)
    ap_paterno = _norm_name(ap_paterno)
    ap_materno = _norm_name(ap_materno)

    if not ap_paterno and ap_materno:
        ap_paterno, ap_materno = ap_materno, ""

    full = f"{ap_paterno} {ap_materno} {nombres}".strip()
    if not full:
        return "00"

    nums = "0"
    for ch in full:
        nums += HOMO_TABLE.get(ch, "00")

    total = 0
    for i in range(len(nums) - 1):
        pair = int(nums[i:i+2])
        nxt = int(nums[i+1])
        total += pair * nxt

    resid = total % 1000
    q = resid // 34
    r = resid % 34

    return HOMO_CHARS[q] + HOMO_CHARS[r]


# ============================================================
#  DIGITO VERIFICADOR (1)
# ============================================================

def _dv_value(ch: str) -> int:
    return DV_TABLE.get(ch, 0)

def _digito_verificador(rfc12: str) -> str:
    rfc12 = (rfc12 or "").strip().upper()
    if len(rfc12) != 12:
        raise ValueError("rfc12 debe tener 12 caracteres")

    s = 0
    for ch, w in zip(rfc12, DV_WEIGHTS):
        s += _dv_value(ch) * w

    resid = s % 11
    dv = 11 - resid

    if dv == 11:
        return "0"
    if dv == 10:
        return "A"
    return str(dv)

# ============================================================
#  RFC COMPLETO (13) - LO ÚNICO QUE ENTREGAMOS
# ============================================================

def rfc_pf_13(nombres: str, ap_paterno: str, ap_materno: str, fecha_yyyy_mm_dd: str) -> str:
    base10 = _rfc_base10_interno(nombres, ap_paterno, ap_materno, fecha_yyyy_mm_dd)
    homo2 = _homoclave_2(nombres, ap_paterno, ap_materno)
    rfc12 = base10 + homo2
    dv = _digito_verificador(rfc12)
    return rfc12 + dv

def rfc_pf_13_candidates(nombres: str, ap_paterno: str, ap_materno: str, fecha_yyyy_mm_dd: str) -> list[str]:
    candidatos = []
    vistos = set()

    def add(n, ap1, ap2):
        try:
            r = rfc_pf_13(n, ap1, ap2, fecha_yyyy_mm_dd)
            r = (r or "").strip().upper()
            if r and r not in vistos:
                vistos.add(r)
                candidatos.append(r)
        except Exception:
            pass

    n = (nombres or "").strip().upper()
    ap1 = (ap_paterno or "").strip().upper()
    ap2 = (ap_materno or "").strip().upper()

    add(n, ap1, ap2)

    parts = [p for p in _norm_name(n).split() if p]

    if parts:
        add(parts[0], ap1, ap2)

    if len(parts) >= 2 and parts[0] in NOMBRES_COMUNES:
        add(" ".join(parts[1:]), ap1, ap2)
        add(parts[1], ap1, ap2)

    n_sin_particulas = re.sub(r"\b(DE|DEL|LA|LAS|LOS|Y)\b", " ", n)
    n_sin_particulas = re.sub(r"\s+", " ", n_sin_particulas).strip()

    if n_sin_particulas and n_sin_particulas != n:
        add(n_sin_particulas, ap1, ap2)

    return candidatos

# ============================================================
#  CLI
# ============================================================

def main():
    print("=" * 60)
    print("CALCULADORA RFC PERSONA FÍSICA (PRO / INFORMATIVA)")
    print("=" * 60)
    print("Se imprime SOLO RFC completo (13). Fecha: DD-MM-AAAA")
    print("Nota: NO garantiza coincidencia con SAT.\n")

    nombres = pedir("Nombre(s): ")
    ap_paterno = pedir("Apellido paterno (ENTER si no tiene): ", requerido=False)
    ap_materno = pedir("Apellido materno (ENTER si no tiene): ", requerido=False)
    fecha_user = pedir("Fecha de nacimiento (DD-MM-AAAA): ")

    try:
        fecha_iso = fecha_ddmmaaaa_a_iso(fecha_user)
        rfc13 = rfc_pf_13(nombres, ap_paterno, ap_materno, fecha_iso)
    except Exception as e:
        print("\n❌ Error:", e)
        return

    print("\n" + "=" * 60)
    print("RFC COMPLETO (13):", rfc13)
    print("=" * 60)
    print("Uso informativo / académico.\n")

if __name__ == "__main__":
    main()
