import re
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests


FGR_CURP_URL = (
    "https://registrate.fgr.org.mx"
    "/Applicant/CheckCurp"
)

CURP_PATTERN = re.compile(
    r"^[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d$"
)

ENTIDADES_CURP = {
    "AS": "AGUASCALIENTES",
    "BC": "BAJA CALIFORNIA",
    "BS": "BAJA CALIFORNIA SUR",
    "CC": "CAMPECHE",
    "CL": "COAHUILA DE ZARAGOZA",
    "CM": "COLIMA",
    "CS": "CHIAPAS",
    "CH": "CHIHUAHUA",
    "DF": "CIUDAD DE MEXICO",
    "DG": "DURANGO",
    "GT": "GUANAJUATO",
    "GR": "GUERRERO",
    "HG": "HIDALGO",
    "JC": "JALISCO",
    "MC": "MEXICO",
    "MN": "MICHOACAN DE OCAMPO",
    "MS": "MORELOS",
    "NT": "NAYARIT",
    "NL": "NUEVO LEON",
    "OC": "OAXACA",
    "PL": "PUEBLA",
    "QT": "QUERETARO",
    "QR": "QUINTANA ROO",
    "SP": "SAN LUIS POTOSI",
    "SL": "SINALOA",
    "SR": "SONORA",
    "TC": "TABASCO",
    "TS": "TAMAULIPAS",
    "TL": "TLAXCALA",
    "VZ": "VERACRUZ DE IGNACIO DE LA LLAVE",
    "YN": "YUCATAN",
    "ZS": "ZACATECAS",
    "NE": "NACIDO EN EL EXTRANJERO",
}


def _norm(value) -> str:
    return str(
        value or ""
    ).strip().upper()


class _FgrInputParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(
            convert_charrefs=True
        )
        self.values = {}

    def handle_starttag(
        self,
        tag,
        attrs,
    ) -> None:
        if str(tag).lower() != "input":
            return

        values = {
            str(key): (
                ""
                if value is None
                else str(value)
            )
            for key, value in attrs
        }

        key = (
            values.get("id")
            or values.get("name")
            or ""
        ).strip()

        if not key:
            return

        self.values[key] = (
            values.get("value")
            or ""
        )


def _parse_inputs(
    html: str,
) -> dict:
    parser = _FgrInputParser()
    parser.feed(
        str(html or "")
    )
    return parser.values


def _validate_birthdate(
    value: str,
) -> None:
    value = str(
        value or ""
    ).strip()

    for fmt in (
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
    ):
        try:
            datetime.strptime(
                value,
                fmt,
            )
            return
        except ValueError:
            pass

    raise RuntimeError(
        "FGR_CURP_BIRTHDATE_INVALID:"
        f"{value}"
    )


def consultar_curp_fgr(
    curp: str,
    timeout_s: int = 20,
) -> dict:
    """
    Fallback FGR/RENAPO para obtener identidad
    desde una CURP.

    IMPORTANTE:
    FGR devuelve un campo Rfc de 10 caracteres.
    NO se usa como RFC final. El RFC de 13
    caracteres sigue calculándose con Moffin.
    """

    curp = _norm(curp)

    if not CURP_PATTERN.fullmatch(
        curp
    ):
        raise RuntimeError(
            "CURP_INVALIDA"
        )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,"
            "application/xhtml+xml,"
            "application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language":
            "es-MX,es;q=0.9,en;q=0.8",
    }

    masked = (
        curp[:4]
        + "..."
        + curp[-4:]
    )

    with requests.Session() as session:
        try:
            landing = session.get(
                FGR_CURP_URL,
                headers=headers,
                timeout=timeout_s,
            )
        except requests.Timeout as exc:
            raise RuntimeError(
                "FGR_CURP_GET_TIMEOUT"
            ) from exc
        except requests.RequestException as exc:
            raise RuntimeError(
                "FGR_CURP_GET_REQUEST_ERROR:"
                f"{type(exc).__name__}"
            ) from exc

        if landing.status_code != 200:
            raise RuntimeError(
                "FGR_CURP_GET_HTTP_ERROR:"
                f"{landing.status_code}"
            )

        landing_fields = _parse_inputs(
            landing.text
        )

        token = str(
            landing_fields.get(
                "__RequestVerificationToken"
            )
            or ""
        ).strip()

        if not token:
            raise RuntimeError(
                "FGR_CURP_TOKEN_NOT_FOUND"
            )

        post_headers = dict(
            headers
        )
        post_headers.update({
            "Referer": FGR_CURP_URL,
            "Content-Type":
                "application/x-www-form-urlencoded",
        })

        try:
            response = session.post(
                FGR_CURP_URL,
                data={
                    "Curp": curp,
                    "__RequestVerificationToken":
                        token,
                },
                headers=post_headers,
                timeout=timeout_s,
                allow_redirects=False,
            )
        except requests.Timeout as exc:
            raise RuntimeError(
                "FGR_CURP_POST_TIMEOUT"
            ) from exc
        except requests.RequestException as exc:
            raise RuntimeError(
                "FGR_CURP_POST_REQUEST_ERROR:"
                f"{type(exc).__name__}"
            ) from exc

        if response.status_code in (
            301,
            302,
            303,
            307,
            308,
        ):
            location = str(
                response.headers.get(
                    "Location"
                )
                or ""
            ).strip()

            if not location:
                raise RuntimeError(
                    "FGR_CURP_REDIRECT_"
                    "WITHOUT_LOCATION"
                )

            result_url = urljoin(
                FGR_CURP_URL,
                location,
            )

            result_headers = dict(
                headers
            )
            result_headers[
                "Referer"
            ] = FGR_CURP_URL

            try:
                result = session.get(
                    result_url,
                    headers=result_headers,
                    timeout=timeout_s,
                )
            except requests.Timeout as exc:
                raise RuntimeError(
                    "FGR_CURP_RESULT_TIMEOUT"
                ) from exc
            except requests.RequestException as exc:
                raise RuntimeError(
                    "FGR_CURP_RESULT_"
                    "REQUEST_ERROR:"
                    f"{type(exc).__name__}"
                ) from exc

        elif response.status_code == 200:
            result = response

        else:
            raise RuntimeError(
                "FGR_CURP_POST_HTTP_ERROR:"
                f"{response.status_code}"
            )

        if result.status_code != 200:
            raise RuntimeError(
                "FGR_CURP_RESULT_HTTP_ERROR:"
                f"{result.status_code}"
            )

        fields = _parse_inputs(
            result.text
        )

    renapo_success = _norm(
        fields.get(
            "RenapoSuccess"
        )
    )

    curp_not_found = _norm(
        fields.get(
            "CurpNotFound"
        )
    )

    if curp_not_found == "TRUE":
        raise RuntimeError(
            "FGR_CURP_NOT_FOUND"
        )

    if renapo_success != "TRUE":
        raise RuntimeError(
            "FGR_RENAPO_NOT_SUCCESS:"
            f"{renapo_success or 'EMPTY'}"
        )

    returned_curp = _norm(
        fields.get("Curp")
    )

    same_curp = (
        returned_curp == curp
    )

    corrected_last_two = (
        len(returned_curp) == 18
        and len(curp) == 18
        and returned_curp[:16]
        == curp[:16]
    )

    if not (
        same_curp
        or corrected_last_two
    ):
        raise RuntimeError(
            "FGR_CURP_MISMATCH:"
            f"requested={curp}:"
            f"returned={returned_curp}"
        )

    nombre = _norm(
        fields.get("Names")
    )

    apellido_paterno = _norm(
        fields.get("LastName")
    )

    apellido_materno = _norm(
        fields.get(
            "SecondLastName"
        )
    )

    fecha = str(
        fields.get("BirthDay")
        or ""
    ).strip()

    if (
        not nombre
        or not (
            apellido_paterno
            or apellido_materno
        )
        or not fecha
    ):
        raise RuntimeError(
            "FGR_CURP_DATA_INCOMPLETE"
        )

    if (
        not apellido_paterno
        and apellido_materno
    ):
        apellido_paterno = (
            apellido_materno
        )
        apellido_materno = ""

    _validate_birthdate(
        fecha
    )

    entidad_clave = (
        returned_curp[11:13]
        if len(returned_curp) == 18
        else ""
    )

    entidad = ENTIDADES_CURP.get(
        entidad_clave,
        "",
    )

    print(
        "[FGR_CURP_OK]",
        {
            "curp": masked,
            "entidad_clave":
                entidad_clave,
        },
        flush=True,
    )

    return {
        "CURP": returned_curp,
        "NOMBRE": nombre,
        "PRIMER_APELLIDO":
            apellido_paterno,
        "SEGUNDO_APELLIDO":
            apellido_materno,
        "FECHA_NACIMIENTO":
            fecha,
        "ENTIDAD_CLAVE":
            entidad_clave,
        "ENTIDAD_REGISTRO":
            entidad,
        "ENTIDAD":
            entidad,
        "SEXO": (
            returned_curp[10]
            if len(returned_curp) == 18
            else ""
        ),
        # Solo referencia.
        # NO utilizar como RFC final.
        "RFC_BASE_FGR": _norm(
            fields.get("Rfc")
        ),
        "_CURP_SOURCE": "FGR",
        "_ORIGEN": "FGR_RENAPO",
    }
