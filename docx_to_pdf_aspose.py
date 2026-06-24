import os
from pathlib import Path
import requests

ASPOSE_TOKEN_TIMEOUT = int(os.getenv("ASPOSE_TOKEN_TIMEOUT", "15"))
ASPOSE_CONVERT_TIMEOUT = int(os.getenv("ASPOSE_CONVERT_TIMEOUT", "60"))

def _aspose_base_url() -> str:
    """
    Puedes setear ASPOSE_BASE_URL:
    - https://api.aspose.cloud
    - https://api.eu.aspose.cloud
    """
    return (os.getenv("ASPOSE_BASE_URL") or "https://api.aspose.cloud").rstrip("/")

def _get_aspose_token(client_id: str, client_secret: str) -> str:
    base = _aspose_base_url()
    token_url = f"{base}/connect/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    r = requests.post(token_url, data=data, timeout=ASPOSE_TOKEN_TIMEOUT)

    if not r.ok:
        raise RuntimeError(f"Aspose token error {r.status_code}: {r.text[:500]}")

    try:
        j = r.json()
    except Exception as e:
        raise RuntimeError(f"Aspose token non-json: {type(e).__name__}: {(r.text or '')[:500]}")

    token = j.get("access_token")
    if not token:
        raise RuntimeError(f"Aspose token sin access_token: {str(j)[:500]}")
    return token

def _docx_to_pdf_aspose_rest(docx_path: str, pdf_path: str) -> str:
    client_id = (os.getenv("ASPOSE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("ASPOSE_CLIENT_SECRET") or "").strip()

    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    token = _get_aspose_token(client_id, client_secret)

    base = _aspose_base_url()
    convert_url = f"{base}/v4.0/words/convert?format=pdf"

    with open(docx_path, "rb") as f:
        doc_bytes = f.read()

    if len(doc_bytes) < 50_000:
        raise RuntimeError(f"ASPOSE_INPUT_DOCX_TOO_SMALL:{len(doc_bytes)}")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
    }

    r = requests.put(
        convert_url,
        headers=headers,
        data=doc_bytes,
        timeout=ASPOSE_CONVERT_TIMEOUT,
    )

    if not r.ok:
        raise RuntimeError(f"Aspose convert error {r.status_code}: {(r.text or '')[:800]}")

    if len(r.content or b"") < 10_000:
        raise RuntimeError(f"ASPOSE_OUTPUT_PDF_TOO_SMALL:{len(r.content or b'')}")

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(r.content)
    return pdf_path

def docx_to_pdf_aspose(docx_path: str, pdf_path: str) -> str:
    """
    WA: ahora también usa REST directo para controlar timeout.
    """
    return _docx_to_pdf_aspose_rest(docx_path, pdf_path)

def docx_to_pdf_aspose_web(docx_path: str, pdf_path: str) -> str:
    """
    WEB: usa el mismo conversor REST directo.
    """
    return _docx_to_pdf_aspose_rest(docx_path, pdf_path)
