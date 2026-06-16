import os
from pathlib import Path
import requests

# =========================
# (WA) NO TOCAR
# =========================
from asposewordscloud import WordsApi
from asposewordscloud.models.requests import ConvertDocumentRequest

def docx_to_pdf_aspose(docx_path: str, pdf_path: str) -> str:
    """
    (WA) Convierte DOCX a PDF usando Aspose Words Cloud.
    Déjalo como lo tienes si WA ya funciona.
    """
    client_id = os.getenv("ASPOSE_CLIENT_ID")
    client_secret = os.getenv("ASPOSE_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    # OJO: en algunas versiones funciona con kwargs, en otras no.
    # Si en WA te funciona, NO lo cambies.
    words_api = WordsApi(client_id=client_id, client_secret=client_secret)

    with open(docx_path, "rb") as f:
        request = ConvertDocumentRequest(document=f, format="pdf")
        pdf_bytes = words_api.convert_document(request)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path


# =========================
# (WEB) REST DIRECTO (sin SDK)
# =========================
def _aspose_base_url() -> str:
    """
    Puedes setear ASPOSE_BASE_URL:
    - https://api.aspose.cloud
    - https://api.eu.aspose.cloud   (si tu cuenta es EU)
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

    r = requests.post(token_url, data=data, timeout=30)
    # si falla, imprime el body para ver si es credencial/region
    if not r.ok:
        raise RuntimeError(f"Aspose token error {r.status_code}: {r.text[:500]}")
    j = r.json()
    token = j.get("access_token")
    if not token:
        raise RuntimeError(f"Aspose token sin access_token: {str(j)[:500]}")
    return token

def docx_to_pdf_aspose_web(docx_path: str, pdf_path: str) -> str:
    """
    (WEB) Convierte DOCX->PDF usando REST directo para evitar:
    - KeyError('RequestId')
    - cambios de firma WordsApi entre versiones
    """
    client_id = (os.getenv("ASPOSE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("ASPOSE_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    token = _get_aspose_token(client_id, client_secret)

    base = _aspose_base_url()
    convert_url = f"{base}/v4.0/words/convert?format=pdf"

    with open(docx_path, "rb") as f:
        doc_bytes = f.read()

    headers = {
        "Authorization": f"Bearer {token}",
        # puedes enviar octet-stream sin problema
        "Content-Type": "application/octet-stream",
    }

    r = requests.put(convert_url, headers=headers, data=doc_bytes, timeout=120)

    # Si 403/404 aquí, YA es cuenta/region/credenciales/ruta
    if not r.ok:
        raise RuntimeError(f"Aspose convert error {r.status_code}: {r.text[:800]}")

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(r.content)
    return pdf_path
