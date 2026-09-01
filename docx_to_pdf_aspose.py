import os
import time
import hashlib
import threading
from pathlib import Path

import requests
import redis


ASPOSE_TOKEN_TIMEOUT = int(os.getenv("ASPOSE_TOKEN_TIMEOUT", "15"))
ASPOSE_CONVERT_TIMEOUT = int(os.getenv("ASPOSE_CONVERT_TIMEOUT", "60"))

_ASPOSE_TOKEN = None
_ASPOSE_TOKEN_EXPIRES_AT = 0.0
_ASPOSE_LOCAL_COOLDOWN_UNTIL = 0.0
_ASPOSE_LOCAL_LOCK = threading.Lock()

_REDIS_CLIENT = None
_REDIS_CLIENT_LOCK = threading.Lock()


def _aspose_base_url() -> str:
    return (
        os.getenv("ASPOSE_BASE_URL")
        or "https://api.aspose.cloud"
    ).rstrip("/")


def _get_redis():
    global _REDIS_CLIENT

    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT

    redis_url = (os.getenv("REDIS_URL") or "").strip()

    if not redis_url:
        return None

    with _REDIS_CLIENT_LOCK:
        if _REDIS_CLIENT is not None:
            return _REDIS_CLIENT

        try:
            r = redis.Redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
            r.ping()

            _REDIS_CLIENT = r

            print(
                "[ASPOSE REDIS READY]",
                flush=True,
            )

            return r

        except Exception as e:
            print(
                "[ASPOSE REDIS WARN]",
                repr(e),
                flush=True,
            )
            return None


def _aspose_key_suffix(client_id: str) -> str:
    raw = (
        _aspose_base_url()
        + "|"
        + client_id
    )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:16]


def _aspose_keys(client_id: str):
    suffix = _aspose_key_suffix(client_id)

    return {
        "token": f"aspose:oauth:token:v2:{suffix}",
        "cooldown": f"aspose:oauth:cooldown:v2:{suffix}",
        "lock": f"aspose:oauth:lock:v2:{suffix}",
    }


def _shared_cooldown_remaining(
    client_id: str,
) -> int:
    global _ASPOSE_LOCAL_COOLDOWN_UNTIL

    now = time.time()

    local_remaining = int(
        _ASPOSE_LOCAL_COOLDOWN_UNTIL - now
    )

    if local_remaining > 0:
        return local_remaining

    r = _get_redis()

    if r is None:
        return 0

    try:
        key = _aspose_keys(client_id)["cooldown"]
        ttl = int(r.ttl(key) or 0)

        if ttl > 0:
            _ASPOSE_LOCAL_COOLDOWN_UNTIL = (
                time.time() + ttl
            )
            return ttl

    except Exception as e:
        print(
            "[ASPOSE REDIS COOLDOWN WARN]",
            repr(e),
            flush=True,
        )

    return 0


def _read_shared_token(
    client_id: str,
):
    global _ASPOSE_TOKEN
    global _ASPOSE_TOKEN_EXPIRES_AT

    r = _get_redis()

    if r is None:
        return None

    try:
        key = _aspose_keys(client_id)["token"]

        token = r.get(key)

        if not token:
            return None

        ttl = int(r.ttl(key) or 0)

        if ttl <= 0:
            return None

        _ASPOSE_TOKEN = token
        _ASPOSE_TOKEN_EXPIRES_AT = (
            time.time() + ttl
        )

        return token

    except Exception as e:
        print(
            "[ASPOSE REDIS TOKEN WARN]",
            repr(e),
            flush=True,
        )

        return None


def _set_shared_cooldown(
    client_id: str,
    seconds: int,
):
    global _ASPOSE_LOCAL_COOLDOWN_UNTIL

    seconds = max(
        int(seconds),
        60,
    )

    _ASPOSE_LOCAL_COOLDOWN_UNTIL = (
        time.time() + seconds
    )

    r = _get_redis()

    if r is not None:
        try:
            key = _aspose_keys(client_id)["cooldown"]

            r.set(
                key,
                "1",
                ex=seconds,
            )

        except Exception as e:
            print(
                "[ASPOSE REDIS COOLDOWN SET WARN]",
                repr(e),
                flush=True,
            )


def _store_shared_token(
    client_id: str,
    token: str,
    expires_in: int,
):
    global _ASPOSE_TOKEN
    global _ASPOSE_TOKEN_EXPIRES_AT
    global _ASPOSE_LOCAL_COOLDOWN_UNTIL

    # Margen de seguridad antes de expiración real.
    ttl = max(
        int(expires_in) - 90,
        30,
    )

    _ASPOSE_TOKEN = token
    _ASPOSE_TOKEN_EXPIRES_AT = (
        time.time() + ttl
    )

    _ASPOSE_LOCAL_COOLDOWN_UNTIL = 0.0

    r = _get_redis()

    if r is not None:
        try:
            keys = _aspose_keys(client_id)

            r.set(
                keys["token"],
                token,
                ex=ttl,
            )

            r.delete(
                keys["cooldown"]
            )

        except Exception as e:
            print(
                "[ASPOSE REDIS TOKEN SET WARN]",
                repr(e),
                flush=True,
            )


def _invalidate_aspose_token(
    client_id: str,
):
    global _ASPOSE_TOKEN
    global _ASPOSE_TOKEN_EXPIRES_AT

    _ASPOSE_TOKEN = None
    _ASPOSE_TOKEN_EXPIRES_AT = 0.0

    r = _get_redis()

    if r is not None:
        try:
            key = _aspose_keys(client_id)["token"]
            r.delete(key)
        except Exception:
            pass


def _get_aspose_token(
    client_id: str,
    client_secret: str,
    force_refresh: bool = False,
) -> str:
    global _ASPOSE_TOKEN
    global _ASPOSE_TOKEN_EXPIRES_AT

    remaining = _shared_cooldown_remaining(
        client_id
    )

    if remaining > 0:
        raise RuntimeError(
            "Aspose token error 429: "
            f"SHARED_COOLDOWN retry_after={remaining}"
        )

    now = time.time()

    if (
        not force_refresh
        and _ASPOSE_TOKEN
        and now < (_ASPOSE_TOKEN_EXPIRES_AT - 10)
    ):
        return _ASPOSE_TOKEN

    if not force_refresh:
        shared = _read_shared_token(
            client_id
        )

        if shared:
            return shared

    with _ASPOSE_LOCAL_LOCK:
        remaining = _shared_cooldown_remaining(
            client_id
        )

        if remaining > 0:
            raise RuntimeError(
                "Aspose token error 429: "
                f"SHARED_COOLDOWN retry_after={remaining}"
            )

        if not force_refresh:
            shared = _read_shared_token(
                client_id
            )

            if shared:
                return shared

        r = _get_redis()
        keys = _aspose_keys(client_id)

        lock_owner = (
            f"{os.getpid()}:"
            f"{threading.get_ident()}:"
            f"{time.time_ns()}"
        )

        have_lock = False

        if r is not None:
            try:
                have_lock = bool(
                    r.set(
                        keys["lock"],
                        lock_owner,
                        nx=True,
                        ex=20,
                    )
                )
            except Exception:
                have_lock = False

        if r is not None and not have_lock:
            deadline = time.time() + 18

            while time.time() < deadline:
                remaining = (
                    _shared_cooldown_remaining(
                        client_id
                    )
                )

                if remaining > 0:
                    raise RuntimeError(
                        "Aspose token error 429: "
                        "SHARED_COOLDOWN "
                        f"retry_after={remaining}"
                    )

                shared = _read_shared_token(
                    client_id
                )

                if shared:
                    return shared

                time.sleep(0.25)

            try:
                have_lock = bool(
                    r.set(
                        keys["lock"],
                        lock_owner,
                        nx=True,
                        ex=20,
                    )
                )
            except Exception:
                have_lock = False

            if not have_lock:
                raise RuntimeError(
                    "ASPOSE_TOKEN_REFRESH_IN_PROGRESS"
                )

        try:
            # Última revisión antes de tocar Aspose.
            remaining = (
                _shared_cooldown_remaining(
                    client_id
                )
            )

            if remaining > 0:
                raise RuntimeError(
                    "Aspose token error 429: "
                    "SHARED_COOLDOWN "
                    f"retry_after={remaining}"
                )

            if not force_refresh:
                shared = _read_shared_token(
                    client_id
                )

                if shared:
                    return shared

            base = _aspose_base_url()
            token_url = f"{base}/connect/token"

            data = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }

            r_http = requests.post(
                token_url,
                data=data,
                timeout=ASPOSE_TOKEN_TIMEOUT,
            )

            if not r_http.ok:
                retry_raw = (
                    r_http.headers.get("Retry-After")
                    or r_http.headers.get("retry-after")
                    or ""
                )

                if r_http.status_code == 429:
                    try:
                        retry_after = int(
                            float(retry_raw or 600)
                        )
                    except Exception:
                        retry_after = 600

                    retry_after = max(
                        retry_after,
                        60,
                    )

                    _set_shared_cooldown(
                        client_id,
                        retry_after,
                    )

                    print(
                        "[ASPOSE TOKEN SHARED COOLDOWN]",
                        {
                            "retry_after": retry_after,
                        },
                        flush=True,
                    )

                    raise RuntimeError(
                        "Aspose token error 429: "
                        f"retry_after={retry_after}"
                    )

                raise RuntimeError(
                    f"Aspose token error "
                    f"{r_http.status_code}: "
                    f"{(r_http.text or '')[:500]}"
                )

            try:
                j = r_http.json()

            except Exception as e:
                raise RuntimeError(
                    "Aspose token non-json: "
                    f"{type(e).__name__}: "
                    f"{(r_http.text or '')[:500]}"
                )

            token = j.get("access_token")

            if not token:
                raise RuntimeError(
                    "Aspose token sin access_token: "
                    f"{str(j)[:500]}"
                )

            try:
                expires_in = int(
                    j.get("expires_in")
                    or 3600
                )
            except Exception:
                expires_in = 3600

            _store_shared_token(
                client_id,
                token,
                expires_in,
            )

            print(
                "[ASPOSE TOKEN REFRESHED SHARED]",
                {
                    "expires_in": expires_in,
                },
                flush=True,
            )

            return token

        finally:
            if r is not None and have_lock:
                try:
                    if r.get(keys["lock"]) == lock_owner:
                        r.delete(keys["lock"])
                except Exception:
                    pass


def _docx_to_pdf_aspose_rest(
    docx_path: str,
    pdf_path: str,
) -> str:

    client_id = (
        os.getenv("ASPOSE_CLIENT_ID")
        or ""
    ).strip()

    client_secret = (
        os.getenv("ASPOSE_CLIENT_SECRET")
        or ""
    ).strip()

    if not client_id or not client_secret:
        raise RuntimeError(
            "❌ Faltan variables "
            "ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET"
        )

    with open(docx_path, "rb") as f:
        doc_bytes = f.read()

    if len(doc_bytes) < 50_000:
        raise RuntimeError(
            f"ASPOSE_INPUT_DOCX_TOO_SMALL:"
            f"{len(doc_bytes)}"
        )

    base = _aspose_base_url()

    convert_url = (
        f"{base}/v4.0/words/convert?format=pdf"
    )

    for auth_attempt in range(2):

        token = _get_aspose_token(
            client_id,
            client_secret,
            force_refresh=(
                auth_attempt == 1
            ),
        )

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

        if (
            r.status_code == 401
            and auth_attempt == 0
        ):
            print(
                "[ASPOSE TOKEN 401 - INVALIDATE]",
                flush=True,
            )

            _invalidate_aspose_token(
                client_id
            )

            continue

        if not r.ok:
            raise RuntimeError(
                f"Aspose convert error "
                f"{r.status_code}: "
                f"{(r.text or '')[:800]}"
            )

        if len(r.content or b"") < 10_000:
            raise RuntimeError(
                f"ASPOSE_OUTPUT_PDF_TOO_SMALL:"
                f"{len(r.content or b'')}"
            )

        Path(pdf_path).parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        Path(pdf_path).write_bytes(
            r.content
        )

        return pdf_path

    raise RuntimeError(
        "ASPOSE_AUTH_FAILED_AFTER_REFRESH"
    )


def docx_to_pdf_aspose(
    docx_path: str,
    pdf_path: str,
) -> str:
    return _docx_to_pdf_aspose_rest(
        docx_path,
        pdf_path,
    )


def docx_to_pdf_aspose_web(
    docx_path: str,
    pdf_path: str,
) -> str:
    return _docx_to_pdf_aspose_rest(
        docx_path,
        pdf_path,
    )
