# cache_store.py
import json
import os
import time
import tempfile
import threading

# Ruta correcta (Render suele usar /app/data)
CACHE_FILE = (os.getenv("CACHE_FILE", "") or "/app/data/cache_checkid.json").strip()
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", str(30 * 24 * 3600)))  # 30 días

_THREAD_LOCK = threading.Lock()

def _ensure_dir():
    os.makedirs(os.path.dirname(CACHE_FILE) or ".", exist_ok=True)

def _load_cache_nolock():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
            return obj if isinstance(obj, dict) else {}
    except Exception as e:
        # Mueve corrupto a .bad.<ts>
        try:
            bad = f"{CACHE_FILE}.bad.{int(time.time())}"
            os.replace(CACHE_FILE, bad)
            print(f"CACHE CORRUPT -> moved to {bad}. Reason: {repr(e)}")
        except Exception:
            print("CACHE LOAD ERROR:", repr(e))
        return {}

def _atomic_write_json(path: str, obj: dict):
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)

    fd, tmp = tempfile.mkstemp(prefix=".tmp_cache_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic swap
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _with_process_lock(fn, *args, **kwargs):
    """
    Lock entre procesos usando flock (solo Linux).
    Si flock falla por cualquier razón, igual ejecuta con thread lock.
    """
    try:
        import fcntl
        _ensure_dir()
        lock_path = CACHE_FILE + ".lock"
        with open(lock_path, "w") as lockf:
            fcntl.flock(lockf, fcntl.LOCK_EX)
            try:
                return fn(*args, **kwargs)
            finally:
                fcntl.flock(lockf, fcntl.LOCK_UN)
    except Exception:
        # fallback
        return fn(*args, **kwargs)

def cache_get(key: str):
    key = (key or "").strip()
    if not key:
        return None

    now = int(time.time())

    def _do():
        with _THREAD_LOCK:
            cache = _load_cache_nolock()
            item = cache.get(key)
            if not item or not isinstance(item, dict):
                return None

            # 1) TTL por item (si existe exp)
            exp = item.get("exp")
            if exp is not None:
                try:
                    if now > int(exp):
                        cache.pop(key, None)
                        _atomic_write_json(CACHE_FILE, cache)
                        return None
                except Exception:
                    # si exp está corrupto, ignóralo y usa fallback
                    pass

            # 2) Fallback: TTL global (compat con entradas viejas sin exp)
            ts = int(item.get("ts") or 0)
            if (not exp) and (CACHE_TTL is not None):
                try:
                    if now - ts > int(CACHE_TTL):
                        cache.pop(key, None)
                        _atomic_write_json(CACHE_FILE, cache)
                        return None
                except Exception:
                    pass

            return item.get("data")

    return _with_process_lock(_do)

def cache_set(key: str, data: dict, ttl: int = None, ttl_seconds: int = None):
    """
    Guarda en CACHE_FILE con expiración opcional.
    Compat:
      - cache_set(k, data)
      - cache_set(k, data, ttl=60)
      - cache_set(k, data, ttl_seconds=60)
    """
    key = (key or "").strip()
    if not key:
        return

    if ttl is None and ttl_seconds is not None:
        ttl = ttl_seconds

    exp = None
    if ttl is not None:
        try:
            ttl_i = int(ttl)
            if ttl_i > 0:
                exp = int(time.time()) + ttl_i
        except Exception:
            exp = None

    def _do():
        with _THREAD_LOCK:
            cache = _load_cache_nolock()
            cache[key] = {"ts": int(time.time()), "exp": exp, "data": data}
            _atomic_write_json(CACHE_FILE, cache)

    _with_process_lock(_do)

def cache_del(key: str):
    key = (key or "").strip()
    if not key:
        return

    def _do():
        with _THREAD_LOCK:
            cache = _load_cache_nolock()
            if key in cache:
                cache.pop(key, None)
                _atomic_write_json(CACHE_FILE, cache)

    _with_process_lock(_do)
