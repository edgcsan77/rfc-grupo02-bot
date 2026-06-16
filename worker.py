import os
from dotenv import load_dotenv
from redis import Redis
from rq import Worker

load_dotenv("/opt/rfc-grupo02-bot/.env")

REDIS_URL = os.getenv("REDIS_URL", "").strip()

if not REDIS_URL:
    raise RuntimeError("REDIS_URL no está definido en .env")

redis_conn = Redis.from_url(
    REDIS_URL,
    socket_connect_timeout=10,
    socket_timeout=None,
    health_check_interval=30,
    retry_on_timeout=True,
)

if __name__ == "__main__":
    print("[RFC_WORKER_ENTRYPOINT] listening on actas")
    worker = Worker(["actas"], connection=redis_conn)
    worker.work()
