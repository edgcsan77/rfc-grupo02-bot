import os
from dotenv import load_dotenv
from redis import Redis
from rq import Queue

load_dotenv("/opt/rfc-grupo02-bot/.env")

REDIS_URL = os.getenv("REDIS_URL", "").strip()

if not REDIS_URL:
    raise RuntimeError("REDIS_URL no está definido. Revisa /opt/rfc-grupo02-bot/.env")

redis_conn = Redis.from_url(
    REDIS_URL,
    socket_connect_timeout=10,
    socket_timeout=None,
    retry_on_timeout=True,
    health_check_interval=30,
)

task_queue = Queue(
    "actas",
    connection=redis_conn,
    default_timeout=900,
)
