import os
from redis import Redis
from rq import Worker

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

redis_conn = Redis.from_url(
    REDIS_URL,
    socket_connect_timeout=10,
    socket_timeout=600,
    health_check_interval=30,
    retry_on_timeout=True,
)

if __name__ == "__main__":
    worker = Worker(["broadcast"], connection=redis_conn)
    print("[RFC_BROADCAST_WORKER_ENTRYPOINT] listening on broadcast", flush=True)
    worker.work()
