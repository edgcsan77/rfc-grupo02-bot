import os
from redis import Redis
from rq import Queue

REDIS_URL = os.getenv("REDIS_URL", "").strip()

redis_conn = Redis.from_url(REDIS_URL)
task_queue = Queue("constancia_jobs", connection=redis_conn)
