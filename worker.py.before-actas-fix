from rq import Worker
from queue_app import redis_conn

if __name__ == "__main__":
    worker = Worker(["actas"], connection=redis_conn)
    worker.work()
