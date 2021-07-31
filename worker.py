from celery import Celery

from pipeline import Pipeline
from jobs import ValidateJob, SaveJob

CELERY_BROKER_URL = "amqp://test:test@localhost/test"
CELERY_BACKEND_URL = "db+sqlite:///test.db"

app = Celery("worker-cryptocoin", backend=CELERY_BACKEND_URL, broker=CELERY_BROKER_URL)


@app.task()
def run(data):
    p = Pipeline()
    p.add(ValidateJob()).add(SaveJob())

    p.run(data)
