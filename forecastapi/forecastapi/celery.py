from celery import Celery
import os

from . import celeryconfig

broker = os.environ.get('BROKER_URL')
backend = os.environ.get('CELERY_RESULT_BACKEND')

app = Celery('celeryapp', broker=broker, backend=backend)
app.config_from_object(celeryconfig)
