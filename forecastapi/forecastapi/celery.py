from celery import Celery

from . import celeryconfig

rabbit_path = 'amqp://rabbitmq:5672'
app = Celery('celery_app', broker=rabbit_path)
app.config_from_object(celeryconfig)
