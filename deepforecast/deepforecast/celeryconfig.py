import os

broker_url = os.environ.get('BROKER_URL')
result_backend = os.environ.get('CELERY_RESULT_BACKEND')
task_track_started = True
task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = ['pickle', 'json', 'msgpack', 'yaml']
include = ["anyforecast.tasks"]
