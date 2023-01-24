import os

broker_url = os.environ.get('BROKER_URL')
result_backend = os.environ.get('CELERY_RESULT_BACKEND')
task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = ['application/json', 'application/x-python-serialize']
include = ["celery_app.tasks"]
