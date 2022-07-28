broker_url = 'amqp://rabbitmq:5672'

task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
include = ["api.celery_app.tasks"]
