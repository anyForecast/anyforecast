broker_url = 'amqp://rabbitmq:5672'

task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
include = ['celery_app.tasks']
