broker_url = 'amqp://rabbitmq:5672'
task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = ['application/json', 'application/x-python-serialize']
include = ["celery_app.tasks"]
