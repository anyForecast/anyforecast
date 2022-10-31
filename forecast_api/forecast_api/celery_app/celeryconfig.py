broker_url = 'amqp://rabbitmq:5672'
result_backend = 'amqp://rabbitmq:5672'
task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = ['application/json', 'application/x-python-serialize']
include = ["forecast_api.celery_app.tasks"]
