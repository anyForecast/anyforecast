from kombu import Queue, Exchange

accept_content = ['pickle', 'json', 'msgpack', 'yaml']
task_default_queue = 'default'
task_serializer = 'pickle'
result_serializer = 'pickle'
task_queues = (
    Queue('training', Exchange('training'), routing_key='training'),
    Queue('dataloading', Exchange('dataloading'), routing_key='dataloading')
)
task_routes = {
    'TrainSkorchForecasting': {
        'exchange_type': 'direct',
        'queue': 'training'
    },
    'LoadPandas': {
        'exchange_type': 'direct',
        'queue': 'dataloading'
    }
}
