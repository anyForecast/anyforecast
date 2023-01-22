CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']
CELERY_ROUTES = {
    'SkorchForecastingTrainer': {
        'exchange': 'trainer_worker',
        'exchange_type': 'direct',
        'routing_key': 'trainer_worker'
    }
}
