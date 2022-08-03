from .prediction_task import PredictionTask
from .training_task import TrainingTask
from ..celery import app

training_task = TrainingTask().make_celery_task(app)
prediction_task = PredictionTask().make_celery_task(app)

__all__ = [
    'training_task',
    'prediction_task'
]
