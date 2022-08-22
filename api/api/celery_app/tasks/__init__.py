from .prediction import FromDataset
from .training import TrainingTask
from ..celery import app

training_task = TrainingTask().make_celery_task(app)
prediction_from_dataset_task = FromDataset().make_celery_task(app)

__all__ = [
    'training_task',
    'prediction_from_dataset_task'
]
