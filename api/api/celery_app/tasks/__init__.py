from ._minio import ShowParquetPartitions
from ._predict import GroupPrediction
from ._train import TrainingTask
from ..celery import app

train_task = TrainingTask().make_celery_task(app)
predict_task = GroupPrediction().make_celery_task(app)
show_partitions_task = ShowParquetPartitions().make_celery_task(app)

__all__ = [
    'train_task',
    'predict_task',
    'show_partitions_task'
]
