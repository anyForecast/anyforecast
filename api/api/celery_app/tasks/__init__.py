from ._dataloading import LoadDatasetTask
from ._minio import ShowParquetPartitions
from ._predict import GroupPredictionTask
from ._train import TrainTask
from ..celery import app

load_dataset_task = LoadDatasetTask().make_celery_task(app)
train_task = TrainTask().make_celery_task(app)
predict_task = GroupPredictionTask().make_celery_task(app)
show_partitions_task = ShowParquetPartitions().make_celery_task(app)

__all__ = [
    'LoadDatasetTask',
    'TrainTask',
    'GroupPredictionTask',
    'load_dataset_task',
    'train_task',
    'predict_task',
    'show_partitions_task',
]
