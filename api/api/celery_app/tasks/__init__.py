from ._dataloading import LoadDatasetTask
from ._minio import ShowParquetPartitions
from ._predict import GroupPredictionTask, ResponseFunctionEstimationTask
from ._train import TrainTask
from ..celery import app

load_dataset_task = LoadDatasetTask().make_celery_task(app)
train_task = TrainTask(bind=True).make_celery_task(app)
predict_task = GroupPredictionTask().make_celery_task(app)
response_function_estimation_task = ResponseFunctionEstimationTask() \
    .make_celery_task(app)
show_partitions_task = ShowParquetPartitions().make_celery_task(app)

__all__ = [
    'LoadDatasetTask',
    'TrainTask',
    'GroupPredictionTask',
    'ResponseFunctionEstimationTask',
    'load_dataset_task',
    'train_task',
    'predict_task',
    'response_function_estimation_task',
    'show_partitions_task',
]
