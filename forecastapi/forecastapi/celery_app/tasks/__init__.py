from ._dataloading import LoadDatasetTask
from ._minio import GetParquetPartitions, GetLastKnownDate
from ._predict import GroupPredictionTask, ResponseFunctionEstimationTask
from ._train import TrainTask
from ..celery import app

load_dataset_task = LoadDatasetTask().make_celery_task(app)
train_task = TrainTask(bind=True).make_celery_task(app)
predict_task = GroupPredictionTask().make_celery_task(app)
estimate_response_function_task = ResponseFunctionEstimationTask() \
    .make_celery_task(app)
get_partitions_task = GetParquetPartitions().make_celery_task(app)
get_last_known_date_task = GetLastKnownDate().make_celery_task(app)

__all__ = [
    'LoadDatasetTask',
    'TrainTask',
    'GroupPredictionTask',
    'ResponseFunctionEstimationTask',
    'load_dataset_task',
    'train_task',
    'predict_task',
    'estimate_response_function_task',
    'get_last_known_date_task',
    'get_partitions_task',
]
