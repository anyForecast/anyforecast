from typing import List, Dict, Optional

from fastapi import Depends, APIRouter

from ..auth import CredentialsProvider
from ..celery_app.tasks import task_registry, TaskChainer
from ..models.ml import (
    WhatIf,
    PredictionDateRange,
    Predictor,
    Dataset,
    InputCol,
    MergeTruth
)
from ..models.users import User

router = APIRouter()


@router.post("/predict/")
async def predict(
        date_range: PredictionDateRange,
        predictor: Predictor,
        dataset: Dataset,
        partitions: List[Dict],
        what_ifs: Optional[List[WhatIf]] = None,
        merge_truth: Optional[MergeTruth] = None,
        merge_kwargs: Optional[Dict] = None,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """Creates forecaster.

    By "forecaster" it is meant any time series estimator.
    """
    load_dataset_kwargs = {
        'dataset': dataset.dict(),
        'user': current_user.dict(),
        'partitions': partitions,
        'format': 'pandas',
        'return_schema': True,
        'enforce_schema_dtypes': True
    }

    # Preprocess optional parameters.
    if what_ifs is not None:
        what_ifs = [w.dict() for w in what_ifs]
    if merge_truth is not None:
        merge_truth = merge_truth.bool
    if merge_kwargs is None:
        merge_kwargs = {}

    predict_kwargs = {
        'predictor': predictor.dict(),
        'date_range': date_range.dict(),
        'what_ifs': what_ifs,
        'merge_truth': merge_truth,
        **merge_kwargs
    }

    # Get tasks.
    load_dataset_task = task_registry.get_task('LoadDatasetTask')
    group_prediction_task = task_registry.get_task('GroupPredictionTask')

    # Make chain.
    chainer = TaskChainer()
    chainer.add_task(load_dataset_task, **load_dataset_kwargs)
    chainer.add_task(group_prediction_task, **predict_kwargs)
    chain = chainer.make_chain()

    task_result = chain.apply()
    return task_result.get()


@router.post("/estimate_response_function/")
async def estimate_response_function(
        date_range: PredictionDateRange,
        predictor: Predictor,
        dataset: Dataset,
        partitions: List[Dict],
        input_col: InputCol,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """
    """
    load_dataset_kwargs = {
        'dataset': dataset.dict(),
        'user': current_user.dict(),
        'partitions': partitions,
        'format': 'pandas',
        'return_schema': True,
        'enforce_schema_dtypes': True
    }

    response_function_estimation_kwargs = {
        'predictor': predictor.dict(),
        'date_range': date_range.dict(),
        'input_col': input_col.dict()['string']
    }

    # Get tasks.
    load_dataset_task = task_registry.get_task('LoadDatasetTask')
    response_function_estimation_task = task_registry.get_task(
        'ResponseFunctionEstimationTask')

    # Make chain.
    chainer = TaskChainer()
    chainer.add_task(load_dataset_task, **load_dataset_kwargs)
    chainer.add_task(
        response_function_estimation_task,
        **response_function_estimation_kwargs
    )
    chain = chainer.make_chain()

    task_result = chain.apply()
    return task_result.get()


@router.post("/get_last_known_date/")
async def get_last_known_date(
        dataset: Dataset,
        partitions: List[Dict],
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """
    TODO: This should be an Athena query instead and NOT an api task.
    """
    load_dataset_kwargs = {
        'dataset': dataset.dict(),
        'user': current_user.dict(),
        'partitions': partitions,
        'format': 'pandas',
        'return_schema': True,
        'enforce_schema_dtypes': True
    }

    # Get tasks.
    load_dataset_task = task_registry.get_task('LoadDatasetTask')
    get_last_known_date_task = task_registry.get_task('GetLastKnownDate')

    # Make chain.
    chainer = TaskChainer()
    chainer.add_task(load_dataset_task, **load_dataset_kwargs)
    chainer.add_task(get_last_known_date_task)
    chain = chainer.make_chain()

    task_result = chain.apply()
    return task_result.get()


@router.post("/get_parquet_partitions/")
async def get_parquet_partitions(
        dataset: Dataset,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """
    TODO: This should be an Athena query instead and NOT an api task.
    """
    get_partitions_task = task_registry.get_task('GetParquetPartitions')
    return get_partitions_task.run(current_user.dict(), dataset.dict())
