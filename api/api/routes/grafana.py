from typing import List, Dict, Optional

from celery import chain
from fastapi import Depends, APIRouter

from ..auth import CredentialsProvider
from ..celery_app.tasks import (
    load_dataset_task,
    predict_task,
    get_partitions_task,
    get_last_known_date_task,
    estimate_response_function_task
)
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
    return chain(
        load_dataset_task.s(**load_dataset_kwargs),
        predict_task.s(**predict_kwargs)
    ).apply().get()


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

    estimate_response_function_kwargs = {
        'predictor': predictor.dict(),
        'date_range': date_range.dict(),
        'input_col': input_col.dict()['string']
    }
    return chain(
        load_dataset_task.s(**load_dataset_kwargs),
        estimate_response_function_task.s(**estimate_response_function_kwargs)
    ).apply().get()


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
    print(partitions)
    return chain(
        load_dataset_task.s(**load_dataset_kwargs),
        get_last_known_date_task.s()
    ).apply().get()


@router.post("/get_parquet_partitions/")
async def get_parquet_partitions(
        dataset: Dataset,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """
    TODO: This should be an Athena query instead and NOT an api task.
    """
    return get_partitions_task.run(current_user.dict(), dataset.dict())
