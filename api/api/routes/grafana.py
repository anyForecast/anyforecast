from typing import List, Dict, Optional

from celery import chain
from fastapi import Depends, APIRouter

from ..auth import CredentialsProvider
from ..celery_app.tasks import (
    load_dataset_task,
    predict_task,
    show_partitions_task,
)
from ..models.ml import (
    WhatIf,
    PredictionDateRange,
    Predictor,
    Dataset
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

    if what_ifs is not None:
        what_ifs = [w.dict() for w in what_ifs]

    predict_kwargs = {
        'predictor': predictor.dict(),
        'date_range': date_range.dict(),
        'what_ifs': what_ifs
    }
    return chain(
        load_dataset_task.s(**load_dataset_kwargs),
        predict_task.s(**predict_kwargs)
    ).apply().result


@router.post("/show_parquet_partitions/")
async def show_parquet_partitions(
        dataset: Dataset,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """Shows parquet partitions for a single dataset.
    """
    return show_partitions_task.run(current_user, dataset)
