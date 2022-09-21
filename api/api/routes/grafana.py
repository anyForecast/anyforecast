from typing import List

from fastapi import Depends, APIRouter

from ..auth import CredentialsProvider, TokenProvider
from ..celery_app.tasks import (
    predict_task,
    show_partitions_task
)
from ..models.ml import (
    GroupParams,
    PredictionDateRange,
    Predictor,
    Pivot,
    ReturnTruth,
    Dataset
)
from ..models.users import User

router = APIRouter()


@router.post("/predict/")
async def predict(
        date_range: PredictionDateRange,
        predictor: Predictor,
        dataset: Dataset,
        group_params: List[GroupParams],
        pivot: Pivot,
        return_truth: ReturnTruth,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """Creates forecaster.

    By "forecaster" it is meant any time series estimator.
    """
    return predict_task.run(
        predictor=predictor.dict(),
        dataset=dataset.dict(),
        user=current_user.dict(),
        date_range=date_range.dict(),
        group_params=[param.dict() for param in group_params],
        pivot=pivot.bool,
        return_truth=return_truth.bool
    )


@router.post("/show_parquet_partitions/")
async def show_parquet_partitions(
        dataset: Dataset,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """Shows parquet partitions for a single dataset.
    """
    return show_partitions_task.run(current_user, dataset)
