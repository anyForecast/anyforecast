from typing import List, Dict

from fastapi import Depends, APIRouter, Request

from ..auth import CredentialsProvider
from ..celery_app.tasks import prediction_from_dataset_task
from ..models import User, Predictor, Dataset, Pivot

router = APIRouter()


@router.post("/prediction/")
async def predict(
        predictor: Predictor,
        dataset: Dataset,
        group_ids: List[Dict],
        pivot: Pivot,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """Creates forecaster.

    By "forecaster" it is meant any time series estimator.
    """
    to_dict = (predictor, dataset, current_user)
    return prediction_from_dataset_task.run(
        *map(dict, to_dict), group_ids, pivot.pivot)


@router.post("/grafana/variable")
async def variable(
        request: Request,
        current_user: User = Depends(CredentialsProvider().load_user),
):
    print(f'Request from grafana to /variable: {await request.body()}')
    print(' ')
    return [
        {"__text": "Label 1", "__value": "Value1"},
        {"__text": "Label 2", "__value": "Value2"},
        {"__text": "Label 3", "__value": "Value3"}
    ]
