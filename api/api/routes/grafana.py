from typing import List

from fastapi import Depends, APIRouter

from ..auth import TokenProvider
from ..celery_app.tasks import prediction_from_dataset_task
from ..models import User, Predictor, Dataset, Pivot, GroupIdPrediction, \
    DateRange

router = APIRouter()


@router.post("/prediction/")
async def predict(
        date_range: DateRange,
        predictor: Predictor,
        dataset: Dataset,
        prediction_params: List[GroupIdPrediction],
        pivot: Pivot,
        current_user: User = Depends(TokenProvider().load_user)
):
    """Creates forecaster.

    By "forecaster" it is meant any time series estimator.
    """
    to_dict = predictor, dataset, current_user, date_range
    to_dict = map(dict, to_dict)
    return prediction_from_dataset_task.run(to_dict)
