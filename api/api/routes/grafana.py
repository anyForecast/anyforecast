from fastapi import Depends, APIRouter, Request

from ..auth import CredentialsProvider
from ..celery_app.tasks import prediction_task
from ..models import User, Dataset, Predictor

router = APIRouter()


@router.get("/grafana/")
async def test_connection(
        current_user: User = Depends(CredentialsProvider().load_user),
):
    """Test connection.

    Used for "Test connection" on the datasource config page.
    """
    pass


@router.post("/predict")
async def predict(
        predictor: Predictor,
        dataset: Dataset,
        current_user: User = Depends(CredentialsProvider().load_user)
):
    """Creates forecaster.

    By "forecaster" it is meant any time series estimator.
    """
    return prediction_task.run(
        *map(dict, (predictor, dataset, current_user))
    )


@router.post("/grafana/search")
async def search(
        request: Request,
        current_user: User = Depends(CredentialsProvider().load_user),
):
    """List available metrics.

    Used by the find metric options on the query tab in panels.
    """
    print(f'Request from grafana to /search: {await request.body()}')
    print(' ')
    return ["sine", "cosine"]


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


@router.post("/grafana/query")
async def query(
        request: Request,
        current_user: User = Depends(CredentialsProvider().load_user),
):
    """Queries.

    Used to return metrics data.
    """
    print(f'Request from grafana to /query: {await request.body()}')
    print(' ')
    return [
        {
            "target": "pps in",
            "datapoints": [
                [622, 1450754160000],
                [365, 1450754220000]
            ]
        },
        {
            "target": "pps out",
            "datapoints": [
                [861, 1450754160000],
                [767, 1450754220000]
            ]
        },
        {
            "target": "errors out",
            "datapoints": [
                [861, 1450754160000],
                [767, 1450754220000]
            ]
        },
        {
            "target": "errors in",
            "datapoints": [
                [861, 1450754160000],
                [767, 1450754220000]
            ]
        }
    ]
