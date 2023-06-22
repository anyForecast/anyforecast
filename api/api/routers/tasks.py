from typing import Dict

import celery
import fastapi

from .. import models
from ..celery import app as celery_app


def create_signature(task_name, **kwargs):
    """Auxiliary for creating celery signatures.
    """
    return celery.signature(task_name, kwargs=kwargs, app=celery_app)


@router.post("/tasks/deepforecast/")
async def make_deepforecast(
        tsdataset_args: models.TimeseriesDatasetArgs,
        forecast_args: models.ForecastArgs,
        hparams: Dict,
        s3_args: models.S3Args,
        current_user: models.User = fastapi.Depends(load_user)
):
    """Creates deep forecaster.
    """
    chain = create_signature(
        task_name='extract',
        s3_args=s3_args.dict(),
        user=current_user.dict()
    )

    chain |= create_signature(
        task_name='train',
        tsdataset_args=tsdataset_args.dict(),
        forecast_args=forecast_args.dict()
    )

    return {'async_task_id': chain.apply_async()}


@router.post("/tasks/preprocess/")
async def make_proprocess(
        ds_args: models.DatasetArgs,
        strategy: models.PreprocessStrategy,
        s3_args: models.S3Args,
        current_user: models.User = fastapi.Depends(load_user)
):
    chain = create_signature(
        task_name='extract',
        url=s3_args.s3_url,
        user=current_user.dict()
    )

    chain |= create_signature(
        task_name='preprocess',
        data_args=ds_args.dict(),
        strategy=strategy.dict()

    )

    chain |= create_signature(
        task_name='load',
        dest_url=s3_args.s3_dest_url,
        user=current_user.dict()
    )

    return {'async_task_id': chain.apply_async()}
