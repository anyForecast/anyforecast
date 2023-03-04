from typing import List, Dict, Optional

import celery
import fastapi

from .. import security
from ..celery import app as celery_app
from ..models import ml, auth, users

router = fastapi.APIRouter()


@router.post("/users/me/", response_model=users.User)
async def read_users_me(
        current_user: users.User =
        fastapi.Depends(security.TokenProvider().load_user)
):
    return current_user


@router.post("/train/")
async def train(
        trainer: ml.Trainer,
        dataset: ml.Dataset,
        partitions: Optional[List[Dict]] = None,
        current_user: users.User =
        fastapi.Depends(security.TokenProvider().load_user)
):
    """Creates forecaster.

    By "forecaster" it is meant any time series estimator.
    """
    chain = celery.signature(
        'loadPandas', kwargs={
            'dataset': dataset.dict(), 'user': current_user.dict(),
            'partitions': partitions, 'return_schema': True}, app=celery_app)
    chain |= celery.signature(
        'trainSkorchForecasting', kwargs={'trainer': trainer}, app=celery_app,
        queue='training')

    async_task = chain.apply_async()
    return {'async_task_id': async_task.id}


@router.post("/login", response_model=auth.Token)
async def login(
        form_data: fastapi.security.OAuth2PasswordRequestForm
        = fastapi.Depends()
):
    user = security.CredentialsProvider().authenticate(form_data)
    if not user:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = security.TokenGen().generate_token(
        data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}
