from typing import List, Dict, Optional

from celery import signature
from ..celery import app
from fastapi import Depends, HTTPException, APIRouter, status
from fastapi.security import OAuth2PasswordRequestForm

from ..auth import CredentialsProvider, TokenGen, TokenProvider
from ..models.auth import Token
from ..models.ml import Trainer, Dataset
from ..models.users import User

router = APIRouter()


@router.post("/users/me/", response_model=User)
async def read_users_me(
        current_user: User = Depends(TokenProvider().load_user)
):
    return current_user


@router.post("/train/")
async def train(
        trainer: Trainer,
        dataset: Dataset,
        partitions: Optional[List[Dict]] = None,
        current_user: User = Depends(TokenProvider().load_user)
):
    """Creates forecaster.

    By "forecaster" it is meant any time series estimator.
    """
    chain = signature(
        'LoadPandas',
        kwargs={
            'dataset': dataset.dict(),
            'user': current_user.dict(),
            'partitions': partitions,
            'return_schema': True
        },
        app=app
    )
    chain |= signature(
        'TrainSkorchForecasting',
        kwargs={'trainer': trainer},
        queue='training',
        app=app
    )

    async_task = chain.apply_async()
    return {'async_task_id': async_task.id}


@router.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = CredentialsProvider().authenticate(form_data)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = TokenGen().generate_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}
