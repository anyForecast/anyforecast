from fastapi import Depends, HTTPException, APIRouter, status
from fastapi.security import OAuth2PasswordRequestForm

from ..auth import (
    CredentialsProvider,
    TokenGen
)
from ..auth import (
    TokenProvider
)
from ..celery_app.tasks import training_task
from ..models import *

router = APIRouter()


@router.post("/users/me/", response_model=User)
async def read_users_me(
        current_user: User = Depends(TokenProvider().load_user)
):
    return current_user


@router.post("/trainer/")
async def train(
        trainer: Trainer,
        dataset: Dataset,
        current_user: User = Depends(TokenProvider().load_user)
):
    """Creates forecaster.

    By "forecaster" it is meant any time series estimator.
    """
    task_id = training_task.delay(
        *map(dict, (trainer, dataset, current_user))
    )
    return {'task_id': str(task_id), 'status': 'Processing'}


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
