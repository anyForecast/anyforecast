from datetime import timedelta

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from .auth import (
    authenticate_user,
    create_access_token,
    get_current_active_user,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from .celery_app.tasks import create_forecaster_task
from .models import *

app = FastAPI()


@app.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # `timedelta` with the expiration time of the token.
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/users/me/", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user


@app.post("/forecast/")
async def create_forecaster(
        forecaster: Forecaster,
        dataset: Dataset,
        current_user: User = Depends(get_current_active_user)
):
    """Creates forecaster.
    """
    task_id = create_forecaster_task.delay(
        *map(dict, (forecaster, dataset, current_user))
    )
    return {'task_id': str(task_id), 'status': 'Processing'}


@app.post("/predict/")
async def predict(
        model: Model,
        dataset: Dataset,
        current_user: User = Depends(get_current_active_user),
):
    """Requests predictions.
    """
    json = PredictTask.run(
        *map(dict, (model, dataset, current_user))
    )
    return json
