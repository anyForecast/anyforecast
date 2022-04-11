from fastapi import FastAPI

from pydantic import BaseModel


class Forecaster(BaseModel):
    name: str
    algorithm: str
    forecast_horizon = int
    perform_hpo: bool = False
    bucket_name: str
    minio_host: str = None


class Credentials(BaseModel):
    access_key: str
    secret_key: str
    token: str = None


class Task(BaseModel):
    """Celery task representation.
    """
    task_id: str
    status: str


app = FastAPI()


@app.post('/forecast')
async def create_forecaster(service: Forecaster, credentials: Credentials):
    """Creates a forecast creation task.

    Returns
    -------
    response : dict
        Json with task_id to client in order to retrieve result
    """
    return {'service': service, 'credentials': credentials}
    # return {'task_id': str(task_id), 'status': 'Processing'}
