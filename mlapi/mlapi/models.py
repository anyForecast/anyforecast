from pydantic import BaseModel


class Forecaster(BaseModel):
    name: str
    algorithm: str
    forecast_horizon = int
    perform_hpo: bool = False
    bucket_name: str


class Task(BaseModel):
    """ Celery task representation """
    task_id: str
    status: str
