from pydantic import BaseModel


class Predictor(BaseModel):
    name: str
    algorithm: str
    forecast_horizon = int
    perform_hpo: bool = False
    bucket_name: str
