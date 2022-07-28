from typing import Optional

from pydantic import BaseModel


class Token(BaseModel):
    """Pydantic Model that will be used in the token endpoint for the response.
    """
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    s3_endpoint: Optional[str] = None


class Dataset(BaseModel):
    dataset_group_name: str
    dataset_name: str
    bucket_name: Optional[str] = 'datasets'


class UserInDB(User):
    hashed_password: str


class Model(BaseModel):
    name: str


class Forecaster(BaseModel):
    task_name: str
    algorithm: str
    forecast_horizon: int
    freq: str
    perform_hpo: Optional[bool] = False
