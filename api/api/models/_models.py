from typing import Optional, Union

from pydantic import BaseModel


class Pivot(BaseModel):
    pivot: bool


class Predictor(BaseModel):
    task_id: str


class PredictionModel(BaseModel):
    """Base model for prediction tasks.
    """


class DateRange(BaseModel):
    """Defines date range.
    """
    start: str
    end: str


class WhatIf(PredictionModel):
    """Defines a "what if" scenario.

    Parameters
    ----------
    feature : str
        Feature to modify.

    value : int or float
        Value to insert
    """
    feature: str
    value: Union[int, float]


class GroupIdPrediction(PredictionModel):
    """Defines how a prediction for a single group_id is computed.

    Parameters
    ----------
    group_id : dict, str -> str
        Dictionary that maps from group_id label to its value. For example,
        >>> group_id = {"product_id": "10"}  # single-valued group_id
        or
        >>> group_id = {"product_id": "10", "store_id": "1"}  # multi-value group_id

    what_if : None or WhatIf, default=None
        Include this parameter to perform a "what if" scenario on the given
        group_id.
    """
    group_id: dict
    what_if: Optional[WhatIf] = None


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


class Trainer(BaseModel):
    task_name: str
    algorithm: str
    forecast_horizon: int
    freq: str
    perform_hpo: Optional[bool] = False
    kwargs: Optional[dict] = None
