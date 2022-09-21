"""
This module contains Pydantic models used during the machine learning cycle.
"""

from typing import Optional, Union

from pydantic import BaseModel


class ReturnTruth(BaseModel):
    bool: bool


class Pivot(BaseModel):
    bool: bool


class Predictor(BaseModel):
    """Defines the predictor to be loaded from MlFlow to perform predictions.
    """
    model_name: str


class PredictionDateRange(BaseModel):
    """Defines prediction date range.
    """
    start: str
    end: str


class WhatIf(BaseModel):
    """Defines a "what if" scenario.

    Parameters
    ----------
    feature : str
        Feature to modify.

    value : int or float
        Value to insert
    """
    column: str
    value: Optional[Union[int, float]] = None
    method: Optional[str] = None
    percentage: Optional[Union[int, float]] = None


class GroupParams(BaseModel):
    """Defines how a prediction for a single group_id is computed.

    Parameters
    ----------
    group_id : dict, str -> str

    what_if : None or WhatIf, default=None
        Include this parameter to perform a "what if" scenario on the given
        group_id.
    """
    group_id: dict
    what_if: Optional[WhatIf] = None


class Dataset(BaseModel):
    dataset_group_name: str
    dataset_name: str
    bucket_name: Optional[str] = 'datasets'


class Trainer(BaseModel):
    task_name: str
    algorithm: str
    forecast_horizon: int
    freq: str
    perform_hpo: Optional[bool] = False
    kwargs: Optional[dict] = None
