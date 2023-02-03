"""
This module contains Pydantic models used during the machine learning cycle.
"""

from typing import Optional, Union, List

from pydantic import BaseModel


class Predictor(BaseModel):
    """Defines the predictor to be loaded from MlFlow to perform predictions.
    """
    model_name: str


class DateRange(BaseModel):
    """Defines prediction date range.
    """
    start: str
    end: str


class WhatIf(BaseModel):
    """Defines a "what if" scenario.
    """
    group_id: dict
    input_cols: List[str]
    value: Optional[Union[int, float]] = None
    method: Optional[str] = None
    percentage: Optional[Union[int, float]] = None


class Dataset(BaseModel):
    dataset_group_name: str
    dataset_name: str
    bucket_name: Optional[str] = 'datasets'


class Trainer(BaseModel):
    algorithm: str
    max_prediction_length: int
    freq: str
    hparams: Optional[dict] = None
