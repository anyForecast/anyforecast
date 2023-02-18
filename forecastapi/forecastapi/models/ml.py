"""
This module contains Pydantic models used during the machine learning cycle.
"""

from typing import Optional, Union, List, Dict

from pydantic import BaseModel


class Predictor(BaseModel):
    """
    """
    user: str
    model_name: str
    service_name: str = 'mlflow'


class DateRange(BaseModel):
    """Prediction date range.
    """
    start: str
    end: str


class WhatIf(BaseModel):
    """Defines a "what if" scenario.
    """
    group_id: Dict
    input_cols: List[str]
    value: Optional[Union[int, float]] = None
    method: Optional[str] = None
    percentage: Optional[Union[int, float]] = None


class Dataset(BaseModel):
    """Dataset location.
    """
    dataset_group_name: str
    dataset_name: str
    bucket_name: Optional[str] = 'datasets'


class Transformer(BaseModel):
    """Transformer.
    """
    package: str = 'sklearn'
    transformer: str
    kwargs: Dict


class Preprocessing(BaseModel):
    """Preprocessing strategy.
    """
    target_transformer: Optional[Transformer] = None
    num_transformer: Optional[Transformer] = None
    cat_encoder: Optional[Transformer] = None


class Trainer(BaseModel):
    """Training configuration.
    """
    algorithm: str
    max_prediction_length: int
    freq: str
    hparams: Optional[Dict] = None
    preprocessing: Optional[Preprocessing] = None
