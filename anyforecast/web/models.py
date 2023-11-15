"""anyForecast pydantic models."""

from typing import Dict, Optional

from pydantic import BaseModel


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None


class UserInDB(User):
    hashed_password: str


class DatasetArgs(BaseModel):
    """Dataset args."""

    group_ids: list[str]
    timestamp: str
    target: str
    freq: str


class TimeseriesDatasetArgs(DatasetArgs):
    """Timeseries datasets args."""

    time_varying_known: list[str]
    time_varying_unknown: list[str]
    static_categoricals: list[str]


class ForecastArgs(BaseModel):
    """Time series neuralmodels args."""

    freq: str
    seasonality: int
    horizon: int = 28


class S3Args(BaseModel):
    """File arguments."""

    s3_url: str
    s3_dest_url: Optional[str] = None


class Transformer(BaseModel):
    """Transformer."""

    package: str = "sklearn"
    transformer: str
    kwargs: Dict


class PreprocessStrategy(BaseModel):
    """Preprocessing strategy."""

    target_transformer: Optional[Transformer] = None
    num_transformer: Optional[Transformer] = None
    cat_encoder: Optional[Transformer] = None


class Token(BaseModel):
    """Pydantic Model that will be used in the token endpoint for the response."""

    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None
