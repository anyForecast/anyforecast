import dataclasses
import os
from typing import Literal

import pandas as pd

from anyforecast.definitions import ROOT_DIR

__all__ = ["load_stallion", "load_iris"]

DATA_DIR = os.path.join(ROOT_DIR, "datasets/data")


def _get_filepath(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


def _load_csv(
    filepath,
    names: list[str] | None = None,
    header: int | Literal["infer"] | None = "infer",
) -> pd.DataFrame:
    return pd.read_csv(filepath, names=names, header=header)


def _load_parquet():
    raise NotImplementedError()


@dataclasses.dataclass
class TimeseriesDataset:
    target: list[str]
    group_cols: list[str]
    datetime: str
    feature_names: list[str]
    freq: str
    filepath: str

    def load_csv(self):
        return pd.read_csv(self.filepath)


@dataclasses.dataclass
class Dataset:
    target: list[str]
    feature_names: list[str]
    filepath: str

    def load_csv(self):
        return pd.read_csv(self.filepath)


def load_stallion() -> TimeseriesDataset:
    """Load and return the iris dataset (time series)."""

    filepath = _get_filepath("stallion.csv")

    feature_names = [
        "agency",
        "sku",
        "date",
        "industry_volume",
        "price_regular",
        "price_actual",
        "discount",
    ]

    target = "volume"
    group_cols = ["agency", "sku"]
    datetime = "date"
    freq = "MS"

    return TimeseriesDataset(
        target=target,
        group_cols=group_cols,
        datetime=datetime,
        freq=freq,
        feature_names=feature_names,
        filepath=filepath,
    )


def load_iris() -> Dataset:
    filepath = _get_filepath("iris.csv")

    target = "species"

    feature_names = [
        "sepal length (cm)",
        "sepal width (cm)",
        "petal length (cm)",
        "petal width (cm)",
    ]

    return Dataset(
        target=target, feature_names=feature_names, filepath=filepath
    )
