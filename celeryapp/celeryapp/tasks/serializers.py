"""DataFrame serializers.

This package contains classes that implement input serialization
for DataFrame-like objects.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, Literal

import pandas as pd


class DataFrameSerializer(metaclass=ABCMeta):

    @abstractmethod
    def serialize(self, data: pd.DataFrame):
        pass


class PandasSerializer(DataFrameSerializer):
    """Interface for pandas DataFrame serializers.

    Parameters
    ----------
    orient : str, {"dict", "list", "series", "split", "tight", "records",
    "index"}
        Serializer model.
    """

    def __init__(
            self,
            orient: Literal[
                "dict", "list", "series", "split", "tight", "records", "index"
            ] = "dict",
            outer_key: str = None
    ):
        self.orient = orient
        self.outer_key = outer_key

    def serialize(self, data: pd.DataFrame) -> Dict:
        data = data.to_dict(orient=self.orient)

        if self.outer_key is not None:
            return {self.outer_key: data}
        return data

