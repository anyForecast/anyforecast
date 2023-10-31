import os
from typing import Any, Protocol

import pandas as pd
import requests


class Serializer(Protocol):
    def serialize(self, data: Any) -> dict:
        ...


class IdentitySerializer:
    def serialize(self, data: dict) -> dict:
        return data


class PandasSerializer:
    def serialize(self, data: pd.DataFrame) -> dict:
        return data.to_dict(orient="split")


class Predictor:
    """Real time inference for MLFlow hosted models."""

    def __init__(
        self, endpoint_name, serializer: Serializer = IdentitySerializer()
    ):
        self.endpoint_name = endpoint_name
        self.serializer = serializer
        self.session = requests.Session()

    @property
    def url(self) -> str:
        return os.path.join(self.endpoint_name, "invocations")

    def predict(self, data: Any):
        """Returns the inference from the specified endpoint.

        Parameters
        ----------
        data : object
            Input data for which you want the model to provide inference.
            If a serializer was specified when creating the Predictor, the
            result of the serializer is sent as input data. Otherwise the data
            must be json serializable and the predict method then sends the
            data in the request body as is.
        """
        data = self.serializer.serialize(data)
        response: requests.Response = self.session.post(self.url, json=data)
        response.raise_for_status()

        return response.json()
