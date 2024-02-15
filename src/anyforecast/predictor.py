from typing import Any, Protocol

from anyforecast import endpoint, serializers


class Serializer(Protocol):
    def serialize(self, data: Any) -> Any: ...


class Predictor:
    """Real time inference for MLflow hosted models."""

    def __init__(
        self,
        endpoint: endpoint.Endpoint,
        serializer: Serializer = serializers.IdentitySerializer(),
    ):
        self.endpoint = endpoint
        self.serializer = serializer

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
        response = self.endpoint.make_request(json=data)
        response.raise_for_status()

        return response.json()
