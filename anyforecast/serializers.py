import json
from typing import Any

import numpy as np
import pandas as pd


class JSONSerializer:
    """Serialize data to a JSON formatted string."""

    def serialize(self, data: Any) -> str:
        """Serialize data of various formats to a JSON formatted string.

        Parameters
        ----------
        data : object
            Data to be serialized.

        Returns
        -------
        The data serialized as a JSON string.
        """
        if isinstance(data, dict):
            return json.dumps(
                {
                    key: value.tolist()
                    if isinstance(value, np.ndarray)
                    else value
                    for key, value in data.items()
                }
            )

        if hasattr(data, "read"):
            return data.read()

        if isinstance(data, np.ndarray):
            return json.dumps(data.tolist())

        return json.dumps(data)


class IdentitySerializer:
    def serialize(self, data: Any) -> Any:
        return data


class PandasSerializer:
    def serialize(self, data: pd.DataFrame) -> dict:
        return data.to_dict(orient="split")
