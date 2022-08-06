import json
from typing import Dict

import pandas as pd

from ..exceptions import UnknownPandasSerializer


def _make_pandas_serializer(name):
    """Factory function for Pandas serializers.

    Parameters
    ----------
    name : str
        Name of serializer.

    Returns
    -------
    serializer
    """
    serializers = {
        'to_records': ToRecords
    }
    try:
        return serializers[name]()
    except KeyError:
        raise UnknownPandasSerializer(name=name)


class PandasSerializer:
    """Interface for pandas DataFrame serializers.

    Parameters
    ----------
    serializer_model : str
        Serializer model.
    """

    def __init__(self, serializer_model):
        self.serializer = _make_pandas_serializer(serializer_model)

    def serialize(self, data: pd.DataFrame, *args, **kwargs) -> Dict:
        return self.serializer.serialize(data, *args, **kwargs)


class BaseSerializer:
    def __init__(self):
        pass

    def check_data(self, data):
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise ValueError("Received object is not a pandas DataFrame "
                             "or Series.")


class ToRecords(BaseSerializer):
    def __init__(self):
        super().__init__()

    def serialize(self, data: pd.DataFrame):
        return data.to_dict('records')
