from typing import Dict

import pandas as pd


class PandasNativeSerializer:
    """Interface for pandas DataFrame serializers.

    Parameters
    ----------
    orient : str
        Serializer model.
    """

    def __init__(self, orient='dict'):
        self.orient = orient

    def serialize(self, data: pd.DataFrame, timestamp=None) -> Dict:
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise ValueError("Received object is not a pandas DataFrame "
                             "or Series.")

        if timestamp is not None:
            data.rename(columns={timestamp: 'timestamp'}, inplace=True)

        return data.to_dict(self.orient)
