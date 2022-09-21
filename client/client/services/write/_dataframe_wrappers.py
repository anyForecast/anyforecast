from abc import ABCMeta, abstractmethod

from .writers import PandasWriter, SparkWriter
import pandas as pd


def make_dataframe_wrapper(data):
    """Factory function for DataFrame wrappers.

    Parameters
    ----------
    data : DataFrame_like

    Returns
    -------
    dataframe_wrapper : DataFrameWrapper
    """
    wrappers = {
        pd.core.frame.DataFrame: PandasWrapper
    }
    return wrappers[type(data)](data)


class DataFrameWrapper(metaclass=ABCMeta):
    """
    .. note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self, data):
        self.data = data

    @abstractmethod
    def get_dtypes(self):
        pass

    @abstractmethod
    def get_names(self):
        pass

    @property
    @abstractmethod
    def write(self):
        return None


class PandasWrapper(DataFrameWrapper):
    def __init__(self, data):
        super().__init__(data)

    def get_dtypes(self):
        return self.data.dtypes.astype(str)

    def get_names(self):
        return list(self.data)

    @property
    def write(self):
        return PandasWriter()


class SparkWrapper(DataFrameWrapper):
    def __init__(self, data):
        super().__init__(data)
        raise NotImplementedError()

    def get_dtypes(self):
        pass

    def get_names(self):
        pass

    @property
    def write(self):
        return SparkWriter()
