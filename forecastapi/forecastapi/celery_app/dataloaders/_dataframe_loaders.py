from abc import ABCMeta, abstractmethod

from ._minio_dataloaders import MinioDataloadersFactory


def make_dataframe_loader(name, dataset, user):
    """Factory function for :class:`DataFrameLoader` instances.

    Parameters
    ----------
    name : str, {'pandas', 'spark'}
        Name of dataframe loader

    dataset : dict
        Dataset dict model

    user : dict
        User dict model
    """
    dataframe_loader = {
        'pandas': PandasLoader,
        'spark': SparkLoader
    }
    dataloaders_factory = MinioDataloadersFactory(user, dataset)
    return dataframe_loader[name](dataloaders_factory)


class DataFrameLoader(metaclass=ABCMeta):
    def __init__(self, dataloaders_factory):
        self._dataloaders_factory = dataloaders_factory

    def get_dataloader(self, data_format):
        return self._dataloaders_factory.get_dataloader(data_format)

    def load_schema(self):
        json_loader = self.get_dataloader('json')
        return json_loader.load('schema')

    @abstractmethod
    def load(self, *args, **kwargs):
        pass


class PandasLoader(DataFrameLoader):
    def __init__(self, dataloaders_factory):
        super().__init__(dataloaders_factory)

    def load(self, partition_filter=None, **kwargs):
        parquet_loader = self.get_dataloader('parquet')
        return parquet_loader.to_pandas(
            partition_filter=partition_filter, **kwargs)


class SparkLoader(DataFrameLoader):
    def __init__(self, dataloaders_factory):
        super().__init__(dataloaders_factory)

    def load(self):
        parquet_loader = self.get_dataloader('parquet')
        return parquet_loader.to_spark()
