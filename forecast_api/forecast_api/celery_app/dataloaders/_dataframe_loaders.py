from abc import ABCMeta, abstractmethod

from ._s3_loaders import S3LoadersFactory


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
    loaders_factory = S3LoadersFactory(user, dataset)
    return dataframe_loader[name](loaders_factory)


class DataFrameLoader(metaclass=ABCMeta):
    def __init__(self, loaders_factory):
        self.loaders_factory = loaders_factory
        self.parquet_loader = loaders_factory.get_loader('parquet')

    def load_schema(self):
        json_loader = self.loaders_factory.get_loader('json')
        return json_loader.load('schema')

    @abstractmethod
    def load(self, *args, **kwargs):
        pass


class PandasLoader(DataFrameLoader):
    def __init__(self, loaders_factory):
        super().__init__(loaders_factory)

    def load(self, partition_filter=None, **kwargs):
        return self.parquet_loader.to_pandas(
            partition_filter=partition_filter, **kwargs)


class SparkLoader(DataFrameLoader):
    def __init__(self, loaders_factory):
        super().__init__(loaders_factory)

    def load(self):
        return self.parquet_loader.to_spark()