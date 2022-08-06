from abc import ABCMeta, abstractmethod

from ._s3_loaders import S3Loader
from ..client_args import create_client_args


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
    return dataframe_loader[name](dataset, user)


class DataFrameLoader(metaclass=ABCMeta):
    def __init__(self, dataset, user):
        client_args = create_client_args(user)
        self.s3_loader = S3Loader(client_args, **dataset)

    def load_schema(self):
        return self.s3_loader.schema.load()

    @abstractmethod
    def load(self, *args, **kwargs):
        pass


class PandasLoader(DataFrameLoader):
    def __init__(self, dataset, user):
        super().__init__(dataset, user)

    def load(self, category_as_object=True, partition_filter=None, **kwargs):
        pandas = self.s3_loader.parquet.to_pandas(
            partition_filter=partition_filter, **kwargs)

        if category_as_object:
            pandas.loc[:, pandas.dtypes == 'category'] = pandas.select_dtypes(
                ['category']).apply(lambda x: x.astype('object'))
        return pandas


class SparkLoader(DataFrameLoader):
    def __init__(self, dataset, user):
        super().__init__(dataset, user)

    def load(self):
        return self.s3_loader.parquet.to_spark()
