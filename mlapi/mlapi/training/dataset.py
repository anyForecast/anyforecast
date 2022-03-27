import pandas as pd
import pyarrow.parquet as pq
import s3fs


class DatasetBridge:
    """Bridges buckets and dataset creation.
    """
    MINIO_ENDPOINT = 'http://minio:9000'
    ROOT_PATH = 'data/'

    def __init__(self, bucket_name, access_key, secret_key):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key

    def get_parquet_dataset(self, base_dir):
        s3_path = self._make_s3_root_path(base_dir)
        fs = self._get_s3_filesystem()
        parquet_dataset = pq.ParquetDataset(s3_path, filesystem=fs)
        return parquet_dataset

    def _make_s3_root_path(self, *args):
        path = ("s3://" +
                # self.ROOT_PATH +
                self.bucket_name +
                '/' +
                '/'.join(args))
        return path

    def _get_s3_filesystem(self):
        client_kwargs = {
            'endpoint_url': self.MINIO_ENDPOINT,
            'aws_access_key_id': self.access_key,
            'aws_secret_access_key': self.secret_key,
            'verify': False
        }
        fs = s3fs.S3FileSystem(anon=False, use_ssl=False,
                               client_kwargs=client_kwargs)

        return fs


class Dataset:
    """Interface for parquet datasets.
    """

    def __init__(self, parquet_ds):
        self.parquet_ds = parquet_ds

    def get_pandas_df(self):
        return self.parquet_ds.read_pandas().to_pandas()

    def get_group_ids(self):
        arrow_schema = self.get_arrow_schema()
        return [x for x in arrow_schema.names if x.startswith('group_id')]

    def get_arrow_schema(self):
        return self.parquet_ds.schema.to_arrow_schema()

    def get_names(self):
        arrow_schema = self.get_arrow_schema()
        return arrow_schema.names

    def merge(self, parquet_ds):
        left_df = self.get_pandas_df()
        right_df = parquet_ds.get_pandas_df()
        group_ids = self.get_group_ids()
        merged_df = pd.merge(
            left=left_df,
            right=right_df,
            on=group_ids + ['timestamp']
        )
        return merged_df


class DatasetsCollector:
    """Base class for dataset collectors.
    """

    def __init__(self, **kwargs):
        self._validate_kwargs(**kwargs)
        self.kwargs = kwargs
        vars(self).update(self.kwargs)

    def get_dataset_by_name(self, name):
        if name not in self.kwargs:
            raise ValueError('Unkown dataset name {}'.format(name))
        return self.kwargs[name]

    def get_names(self, include_pk=True):
        names = {
            k: v.get_names()
            if v is not None
            else [] for k, v in self.kwargs.items()
        }
        if not include_pk:
            group_ids = self.get_group_ids()
            pk = group_ids + ['timestamp']
            names_without_pk = {
                k: [x for x in v if x not in pk]
                for k, v in names.items()
            }
            return names_without_pk
        return names

    def _get_datasets(self):
        return list(self.kwargs.values())

    def get_group_ids(self, validate=True):
        datasets = self._get_datasets()
        all_group_ids = [ds.get_group_ids() for ds in datasets if
                         ds is not None]

        # Uniqueness for list of lists.
        unique_group_ids = [list(x) for x in
                            set(tuple(x) for x in all_group_ids)]

        if validate:
            if len(unique_group_ids) > 1:
                raise
            return unique_group_ids[0]
        return unique_group_ids

    def _validate_kwargs(self, **kwargs):
        for key, dataset in kwargs.items():
            if dataset is not None:
                if not isinstance(dataset, Dataset):
                    raise TypeError(
                        'All parameters must be of type Dataset. '
                        'Instead, kwarg {} received type {}'.format(key, type(
                            dataset).__name__)
                    )


class TimeSeriesCollector(DatasetsCollector):
    def __init__(
            self,
            target,
            time_varying_known_reals=None,
            time_varying_unknown_reals=None,
            static_categoricals=None
    ):
        super().__init__(
            target=target,
            time_varying_known_reals=time_varying_known_reals,
            time_varying_unknown_reals=time_varying_unknown_reals,
            static_categoricals=static_categoricals
        )
