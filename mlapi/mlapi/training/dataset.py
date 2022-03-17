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

    def get_dataset(self, base_dir):
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
