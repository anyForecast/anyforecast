import io
import json
from typing import Union, List, Optional, Callable, Dict

import awswrangler as wr

from ._user_session import UserSession


def create_dataloader(user, dataloader_name):
    dataloaders = {
        'parquet': ParquetLoader,
        'json': JsonLoader
    }
    return dataloaders[dataloader_name](UserSession(**user))


class DataLoader:
    def __init__(self, user_session: UserSession):
        self.user_session = user_session

    def _create_boto3_session(self):
        return self.user_session.create_boto3_session()

    def _create_minio_client(self):
        return self.user_session.create_minio_client()


class ParquetLoader(DataLoader):
    """Parquet loaders.
    """

    def __init__(self, user_session: UserSession):
        super().__init__(user_session)
        self._set_wrangler_config()

    def load_pandas(
            self,
            path: Union[str, List[str]],
            dataset: bool = True,
            partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
            use_threads: Union[bool, int] = True,
            map_types: bool = True,
            **kwargs
    ):
        return wr.s3.read_parquet(
            path=path, dataset=dataset, partition_filter=partition_filter,
            use_threads=use_threads, map_types=map_types,
            boto3_session=self._create_boto3_session(), **kwargs)

    def load_spark(self):
        pass

    def _set_wrangler_config(self):
        wr.config.s3_endpoint_url = self.user_session.get_minio_endpoint()


class JsonLoader(DataLoader):
    def __init__(self, user_session: UserSession):
        super().__init__(user_session)

    def load(
            self,
            bucket_name: str,
            object_name: str
    ):
        """Loads stored json object from the bucket.
        """
        minio_client = self._create_minio_client()
        json_object = minio_client.get_object(bucket_name, object_name)
        return json.load(io.BytesIO(json_object.data))
