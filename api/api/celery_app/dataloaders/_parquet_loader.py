from abc import ABCMeta

import awswrangler as wr


def make_parquet_loader(name, base_dir, s3_filesystem):
    parquet_loaders = {
        'pandas': ParquetToPandas,
        'spark': ParquetToSpark
    }
    return parquet_loaders[name](base_dir, s3_filesystem)


class ParquetLoader(metaclass=ABCMeta):
    def __init__(self, base_dir, s3_filesystem):
        self.base_dir = base_dir
        self.s3_filesystem = s3_filesystem


class ParquetToPandas(ParquetLoader):
    def __init__(self, base_dir, s3_filesystem):
        super().__init__(base_dir, s3_filesystem)

    def __call__(self, partition_filter, **kwargs):
        self._set_awswrangler_endpoint()
        return wr.s3.read_parquet(
            self.base_dir, dataset=True, partition_filter=partition_filter,
            **kwargs)

    def _set_awswrangler_endpoint(self):
        endpoint_url = self.s3_filesystem.client_kwargs['endpoint_url']
        wr.config.s3_endpoint_url = endpoint_url


class ParquetToSpark(ParquetLoader):

    def __init__(self, base_dir, s3_filesystem):
        super().__init__(base_dir, s3_filesystem)
        raise NotImplementedError(":class:`ParquetToSpark` not implemented yet.")
