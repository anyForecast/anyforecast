from ._client_args import create_client_args
from ..ml.dataloader import DataLoader
from ..ml.schema_resolver import SchemaResolver


class PandasLoader:
    def __init__(self, dataset, user):
        self.dataset = dataset
        self.user = user

    def load_pandas(self, return_schema_resolver=False):
        loader = DataLoader(create_client_args(self.user))
        schema_data = loader.load_schema(**self.dataset)
        schema_resolver = SchemaResolver(schema_data)

        # Load parquet
        arrow_schema = schema_resolver.to_arrow()
        parquet = loader.load_parquet(**self.dataset, schema=arrow_schema)

        # Convert parquet to pandas :class:`DataFrame`.
        X = parquet.read_pandas().to_pandas()

        if not return_schema_resolver:
            return X
        return X, schema_resolver
