from abc import ABCMeta, abstractmethod

from ._base_task import BaseTask
from ..dataloaders import SchemaResolver
from ..dataloaders import make_dataframe_loader
from ..exceptions import UnknownDataLoadingFormat


class LoadDatasetTask(BaseTask):
    def __init__(self):
        super().__init__()

    def run(
            self, dataset, user, partitions, format, return_schema,
            enforce_schema_dtypes
    ):
        tasks = {
            'pandas': LoadPandasTask,
            'spark': LoadSparkTask
        }

        try:
            return tasks[format](dataset, user, partitions).run(
                enforce_schema_dtypes, return_schema)
        except KeyError:
            raise UnknownDataLoadingFormat(
                format=format, available_formats=list(tasks))


class LoadFormatTask(metaclass=ABCMeta):

    def __init__(self, dataset, user, partitions=None):
        self.dataset = dataset
        self.user = user
        self.partitions = partitions

    @abstractmethod
    def run(self, enforce_schema_dtypes=True, return_schema=True):
        pass

    def _make_partition_filter(self):
        if self.partitions is None:
            return None

        def filter_function(partition):
            return partition in self.partitions

        return filter_function


class LoadSparkTask(LoadFormatTask):
    def __init__(self, dataset, user, partitions):
        super().__init__(dataset, user, partitions)

    def run(self, enforce_schema_dtypes=True, return_schema=True):
        raise NotImplementedError()


class LoadPandasTask(LoadFormatTask):

    def __init__(self, dataset, user, partitions):
        super().__init__(dataset, user, partitions)

    def run(self, enforce_schema_dtypes=True, return_schema=True):
        loader = make_dataframe_loader('pandas', self.dataset, self.user)

        # Load schema data and convert it to an
        # :class:´SchemaResolver´ instance.
        schema = loader.load_schema()
        schema = SchemaResolver(schema)

        # Load only features inside schema.
        feature_names = schema.get_feature_names()
        partition_filter = self._make_partition_filter()
        pandas = loader.load(
            partition_filter=partition_filter,
            columns=feature_names
        )

        if enforce_schema_dtypes:
            dtypes = schema.get_dtypes_for('all', exclude='timestamp')
            pandas = pandas.astype(dtypes)

        # Partition columns are still present in the data eventhough they
        # are not part of the schema. The only solution until now is to drop
        # them manually.
        pandas = pandas[feature_names]

        result = {'X': pandas}
        if return_schema:
            result['schema'] = schema
        return result
