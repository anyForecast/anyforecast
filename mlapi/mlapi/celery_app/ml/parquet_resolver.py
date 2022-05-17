from functools import reduce

import pyarrow.parquet as pq

from .exceptions import UniqueGroupIdsError, GroupIdsNotFound


def make_parquet_resolver(name, **kwargs):
    """Function fabric for parquet resolvers.

    Parameters
    ----------
    name : str
        Name for parquet resolver.

    kwargs : named parquet Datasets.
    """
    parquet_resolvers = {
        'timeseries_resolver': TimeSeriesResolver
    }
    return parquet_resolvers[name](**kwargs)


class BaseParquetResolver:
    """Base class for parquet resolver.

    Parameters
    ----------
    **kwargs : named parquet datasets
    """

    def __init__(self, **kwargs):
        self._validate_kwargs(**kwargs)
        self.kwargs = kwargs
        vars(self).update(self.kwargs)

    def get_dataset_by_name(self, name):
        """Returns dataset by name.

        Returns
        -------
        dataset : pyarrow.parquet.ParquetDataset
        """
        if name not in self.kwargs:
            raise ValueError('Unknown dataset "{}"'.format(name))
        return self.kwargs[name]

    def get_arrow_schema(self, dataset):
        """Returns pyarrow schema.

        Returns
        -------
        arrow_schema : pyarrow.lib.Schema
        """
        return dataset.schema

    def get_features(self, include_none=True):
        """Returns features for each dataset.

        Returns
        -------
        features : dict, str -> list
            Mapping from dataset name to its feature names.
        """
        features = {}
        for name, dataset in self.kwargs.items():
            if dataset is None:
                if include_none:
                    features[name] = []
            else:
                features[name] = self.get_arrow_schema(dataset).names
        return features

    def to_pandas(self, dataset):
        """Converts parquet dataset to pandas DataFrame.
        """
        return dataset.read_pandas().to_pandas()

    def _get_datasets(self, include_nones=False):
        datasets = self.kwargs.values()
        if include_nones:
            return datasets
        return [x for x in datasets if x is not None]

    def merge(self, on, **kwargs):
        datasets = self._get_datasets()

        if len(datasets) == 1:
            return self.to_pandas(datasets[0])

        merged_df = reduce(
            lambda left, right: left.merge(right, on, **kwargs),
            datasets)
        return merged_df

    def _validate_kwargs(self, **kwargs):
        for key, o in kwargs.items():
            if o is not None:
                if not isinstance(o, pq._ParquetDatasetV2):
                    o_name = o.__class__.__name__
                    raise TypeError(
                        'Not "None" parameters must be parquet datasets V2, '
                        'that is, :class:`pyarrow.parquet._ParquetDatasetV2`. '
                        'Instead, kwarg "{}" received type "{}"'.format(key,
                                                                    o_name))


class TimeSeriesResolver(BaseParquetResolver):
    def __init__(self, target, time_varying_known_reals=None,
                 time_varying_unknown_reals=None, static_categoricals=None):
        super().__init__(target=target,
                         time_varying_known_reals=time_varying_known_reals,
                         time_varying_unknown_reals=time_varying_unknown_reals,
                         static_categoricals=static_categoricals)
        self._validate()

    def _validate(self):
        self._validate_group_ids()
        self._validate_timestamp()

    def _validate_group_ids(self):
        """All datasets must share the same group_ids.
        """
        # Obtain all group_ids.
        datasets = self._get_datasets()
        all_group_ids = [self._get_dataset_group_ids(ds) for ds in datasets
                         if ds is not None]

        # Uniqueness for list of lists.
        unique_group_ids = [list(x) for x in
                            set(tuple(x) for x in all_group_ids)]

        # Raise error if datasets dont share same group_ids.
        if len(unique_group_ids) > 1:
            raise UniqueGroupIdsError()

        # Raise error if group_ids were not found.
        group_ids = unique_group_ids[0]
        if not group_ids:
            raise GroupIdsNotFound()

        self._group_ids = group_ids

    def _validate_timestamp(self):
        """All datasets must contain a timestamp column.
        """
        datasets = self._get_datasets()
        for ds in datasets:
            arrow_schema = self.get_arrow_schema(ds)
            # The following call to :meth:`field` method already raises an
            # error if the field does not exist.
            arrow_schema.field('timestamp')
        self._timestamp = 'timestamp'

    def _get_dataset_group_ids(self, dataset):
        arrow_schema = self.get_arrow_schema(dataset)
        return [x for x in arrow_schema.names if x.startswith('group_id')]

    def get_group_ids(self):
        """Returns the group_ids shared by all datasets.
        """
        return self._group_ids

    def get_timestamp(self):
        """Returns timestamp feature name.
        """
        return self._timestamp

    def get_primary_key(self):
        """Returns data primary key.

        Each data record is uniquely identified by the group_ids and the
        timestamp.
        """
        group_ids = self.get_group_ids()
        timestamp = self.get_timestamp()
        pk = group_ids + [timestamp]
        return pk

    def merge(self, **kwargs):
        pk = self.get_primary_key()
        return super().merge(on=pk, **kwargs)

    def resolve(self):
        X = self.merge()
        group_ids = self.get_group_ids()
        timestamp = self.get_timestamp()
        return {'X': X, 'group_ids': group_ids, 'timestamp': timestamp}

    def get_features(self, include_pk=False):
        features = super().get_features()
        if not include_pk:
            pk = self.get_primary_key()
            names_without_pk = {
                k: [x for x in v if x not in pk]
                for k, v in features.items()
            }
            return names_without_pk
        return features
