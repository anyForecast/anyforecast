from ..parquet import ParquetMerger


class TimeSeriesMerger(ParquetMerger):
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

    def _get_dataset_group_ids(self, dataset):
        arrow_schema = dataset.get_arrow_schema()
        return [x for x in arrow_schema.names if x.startswith('group_id')]

    def get_group_ids(self, validate=True):
        # Obtain all group_ids.
        datasets = self._get_datasets()
        all_group_ids = [
            self._get_dataset_group_ids(ds)
            for ds in datasets
            if ds is not None
        ]

        # Uniqueness for list of lists.
        unique_group_ids = [list(x) for x in
                            set(tuple(x) for x in all_group_ids)]

        if validate:
            if len(unique_group_ids) > 1:
                raise
            return unique_group_ids[0]
        return unique_group_ids

    def merge(self, **kwargs):
        group_ids = self.get_group_ids()
        pk = group_ids + ['timestamp']  # Primary key.
        return super().merge(on=pk, **kwargs)

    def resolve_names(self, include_pk=False):
        names = super().resolve_names()
        if not include_pk:
            group_ids = self.get_group_ids()
            pk = group_ids + ['timestamp']
            names_without_pk = {
                k: [x for x in v if x not in pk]
                for k, v in names.items()
            }
            return names_without_pk
        return names
