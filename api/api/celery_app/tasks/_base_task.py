from abc import ABCMeta

from ..dataloaders import make_dataframe_loader
from ..dataloaders import SchemaResolver


class BaseTask(metaclass=ABCMeta):
    def __init__(self, serializer, task_name=None, bind=False):
        self.serializer = serializer
        if task_name is None:
            task_name = self.__class__.__name__
        self.task_name = task_name
        self.bind = bind

    def get_dataframe_loader(self, format, dataset, user):
        return make_dataframe_loader(format, dataset, user)

    def serialize_result(self, result, *args, **kwargs):
        return self.serializer.serialize(result, *args, **kwargs)

    def make_celery_task(self, app):
        task_maker = CeleryTasksMaker(
            self.task_name, self.bind, self.serializer)
        return task_maker.make_task(app, self)

    def load_pandas_and_schema(self, dataset, user, partition_filter=None,
                               enforce_schema_dtypes=True):
        loader = self.get_dataframe_loader('pandas', dataset, user)

        # Load schema data and convert it to an
        # :class:´SchemaResolver´ instance.
        schema = loader.load_schema()
        schema = SchemaResolver(schema)

        # Load only features inside schema.
        feature_names = schema.get_feature_names()
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

        return pandas, schema

    def get_task_id(self, task_obj):
        return task_obj.request.id.__str__()


class CeleryTasksMaker:
    def __init__(self, task_name, bind, serializer):
        self.task_name = task_name
        self.bind = bind
        self.serializer = serializer

    def _make_decorator(self, celery_app):
        return celery_app.task(name=self.task_name, bind=self.bind)

    def make_task(self, celery_app, task_obj):
        if not isinstance(task_obj, BaseTask):
            raise ValueError("`task_obj` is not an instance "
                             "from :class:`BaseTask`.")

        decorator = self._make_decorator(celery_app)
        if self.bind:
            @decorator
            def celery_task(self_task, *args, **kwargs):
                return task_obj.run(self_task, *args, **kwargs)

        else:
            @decorator
            def celery_task(*args, **kwargs):
                return task_obj.run(*args, **kwargs)

        return celery_task
