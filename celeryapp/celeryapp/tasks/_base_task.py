from abc import ABCMeta

from .serializers import PandasSerializer


class BaseTask(metaclass=ABCMeta):

    def __init__(
            self,
            # TODO: Create a DefaultSerializer
            serializer=PandasSerializer(),
            name=None,
            bind=False,
            **kwargs
    ):
        self.serializer = serializer
        self.name = name
        self.bind = bind
        self.kwargs = kwargs

    def serialize_result(self, result):
        return self.serializer.serialize(result)

    def make_celery_task(self, celery_app):
        return CeleryTaskMaker().make_celery_task(
            celery_app=celery_app, task=self, name=self.name, bind=self.bind,
            **self.kwargs)

    def get_celery_uuid(self, celery_task):
        return celery_task.request.id.__str__()


class CeleryTaskMaker:

    def make_celery_task(
            self, celery_app, task, name=None, bind=False, **kwargs
    ):
        if name is None:
            name = task.__class__.__name__

        decorator = self._make_decorator(celery_app, name, bind, **kwargs)

        @decorator
        def celery_task(self_task, *args, **kwargs):
            return task.run(self_task, *args, **kwargs)

        return celery_task

    def _make_decorator(self, celery_app, name, bind, **kwargs):
        return celery_app.task(name=name, bind=bind, **kwargs)
