from abc import ABCMeta

from ..serialize.pandas import PandasNativeSerializer


class BaseTask(metaclass=ABCMeta):

    def __init__(
            self,
            # TODO: Create a DefaultSerializer
            serializer=PandasNativeSerializer(),
            bind=False
    ):
        self.serializer = serializer
        self.bind = bind

    def serialize_result(self, result, *args, **kwargs):
        return self.serializer.serialize(result, *args, **kwargs)

    def make_celery_task(self, celery_app):
        return CeleryTaskMaker().make_task(celery_app, self, self.bind)

    def get_task_id(self, task_obj):
        return task_obj.request.id.__str__()

    def is_bounded(self):
        return self.bind


class CeleryTaskMaker:

    def _make_decorator(self, celery_app, name, bind):
        return celery_app.task(name=name, bind=bind)

    def make_task(self, celery_app, task_obj, bind):
        if not isinstance(task_obj, BaseTask):
            raise ValueError("`task_obj` is not an instance "
                             "from :class:`BaseTask`.")

        decorator = self._make_decorator(
            celery_app,
            task_obj.__class__.__name__,
            bind
        )

        if bind:
            @decorator
            def celery_task(self_task, *args, **kwargs):
                return task_obj.run(self_task, *args, **kwargs)

        else:
            @decorator
            def celery_task(*args, **kwargs):
                return task_obj.run(*args, **kwargs)

        return celery_task
