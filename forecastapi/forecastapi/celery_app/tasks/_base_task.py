from abc import ABCMeta


class BaseTask(metaclass=ABCMeta):
    def __init__(self, serializer, task_name=None, bind=False):
        self.serializer = serializer
        if task_name is None:
            task_name = self.__class__.__name__
        self.task_name = task_name
        self.bind = bind

    def serialize_result(self, result, *args, **kwargs):
        return self.serializer.serialize(result, *args, **kwargs)

    def make_celery_task(self, app):
        task_maker = CeleryTasksMaker(
            self.task_name, self.bind, self.serializer)
        return task_maker.make_task(app, self)

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
