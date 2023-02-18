from abc import ABCMeta
from typing import Optional, Dict, Any

import celery

from . import serializers
from ..celery import app


class BaseTask(metaclass=ABCMeta):
    """Base class for Tasks.

    .. note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(
            self,
            serializer: serializers.DataFrameSerializer =
            serializers.PandasSerializer(),
            name: Optional[str] = None,
            **kwargs
    ):
        self.serializer = serializer
        self.name = name
        self.kwargs = kwargs

        self._celery_task_creator = CeleryTaskCreator(app)

    def serialize_result(self, result: Any) -> Dict:
        """Serializes result.

        Parameters
        ----------
        result : Any

        Returns
        -------
        serialized_result : dict
        """
        return self.serializer.serialize(result)

    def create_celery_task(self) -> celery.Task:
        """Creates celery task from object (self)

        Returns
        -------
        celery_task : celery.Task
        """
        return self._celery_task_creator.create_celery_task(
            self, self.name, **self.kwargs)

    def get_celery_uuid(self, celery_task):
        return celery_task.request.id.__str__()


class CeleryTaskCreator:
    def __init__(self, celery_app: celery.Celery):
        self.celery_app = celery_app

    def create_celery_task(
            self,
            task: BaseTask,
            name: Optional[str] = None,
            **kwargs
    ):
        if name is None:
            name = task.__class__.__name__

        decorator = self._make_decorator(name, **kwargs)

        @decorator
        def celery_task(*args, **kwargs):
            return task.run(*args, **kwargs)

        return celery_task

    def _make_decorator(self, name, **kwargs):
        return self.celery_app.task(name=name, **kwargs)
