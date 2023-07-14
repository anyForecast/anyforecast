from abc import ABC, abstractmethod
from typing import (
    Optional,
    Dict,
    Any,
    Callable
)

import celery
import pandas as pd

from . import serializers, celeryapp


class AnyForecastTask(ABC):
    """Base class for anyForecast tasks.

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

        self._celery_task_creator = CeleryTaskCreator(celeryapp.app)

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

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

    def create_celery_task(self) -> Callable:
        """Factory for celery tasks objects.

        Returns
        -------
        celery_task : celery.Task
        """
        return self._celery_task_creator.create_celery_task(
            self, self.name, **self.kwargs)


class CeleryTaskCreator:
    """Celery tasks creator.

    Parameters
    ----------
    celery_app : celery.Celery
        Celery app.
    """

    def __init__(self, celery_app: celery.Celery):
        self.celery_app = celery_app

    def create_celery_task(
            self,
            task: AnyForecastTask,
            name: Optional[str] = None,
            **kwargs
    ) -> Callable:
        """Creates Celery task from anyForecast task.

        Parameters
        ----------
        task : AnyForecastTask
            anyForecast task object.

        name : str, default=None
            Celery task name. If None, ``task`` class name is used.
        """
        if name is None:
            name = task.__class__.__name__

        decorator = self.make_decorator(name, **kwargs)

        @decorator
        def celery_task(*args, **kwargs):
            return task.run(*args, **kwargs)

        return celery_task

    def make_decorator(self, name, **kwargs):
        return self.celery_app.task(name=name, **kwargs)


class Train(AnyForecastTask):
    def __init__(self):
        super().__init__(ignore_result=True)

    def run(
            self,
            data: pd.DataFrame,
            ts_dataset_args: Dict,
            forecast_args: Dict
    ):
        pass
