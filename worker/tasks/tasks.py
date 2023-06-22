import importlib
from abc import ABCMeta
from typing import Optional, Dict, Any, TypedDict

import pandas as pd

import celery
from . import serializers
from .celery import app


class BaseTask(metaclass=ABCMeta):
    """Base class for Tasks.

    Use :meth:`create_celery_task` to create Celery tasks.

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

        decorator = self.make_decorator(name, **kwargs)

        @decorator
        def celery_task(*args, **kwargs):
            return task.run(*args, **kwargs)

        return celery_task

    def make_decorator(self, name, **kwargs):
        return self.celery_app.task(name=name, **kwargs)


class ForecastArgs(TypedDict):
    """Time series neuralmodels args.
    """
    algorithm: str
    freq: str
    horizon: int


class DatasetArgs(TypedDict):
    group_ids: list[str]
    timestamp: str
    target: str
    freq: str


class TimeseriesDatasetArgs(DatasetArgs):
    time_varying_known: list[str]
    time_varying_unknown: list[str]
    static_categoricals: list[str]


class Train(BaseTask):
    def __init__(self):
        super().__init__(ignore_result=True)

    def run(self,
            data: pd.DataFrame,
            ts_dataset_args: TimeseriesDatasetArgs,
            forecast_args: ForecastArgs
            ):

        algo = forecast_args['algorithm']
        cls = importlib.import_module(f"algorithm.{algo}")
        cls(ts_dataset_args, forecast_args)


