import itertools
from abc import ABCMeta, abstractmethod

import mlflow


class BaseLog(metaclass=ABCMeta):
    """Base class for Loggers.
    """

    @abstractmethod
    def log(self):
        pass


class MetricsLog(BaseLog):
    def __init__(self, name, values):
        self.name = name
        self.values = values

    def log(self):
        for i, val in enumerate(self.values):
            mlflow.log_metric(key=self.name, value=val, step=i)

    def __repr__(self):
        return '%s(%s)' % ('MetricLog', self.name)


class ModelLog(BaseLog):
    def __init__(self, name, model):
        self.name = name
        self.model = model

    def log(self):
        mlflow.sklearn.log_model(self.model, self.name)

    def __repr__(self):
        return '%s(%s)' % ('ModelLog', self.name)


class MlFlowLogger:

    def __init__(self):
        self.metrics = []
        self.models = []

    def save_metric(self, name, values):
        self.metrics.append(MetricsLog(name, values))

    def save_model(self, name, model):
        self.models.append(ModelLog(name, model))

    def log_all(self, run_id=None, experiment_id=None):
        logs = self._collect_all_logs()
        with mlflow.start_run(run_id=run_id, experiment_id=experiment_id):
            for log in logs:
                log.log()

    def _collect_all_logs(self):
        return itertools.chain(self.metrics, self.models)
