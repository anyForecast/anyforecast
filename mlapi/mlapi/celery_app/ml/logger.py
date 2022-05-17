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


class ParametersLog(BaseLog):
    def __init__(self, params):
        self.params = params

    def log(self):
        mlflow.log_params(self.params)


class PythonModelLog(BaseLog):
    def __init__(self, name, python_model, conda_env, signature, artifacts):
        self.name = name
        self.python_model = python_model
        self.conda_env = conda_env
        self.signature = signature
        self.artifacts = artifacts

    def log(self):
        mlflow.pyfunc.log_model(
            self.name, python_model=self.python_model,
            conda_env=self.conda_env, signature=self.signature,
            artifacts=self.artifacts)


class MlFlowLogger:

    def __init__(self):
        self.metrics = []
        self.models = []
        self.params = []

    def save_metric(self, name, values):
        self.metrics.append(MetricsLog(name, values))

    def save_python_model(self, name, python_model, conda_env=None,
                          signature=None, artifacts=None):

        """Saves python model.

        For more reference on what mlflow refers to as a "PythonModel" please
        visit:
        - https://www.mlflow.org/docs/latest/python_api/mlflow.pyfunc.html
        - :class:`mlflow.pyfunc.model.PythonModel`.

        Parameters
        ----------
        name : str
            The run-relative artifact path to which to log the Python model.

        python_model :  An instance of a subclass of :class:`~PythonModel`.

        conda_env: {{ conda_env }}

        signature: :py:class:`ModelSignature <mlflow.models.ModelSignature>`
            Describes model input and output
            :py:class:`Schema <mlflow.types.Schema>`.

        artifacts : dict, str -> str.
             A dictionary containing ``<name, artifact_uri>`` entries.

        References
        ----------
        See docstring from `mlflow.pyfunc.log_model`.
        """
        self.models.append(
            PythonModelLog(name, python_model, conda_env,
                           signature, artifacts))

    def save_params(self, params):
        self.params.append(ParametersLog(params))

    def log_all(self, run_id=None, experiment_id=None, run_name=None):
        logs = self._collect_all_logs()
        with mlflow.start_run(run_id=run_id, experiment_id=experiment_id,
                              run_name=run_name):
            for log in logs:
                log.log()

    def _collect_all_logs(self):
        return itertools.chain(self.metrics, self.models, self.params)
