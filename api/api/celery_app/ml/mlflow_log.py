from abc import ABCMeta, abstractmethod
from itertools import chain

import mlflow
import numpy as np
from mlflow.models.signature import infer_signature

from .wrappers import wrap_pipeline


class BaseMlFLowLog(metaclass=ABCMeta):
    """Base abstract class for MlFlow loggers.

    All derived classed must implement the `log()` method.

    .. note::
        This class is not meant to be used directly. Use derived classes
        instead.
    """

    @abstractmethod
    def log(self):
        pass


class MetricsLog(BaseMlFLowLog):
    def __init__(self, name, values):
        self.name = name
        self.values = values

    def log(self):
        for i, val in enumerate(self.values):
            mlflow.log_metric(key=self.name, value=val, step=i)


class ParametersLog(BaseMlFLowLog):
    def __init__(self, params):
        self.params = params

    def log(self):
        mlflow.log_params(self.params)


class PythonModelLog(BaseMlFLowLog):
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


class MlFlowLogsStorage:
    """Stores MlFlow log objects.
    """

    def __init__(self):
        log_keys = ['metrics', 'params', 'models']
        self.log_storage = dict.fromkeys(log_keys, [])

    def save_metric(self, name, values):
        key = 'metrics'
        log = MetricsLog(name, values)
        self.log_storage[key].append(log)

    def get_logs(self):
        return list(chain.from_iterable(self.log_storage.values()))

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
        key = 'models'
        log = PythonModelLog(name, python_model, conda_env, signature,
                             artifacts)
        self.log_storage[key].append(log)

    def save_params(self, params):
        key = 'params'
        log = ParametersLog(params)
        self.log_storage[key].append(log)


class MlFlowLogger:
    def __init__(self, model_pipeline, X):
        self.logs_storage = MlFlowLogsStorage()
        self.model_pipeline = model_pipeline
        self.X = X

    def log(self, run_id=None, experiment_id=None, run_name=None):
        self.save_metrics()
        self.save_params()
        self.save_pipeline()
        for log in self.logs_storage.get_logs():
            log.log()

    def save_metrics(self):
        metrics = ['train_loss', 'valid_loss']
        for metric in metrics:
            estimator = self.model_pipeline['estimator']
            history = self._get_metric_from_estimator(estimator, metric)
            self.logs_storage.save_metric(name=metric, values=history)

    def save_params(self):
        estimator = self.model_pipeline['estimator']
        params = self._get_estimator_params(estimator)
        self.logs_storage.save_params(params)

    def save_pipeline(self):
        # Save model with a signature that defines the schema of
        # the model's inputs and outputs. When the model is deployed, this
        # signature will be used to validate inputs.
        wrapped_pipeline = wrap_pipeline(self.model_pipeline)
        signature = infer_signature(self.X, wrapped_pipeline.predict(None, self.X))
        self.logs_storage.save_python_model(
            name='pipeline', python_model=wrapped_pipeline,
            signature=signature)

    def _get_metric_from_estimator(self, estimator, name):
        """Obtains history from estimator.

        A history is any collection values recorded during training under a
        given name. Histories can be error metrics, loss functions, flags, etc.
        For any fitted mooncake estimator, histories are stored inside the
        `history` attribute inside the skorch model, i.e., estimator.net_.history.

        Parameters
        ----------
        estimator : mooncake estimator
            Fitted blue meth estimator

        name : str
            Name of the metric

        Returns
        -------
        metric : list
        """
        if hasattr(estimator, 'estimators'):
            # Collect history in all estimators
            histories = []
            for est in estimator.estimators:
                histories.append(est.net_.history[:, name])

            # Get max length
            max_length = max(len(x) for x in histories)

            # Pad each history with nans at the end
            padded_histories = []
            for hist in histories:
                pad_hist = np.pad(
                    hist,
                    (0, max_length - len(hist)),
                    constant_values=np.nan
                )
                padded_histories.append(pad_hist)

            # Return mean of all histories
            avg_histories = np.nanmean(padded_histories, axis=0)
            return avg_histories

        history = estimator.net_.history[:, name]
        return history

    def _get_estimator_params(self, estimator):
        """Private function for obtaining estimator params.
        """
        allowed_types = (int, str, float, list, tuple)

        params = {}
        for k, v in estimator.get_params().items():
            if isinstance(v, allowed_types) or v is None:
                params[k] = v
            else:
                params[k] = v.__name__
        return params
