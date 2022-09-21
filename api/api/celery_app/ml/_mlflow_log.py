from abc import ABCMeta, abstractmethod
from itertools import chain

import mlflow
import numpy as np


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


class ModelLog(BaseMlFLowLog):
    def __init__(self, model, conda_env, signature,
                 use_sklearn_logger=False, artifact_path='model'):
        self.model = model
        self.conda_env = conda_env
        self.signature = signature
        self.use_sklearn_logger = use_sklearn_logger
        self.artifact_path = artifact_path

    def log(self):
        logger = mlflow.sklearn if self.use_sklearn_logger else mlflow.pyfunc
        kwargs = self._create_kwargs()
        logger.log_model(**kwargs)

    def _create_kwargs(self):
        kwargs = {
            'artifact_path': self.artifact_path,
            'signature': self.signature,
            'conda_env': self.conda_env
        }
        if self.use_sklearn_logger:
            kwargs['sk_model'] = self.model
        else:
            kwargs['python_model'] = self.model

        return kwargs


class MlFlowLogsStorage:
    """Stores MlFlow log objects.
    """

    def __init__(self):
        self._log_storage = dict.fromkeys(('metrics', 'models', 'params'), [])

    def store_metric(self, name, values):
        log = MetricsLog(name, values)
        self._log_storage['metrics'].append(log)

    def get_logs(self):
        return self._log_storage

    def get_chained_logs(self):
        return list(chain.from_iterable(self._log_storage.values()))

    def store_model(self, model, conda_env=None, signature=None,
                    use_sklearn_logger=False):
        """Stores Python model.

        Parameters
        ----------
        model :  An instance of a subclass of :class:`~PythonModel`.

        conda_env: {{ conda_env }}

        signature: :py:class:`ModelSignature <mlflow.models.ModelSignature>`
            Describes model input and output
            :py:class:`Schema <mlflow.types.Schema>`.
        """
        log = ModelLog(model, conda_env, signature, use_sklearn_logger)
        self._log_storage['models'].append(log)

    def store_params(self, params):
        log = ParametersLog(params)
        self._log_storage['params'].append(log)


class MlFlowLogger:
    def __init__(self, model, use_sklearn_logger=False):
        self.model = model
        self.use_sklearn_logger = use_sklearn_logger
        self._logs_storage = MlFlowLogsStorage()

    def log(self, **kwargs):
        self._store_metrics()
        self._store_params(**kwargs)
        self._store_model()
        for log in self._logs_storage.get_chained_logs():
            log.log()

    def _store_metrics(self):
        metrics = ['train_loss'] #, 'valid_loss']
        for metric in metrics:
            estimator = self.model.estimator
            history = self._get_metric_from_estimator(estimator, metric)
            self._logs_storage.store_metric(name=metric, values=history)

    def _store_params(self, **kwargs):
        estimator = self.model.estimator
        params = self._get_estimator_params(estimator)
        params.update(kwargs)
        self._logs_storage.store_params(params)

    def _store_model(self):
        # TODO: Save model with a signature that defines the schema of
        #  the model's inputs and outputs. When the model is deployed, this
        #  signature will be used to validate inputs.
        self._logs_storage.store_model(
            model=self.model,
            use_sklearn_logger=self.use_sklearn_logger
            )

    def _get_metric_from_estimator(self, estimator, name):
        """Obtains history from estimator.

        A history is any collection values recorded during training under a
        given name. Histories can be error metrics, loss functions, flags, etc.
        For any fitted mooncake estimator, histories are stored inside the
        `history` attribute inside the skorch model, i.e.,
        estimator.net_.history.

        Parameters
        ----------
        estimator : Estimator
            Fitted estimator.

        name : str
            Name of the metric.

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
            if k == 'callbacks':
                for name, obj in v:
                    params[name] = obj.__class__.__name__
            elif isinstance(v, allowed_types) or v is None:
                params[k] = v
            else:
                params[k] = v.__name__
        return params
