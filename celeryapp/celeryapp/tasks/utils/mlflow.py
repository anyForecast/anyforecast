import mlflow


def infer_mlflow_signature(model_input, model_output=None):
    return mlflow.models.signature.infer_signature(
        model_input, model_output)


class MlFLowLog:
    """Base abstract class for MlFlow loggers.

    All derived classed must implement the `log()` method.

    .. note::
        This class is not meant to be used directly. Use derived classes
        instead.
    """

    def log(self):
        pass


class MetricsLog(MlFLowLog):
    def __init__(self, name, values):
        self.name = name
        self.values = values

    def log(self):
        for i, val in enumerate(self.values):
            mlflow.log_metric(key=self.name, value=val, step=i)


class ParametersLog(MlFLowLog):
    def __init__(self, params):
        self.params = params

    def log(self):
        mlflow.log_params(self.params)


class ModelLog(MlFLowLog):
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


class MlFlowLogger:

    def log_model(
            self, model, signature, use_sklearn_logger=True,
            artifact_path='model', conda_env=None
    ):
        ModelLog(model, conda_env, signature, use_sklearn_logger,
                 artifact_path).log()

    def log_metric_history(self, name, values):
        MetricsLog(name, values).log()

    def log_params(self, params):
        ParametersLog(params).log()
