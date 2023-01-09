import mlflow
import numpy as np
from mlflow.models.signature import infer_signature
from skorch_forecasting.model_wrappers import PreprocessorEstimatorWrapper

from ._base_trainer import BaseTrainer
from ._preprocessor_creators import TimeseriesPreprocessorCreator
from .._transformers import FeatureNamesTransformer


class SkorchForecastingHistory:
    """Retrieves histories recorded during training.

    A history is any collection of values recorded during training under a
    given name. Histories can be error metrics, loss functions, flags, etc.
    For any fitted skorch-forecasting estimator, histories are stored inside the
    `history` attribute inside the skorch model, i.e.,
    estimator.net_.history.

    Parameters
    ----------
    model : skorch-forecasting Estimator
    """

    def __init__(self, model):
        self.model = model

    def _is_ensemble(self):
        if isinstance(self.model, list):
            return True
        return False

    def _get_metric_from_history(self, name, history, step_every):
        if step_every == 'epoch':
            metric = history[:, name]

        elif step_every == 'batch':
            metric = []
            for epoch in history:
                for batch in epoch['batches']:
                    val = batch[name]
                    metric.append(val)

        else:
            raise ValueError("`step_every` can be either 'epoch' or 'batch'.")

        return metric

    def get(self, name, step_every='epoch'):
        """Retrieves model history.

        Parameters
        ----------
        name : str
            Name of the history.

        step_every : str {epoch, batch}, default='epoch'

        Returns
        -------
        history : list
        """
        if self._is_ensemble():

            # Collect all metric histories from :class:`History` instance, which
            # holds all metric values recorded during training.
            histories = []
            for est in self.model.estimators:
                metric = self._get_metric_from_history(
                    name, est.net_.history, step_every)
                histories.append(metric)

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

        return self._get_metric_from_history(
            name, self.model.net_.history, step_every)


def _category_to_object(pandas_df):
    """Private function that casts category columns to object.

    Parameters
    ----------
    pandas_df

    Returns
    -------
    pandas_df
    """
    dtypes = pandas_df.dtypes
    for name, dtype in dtypes.items():
        if dtype.name == 'category':
            pandas_df[name] = pandas_df[name].astype(object)
    return pandas_df


class SkorchForecastingTrainer(BaseTrainer):
    """Preprocess and fits timeseries data.
    """

    def __init__(self):
        super().__init__()
        self._estimator_creator = self.make_estimator_creator(
            'skorch_forecasting')

    def run(self, bind, data, trainer):
        X = data['X']
        schema = data['schema']

        # Currently, MLFlow does not support category data type when inferring
        # the signature (https://github.com/mlflow/mlflow/issues/3849).
        X = _category_to_object(X)
        signature = infer_signature(model_input=X)

        # Create preprocessor.
        freq = trainer.pop('freq')
        preprocessor_creator = TimeseriesPreprocessorCreator.from_schema(schema)
        preprocessor = preprocessor_creator.create_preprocessor(freq=freq)

        with mlflow.start_run():
            preprocessor.fit(X)

            # Get transformed feature names.
            feature_names = schema.get_names_for('all')
            feature_names = FeatureNamesTransformer(preprocessor).transform(
                feature_names)

            # Fit model.
            args = self._make_estimator_args(**trainer, **feature_names)
            estimator = self._estimator_creator.create_estimator(**args)
            wrapper = PreprocessorEstimatorWrapper(estimator, preprocessor)
            wrapper.fit(X)

            # Log to mlflow.
            metrics = self._get_model_histories(wrapper.estimator)
            self.log_mlflow(wrapper, signature, metrics, {})

    def _get_model_histories(self, model):
        model_hist = SkorchForecastingHistory(model)

        # Tuples of the form (name, step_every) where ``name`` is the history
        # name and ``step_every`` indicates whether the history was recorded
        # every epoch or every batch.
        search_for = [
            ('train_loss', 'epoch'),
            ('valid_loss', 'epoch'),
            ('event_lr', 'epoch'),
            ('event_lr', 'batch')
        ]

        histories = {}
        for name, step_every in search_for:
            try:
                hist = model_hist.get(name, step_every)
            except KeyError:
                continue
            histories[name] = hist
        return histories

    def _make_estimator_args(
            self, algorithm, group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals, max_prediction_length,
            callbacks=None, hparams=None
    ):
        return {
            'algorithm': algorithm,
            'group_ids': group_ids,
            'timestamp': timestamp,
            'target': target,
            'time_varying_known': time_varying_known,
            'time_varying_unknown': time_varying_unknown,
            'static_categoricals': static_categoricals,
            'max_prediction_length': max_prediction_length,
            'callbacks': callbacks,
            'hparams': hparams
        }
