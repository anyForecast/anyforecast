from typing import Tuple, Dict, List

import numpy as np
import pandas as pd
import skorch_forecasting
from torch.optim import lr_scheduler

from .base_trainer import BaseTrainer
from ...dataloaders import FeaturesSchema


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


class SkorchForecastingArgsCreator:

    def __init__(
            self, group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals
    ):
        self.group_ids = group_ids
        self.timestamp = timestamp
        self.target = target
        self.time_varying_known = time_varying_known
        self.time_varying_unknown = time_varying_unknown
        self.static_categoricals = static_categoricals

    def default_lr_scheduler(self):
        return {
            'policy': lr_scheduler.OneCycleLR,
            'step_every': 'batch',
            'max_lr': 1e-3,
            'steps_per_epoch': 'iterations',
            'epochs': 'max_epochs'
        }

    def default_callbacks(self):
        """Default callbacks.
        """
        pass

    def create_args(self, max_prediction_length, callbacks=None, hparams=None):
        if callbacks is None:
            callbacks = self.default_callbacks()

        if hparams is None:
            hparams = {}

        max_encoder_length = hparams.get(
            'max_encoder_length', max_prediction_length)

        time_varying_unknown = self.time_varying_unknown + [self.target]

        return {
            'group_ids': self.group_ids,
            'time_idx': self.timestamp,
            'target': self.target,
            'time_varying_known_reals': self.time_varying_known,
            'time_varying_unknown_reals': time_varying_unknown,
            'static_categoricals': self.static_categoricals,
            'max_prediction_length': max_prediction_length,
            'max_encoder_length': max_encoder_length,
            'callbacks': callbacks,
            'cv_split': None,
            **hparams
        }


class SkorchForecastingCreator:
    """Creates skorch-forecasting neural net estimators.

    Parameters
    ----------
    group_ids : list of str
        List of column names identifying a time series. This means that the
        ``group_ids`` identify a sample together with ``date``. If you
        have only one times series, set this to the name of column that is
        constant.

    timestamp : str
        Time index column.

    target : str
        Target column.

    time_varying_known : list of str
        List of continuous variables that change over time and are known in the
        future (e.g. price of a product, but not demand of a product).

    time_varying_unknown : list of str
        List of continuous variables that change over time and are not known in
        the future. You might want to include your ``target`` here.

    static_categoricals : list of str
        List of categorical variables that do not change over time (also known
        as `time independent variables`). You might want to include your
        ``group_ids`` here for the learning algorithm to distinguish between
        different time series.

    """

    def __init__(self, group_ids, timestamp, target, time_varying_known,
                 time_varying_unknown, static_categoricals):
        self._args_creator = SkorchForecastingArgsCreator(
            group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals)

    def get_estimator_cls(self, algorithm):
        return getattr(skorch_forecasting.nn, algorithm)

    def create_estimator(
            self, algorithm: str, max_prediction_length: int, callbacks=List,
            hparams=Dict):

        """Creates estimator.

        Parameters
        ----------
        algorithm : str
        max_prediction_length : int
        callbacks: list
        hparams: dict

        Returns
        -------
        skorch-forecasting estimator
        """
        args = self._args_creator.create_args(
            max_prediction_length, callbacks, hparams)
        estimator_cls = self.get_estimator_cls(algorithm)
        return estimator_cls(**args)

    @classmethod
    def from_schema(cls, schema: FeaturesSchema):
        """From schema factory.

        Parameters
        ----------
        schema : FeaturesSchema

        Returns
        -------
        SkorchForecastingCreator
        """
        return cls(**vars(schema))


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


class SkorchForecastingTrainer(BaseTrainer):
    """Trains skorch forecasting estimator.
    """

    def __init__(self):
        super().__init__(estimator_creator=SkorchForecastingCreator)

    def run(self, data_and_schema: Tuple[pd.DataFrame, FeaturesSchema],
            trainer: Dict) -> None:
        """Runs training task.

        Parameters
        ----------
        data_and_schema : tuple
            Tuple of (pd.DataFrame, FeatureSchema)

        trainer : dict
            Training settings.
        """
        data, schema = data_and_schema

        # Currently, MLFlow does not support category data type when inferring
        # the signature (https://github.com/mlflow/mlflow/issues/3849).
        data = _category_to_object(data)

        # Create preprocessor.
        preprocessor = self.create_timeseries_preprocessor(
            group_ids=schema.group_ids, timestamp=schema.timestamp,
            target=schema.target, freq=trainer.pop('freq'),
            preprocessing_data=trainer.pop('preprocessing'))

        # Update schema with transformed names.
        preprocessor.fit(data)
        schema = self.update_schema(schema, preprocessor)

        # Create estimator and fit.
        estimator = self.create_estimator_from_schema(schema, **trainer)
        self.fit(data, preprocessor, estimator)

    def get_params(self, estimator) -> Dict:
        allowed_types = (int, str, float, list, tuple)
        params = {}
        for k, v in estimator.get_params().items():
            if k.startswith('_'):
                continue

            elif isinstance(v, allowed_types) or v is None:
                params[k] = v

            else:
                params[k] = v.__name__

        return params

    def get_metrics(self, estimator) -> Dict:
        model_hist = SkorchForecastingHistory(estimator)

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
