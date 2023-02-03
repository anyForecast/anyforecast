import numpy as np
from skorch.callbacks import LRScheduler
from skorch_forecasting import nn
from torch.optim.lr_scheduler import OneCycleLR

from ._base_trainer import BaseTrainer
from .._transformers import FeatureNamesTransformer


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

    def default_lr_scheduler(self):
        lr_scheduler = {
            'policy': OneCycleLR,
            'step_every': 'batch',
            'max_lr': 1e-3,
            'steps_per_epoch': 'iterations',
            'epochs': 'max_epochs'
        }
        return LRScheduler(**lr_scheduler)

    def default_callbacks(self):
        """Default callbacks.
        """
        pass

    def create_args(
            self, group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals, max_prediction_length,
            max_encoder_length=None, callbacks=None, hparams=None
    ):
        if callbacks is None:
            callbacks = self.default_callbacks()

        if max_encoder_length is None:
            max_encoder_length = 2 * max_prediction_length

        if hparams is None:
            hparams = {}

        time_varying_unknown_reals = time_varying_unknown + [target]

        return {
            'group_ids': group_ids,
            'time_idx': timestamp,
            'target': target,
            'time_varying_known_reals': time_varying_known,
            'time_varying_unknown_reals': time_varying_unknown_reals,
            'static_categoricals': static_categoricals,
            'max_prediction_length': max_prediction_length,
            'max_encoder_length': max_encoder_length,
            'callbacks': callbacks,
            'cv_split': None,
            **hparams
        }


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


class SkorchForecastingEstimatorCreator:

    def __init__(
            self, algorithm, group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals, max_prediction_length,
            max_encoder_length=None, callbacks=None, hparams=None
    ):
        self.algorithm = algorithm
        self.group_ids = group_ids
        self.timestamp = timestamp
        self.target = target
        self.time_varying_known = time_varying_known
        self.time_varying_unknown = time_varying_unknown
        self.static_categoricals = static_categoricals
        self.max_prediction_length = max_prediction_length
        self.max_encoder_length = max_encoder_length
        self.callbacks = callbacks
        self.hparams = hparams

    def create_estimator(self):
        cls = self._get_estimator_class()
        estimator_args = self._get_estimator_args()
        return cls(**estimator_args)

    def _get_estimator_class(self):
        return getattr(nn, self.algorithm)

    def _get_estimator_args(self):
        return SkorchForecastingArgsCreator().create_args(
            self.group_ids, self.timestamp, self.target,
            self.time_varying_known, self.time_varying_unknown,
            self.static_categoricals, self.max_prediction_length,
            self.max_encoder_length, self.callbacks, self.hparams
        )

    @classmethod
    def from_feature_names(
            cls, feature_names, transform_feature_names=True,
            preprocessor=None, **kwargs
    ):
        if transform_feature_names:
            feature_names = FeatureNamesTransformer(
                preprocessor).transform(feature_names)

        return cls(**feature_names, **kwargs)


class TrainSkorchForecasting(BaseTrainer):
    """Preprocess and fits timeseries data.
    """

    def __init__(self):
        super().__init__()
        self._estimator_creator = SkorchForecastingEstimatorCreator

    def run(self, bind, data, trainer):
        X = data['DataFrame']
        features_schema = data.get('FeaturesSchema')
        feature_names = features_schema.get_names('all')

        # Currently, MLFlow does not support category data type when inferring
        # the signature (https://github.com/mlflow/mlflow/issues/3849).
        X = _category_to_object(X)

        # Create preprocessor.
        preprocessor = self.create_preprocessor(
            group_ids=feature_names['group_ids'],
            timestamp=feature_names['timestamp'],
            target=feature_names['target'], freq=trainer.pop('freq'),
            preprocessing_data=trainer.pop('preprocessing', None)
        )

        # Create estimator
        preprocessor.fit(X)
        estimator_creator = self._estimator_creator.from_feature_names(
            feature_names, preprocessor=preprocessor, **trainer)
        estimator = estimator_creator.create_estimator()

        self.fit(X, preprocessor, estimator)

    def get_metrics(self, model):
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
