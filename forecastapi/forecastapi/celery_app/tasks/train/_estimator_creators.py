from skorch_forecasting import nn

from ._estimator_args import SkorchForecastingArgsCreator


def make_estimator_creator(name):
    estimator_creators = {
        'skorch_forecasting': SkorchForecastingCreator
    }
    return estimator_creators[name]()


class SkorchForecastingCreator:

    def __init__(self):
        self._args_creator = SkorchForecastingArgsCreator()

    def create_estimator(
            self,
            algorithm,
            group_ids,
            timestamp,
            target,
            time_varying_known,
            time_varying_unknown,
            static_categoricals,
            max_prediction_length,
            max_encoder_length=None,
            callbacks=None,
            hparams=None
    ):
        cls = self._get_estimator_class(algorithm)
        estimator_args = self._get_estimator_args(
            group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals, max_prediction_length,
            max_encoder_length, callbacks, hparams)
        return cls(**estimator_args)

    def _get_estimator_class(self, algorithm):
        return getattr(nn, algorithm)

    def _get_estimator_args(
            self, group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals, max_prediction_length,
            max_encoder_length, callbacks, hparams):
        return self._args_creator.create_args(
            group_ids, timestamp, target, time_varying_known,
            time_varying_unknown, static_categoricals, max_prediction_length,
            max_encoder_length, callbacks, hparams)
