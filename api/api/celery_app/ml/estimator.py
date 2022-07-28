from mooncake.nn import SeqToSeq, TemporalFusionTransformer as TFT
from mooncake.helper import common_callbacks
from torch.optim.lr_scheduler import OneCycleLR


class EstimatorCreator:

    ESTIMATORS = {
        'seq2seq': SeqToSeq,
        'tft': TFT
    }

    def __init__(self, predictor):
        self.predictor = predictor

    def create_estimator(
            self, group_ids, target, time_varying_known=None,
            time_varying_unknown=None, static_categoricals=None,
            callbacks=None, time_idx='time_idx'
    ):
        cls = self._get_estimator_class()
        estimator_args = self._get_estimator_args(
            group_ids, target, time_varying_known,
            time_varying_unknown, static_categoricals,
            callbacks, time_idx
        )
        return cls(**estimator_args)

    def _get_estimator_class(self):
        return self.ESTIMATORS[self.predictor['algorithm']]

    def _get_estimator_args(
            self, group_ids, target, time_varying_known,
            time_varying_unknown, static_categoricals,
            callbacks, time_idx
    ):
        args_creator = EstimatorArgsCreator(self.predictor)
        return args_creator.get_estimator_args(
            group_ids, target, time_varying_known,
            time_varying_unknown, static_categoricals,
            callbacks, time_idx
        )


class EstimatorArgsCreator:
    def __init__(self, predictor):
        self.predictor = predictor

    def get_estimator_args(
            self, group_ids, target, time_varying_known,
            time_varying_unknown, static_categoricals,
            callbacks, time_idx
    ):
        lr_scheduler = {
            'policy': OneCycleLR,
            'step_every': 'batch',
            'max_lr': 1e-3,
            'steps_per_epoch': 'iterations',
            'epochs': 'max_epochs'
        }
        if callbacks is None:
            callbacks = common_callbacks(lr_scheduler, early_stopping=True,
                                         gradient_clipping=True, patience=100)

        return {
            'target': target[0],
            'group_ids': group_ids,
            'time_idx': time_idx,
            'time_varying_known_reals': time_varying_known,
            'time_varying_unknown_reals': time_varying_unknown + target,
            'static_categoricals': static_categoricals,
            'max_prediction_length': self.predictor['forecast_horizon'],
            'max_encoder_length': 10,
            'callbacks': callbacks,
            'cv_split': 4,
            'max_epochs': 100,
        }
