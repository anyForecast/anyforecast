from mooncake.nn import SeqToSeq, TemporalFusionTransformer as TFT
from mooncake.helper import common_callbacks
from torch.optim.lr_scheduler import OneCycleLR


class EstimatorCreator:
    ESTIMATORS = {
        'seq2seq': SeqToSeq,
        'tft': TFT
    }

    def __init__(self, predictor, merger):
        self.predictor = predictor
        self.merger = merger

    def create_estimator(self):
        cls = self._get_estimator_class()
        estimator_args = self._get_estimator_args()
        return cls(**estimator_args)

    def _get_estimator_class(self):
        return self.ESTIMATORS[self.predictor.algorithm]

    def _get_estimator_args(self):
        args_creator = EstimatorArgsCreator(self.predictor, self.merger)
        return args_creator.get_estimator_args()


class EstimatorArgsCreator:
    def __init__(self, predictor, merger):
        self.predictor = predictor
        self.merger = merger

    def get_estimator_args(self):
        time_segmentation = self.merger.resolve_names()
        group_ids = self.merger.get_group_ids()
        max_prediction_length = self.predictor.forecast_horizon
        time_idx = 'time_idx'

        # Callbacks.
        lr_scheduler = dict(
            policy=OneCycleLR,
            step_every='batch',
            max_lr=1e-3,
            steps_per_epoch='iterations',
            epochs='max_epochs'
        )
        callbacks = common_callbacks(lr_scheduler, gradient_clipping=True)

        return {
            'group_ids': group_ids,
            'max_prediction_length': max_prediction_length,
            'time_idx': time_idx,
            'max_encoder_length': 20,
            'callbacks': callbacks,
            **time_segmentation
        }