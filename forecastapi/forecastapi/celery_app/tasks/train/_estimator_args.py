from skorch.callbacks import LRScheduler
from torch.optim.lr_scheduler import OneCycleLR


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

        return {
            'group_ids': group_ids,
            'time_idx': timestamp,
            'target': target,
            'time_varying_known_reals': time_varying_known,
            'time_varying_unknown_reals': time_varying_unknown + [target],
            'static_categoricals': static_categoricals,
            'max_prediction_length': max_prediction_length,
            'max_encoder_length': max_encoder_length,
            'callbacks': callbacks,
            'cv_split': None,
            **hparams
        }
