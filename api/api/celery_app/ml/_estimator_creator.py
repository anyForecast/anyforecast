from skorch.callbacks import LRScheduler, GradientNormClipping, EarlyStopping
from skorch_forecasting.nn import SeqToSeq, TemporalFusionTransformer as TFT
from torch.optim.lr_scheduler import OneCycleLR

from ..exceptions import UnknownAlgorithmError


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
            callbacks=None, time_idx='time_index'
    ):
        cls = self._get_estimator_class()
        estimator_args = self._get_estimator_args(
            group_ids, target, time_varying_known,
            time_varying_unknown, static_categoricals,
            callbacks, time_idx
        )
        return cls(**estimator_args)

    def _get_estimator_class(self):
        algorithm = self.predictor['algorithm']
        try:
            return self.ESTIMATORS[algorithm]
        except KeyError:
            raise UnknownAlgorithmError(algorithm=algorithm)

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

    def default_callbacks(self, lr_scheduler=None, early_stopping=False,
                         gradient_clipping=False, patience=5):
        """Helper for constructing callbacks

        Parameters
        ----------
        lr_scheduler : dict
            Dictionary containing all the configuration for the chosen learning
            rate scheduler. This includes the `policy` (torch lr_scheduler
            class name, i.e., 'OneCycleLR'), `step_every` (when the scheduler
            takes a step, can be either 'epoch' or 'batch) and every parameter
            for the torch scheduler to be instantiated correctly.

        early_stopping : bool, default=False
            Callback for stopping training when scores don’t improve.
            Thi callback stops training early if a specified the valid loss did
            not improve in patience number of epochs.

        gradient_clipping : bool, default=False
            Clips gradient norm of a module’s parameters.

        patience : int
            Number of epochs to wait for improvement of the monitor value until
            the training process is stopped. This parameter is ignored if
            ``early_stopping`` is False.

        Returns
        -------
        list of tuples
            List of tuples of the form (name, obj)
        """
        callbacks = []
        if lr_scheduler is not None:
            name = 'lr_scheduler'
            obj = LRScheduler(**lr_scheduler)
            callbacks.append((name, obj))
        if early_stopping:
            name = 'early_stopping'
            obj = EarlyStopping(patience=patience)
            callbacks.append((name, obj))
        if gradient_clipping:
            name = 'gradient_clipping'
            obj = GradientNormClipping(1)
            callbacks.append((name, obj))
        return callbacks

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
            callbacks = self.default_callbacks(
                lr_scheduler,
                early_stopping=False,
                gradient_clipping=True,
                patience=100
            )

        kwargs = self.predictor['kwargs']
        if kwargs is None:
            kwargs = {}
        print(f'Extra kwargs: {kwargs}')

        return {
            'target': target[0],
            'group_ids': group_ids,
            'time_idx': time_idx,
            'time_varying_known_reals': time_varying_known,
            'time_varying_unknown_reals': time_varying_unknown + target,
            'static_categoricals': static_categoricals,
            'max_prediction_length': self.predictor['forecast_horizon'],
            'max_encoder_length': 26,
            'callbacks': callbacks,
            'cv_split': None,
            'max_epochs': 250,
            'hidden_size': 64,
            'batch_size': 64,
            **kwargs
        }
