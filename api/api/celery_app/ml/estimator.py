from mooncake.nn import SeqToSeq, TemporalFusionTransformer as TFT


class EstimatorCreator:
    ESTIMATORS = {
        'seq2seq': SeqToSeq,
        'tft': TFT
    }

    def __init__(self, predictor):
        self.predictor = predictor

    def create_estimator(
            self, group_ids, target, time_varying_known_reals=None,
            time_varying_unknown_reals=None, static_categoricals=None,
            callbacks=None, time_idx='time_idx'
    ):
        cls = self._get_estimator_class()
        estimator_args = self._get_estimator_args(
            group_ids, target, time_varying_known_reals,
            time_varying_unknown_reals, static_categoricals,
            callbacks, time_idx
        )
        return cls(**estimator_args)

    def _get_estimator_class(self):
        return self.ESTIMATORS[self.predictor.algorithm]

    def _get_estimator_args(
            self, group_ids, target, time_varying_known_reals,
            time_varying_unknown_reals, static_categoricals,
            callbacks, time_idx
    ):
        args_creator = EstimatorArgsCreator(self.predictor)
        return args_creator.get_estimator_args(
            group_ids, target, time_varying_known_reals,
            time_varying_unknown_reals, static_categoricals,
            callbacks, time_idx
        )


class EstimatorArgsCreator:
    def __init__(self, predictor):
        self.predictor = predictor

    def get_estimator_args(
            self, group_ids, target, time_varying_known_reals,
            time_varying_unknown_reals, static_categoricals,
            callbacks, time_idx
    ):
        features_time_dependence = self._get_features_time_dependence(
            time_varying_known_reals, time_varying_unknown_reals,
            static_categoricals)
        max_prediction_length = self.predictor.forecast_horizon

        return {
            'target': target,
            'group_ids': group_ids,
            'time_idx': time_idx,
            'max_prediction_length': max_prediction_length,
            'max_encoder_length': 5,
            'callbacks': callbacks,
            'cv_split': 1,
            'max_epochs': 20,
            **features_time_dependence
        }

    def _get_features_time_dependence(self, time_varying_known_reals,
                                      time_varying_unknown_reals,
                                      static_categoricals):
        return {
            'time_varying_known_reals': time_varying_known_reals,
            'time_varying_unknown_reals': time_varying_unknown_reals,
            'static_categoricals': static_categoricals
        }
