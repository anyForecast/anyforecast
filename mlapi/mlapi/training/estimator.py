from mooncake.nn import SeqToSeq, TemporalFusionTransformer as TFT

ESTIMATORS = {
    'seq2seq': SeqToSeq,
    'tft': TFT
}


class EstimatorCreator:
    def __init__(self, predictor, target_dataset):
        self.predictor = predictor
        self.target_dataset = target_dataset

    def create_estimator(self):
        cls = self._get_estimator_class()
        estimator_args = self._get_estimator_args()
        return cls(**estimator_args)

    def _get_estimator_class(self):
        return ESTIMATORS[self.predictor.algorithm]

    def _get_estimator_args(self):
        pass


class EstimatorArgsCreator:
    def __init__(self, predictor, target_dataset):
        self.predictor = predictor
        self.target_dataset = target_dataset

    def get_estimator_args(self):
        pass
