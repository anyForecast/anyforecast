import mlflow
from mooncake.nn import SeqToSeq


def wrap_pipeline(pipeline):
    """Factory function for model wrappers.
    """
    wrappers = {
        SeqToSeq: Seq2SeqModelWrapper
    }
    return wrappers[type(pipeline['estimator'])](pipeline)


class Seq2SeqModelWrapper(mlflow.pyfunc.PythonModel):
    """Wrapper for Seq2Seq model.
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def predict(self, context, model_input):
        preprocessor = self.pipeline['preprocessor']
        predicted_df = self.pipeline.predict(model_input, raw=False)
        inv_predicted_df = preprocessor.inverse_transform(predicted_df)
        return inv_predicted_df
