import mlflow


def _get_class_name(obj):
    return obj.__class__.__name__


def wrap_pipeline(pipeline):
    """Factory function for model wrappers.
    """
    cls_name = _get_class_name(pipeline['estimator'])
    wrapper_catalog = {
        'SeqToSeq': Seq2SeqModelWrapper
    }
    return wrapper_catalog[cls_name](pipeline)


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
