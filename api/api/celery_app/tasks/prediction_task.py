import mlflow

from ._pandas_loader import PandasLoader
from .base_task import BaseTask
from ..serialize import PandasSerializer


class PredictionTask(BaseTask):

    def __init__(self):
        super().__init__(
            serializer=PandasSerializer(serializer_model='to_records'),
            task_name='prediction_task',
            bind=False
        )

    def run(self, estimator, dataset, user):
        # Load data to predict.
        python_estimator = self._load_python_estimator(estimator)
        pandas_loader = PandasLoader(dataset, user)
        X, schema_resolver = pandas_loader.load_pandas(
            return_schema_resolver=True)

        # Create prediction.
        output = self._predict(X, python_estimator)

        # Return serialized output.
        return self.serialize_result({'data': output})

    def _create_result(self, y_hat, group_ids, timestamp_column):
        return {
            'data': y_hat,
            'group_ids': group_ids,
            'timestamp_column': timestamp_column
        }

    def _predict(self, X, python_estimator):
        return python_estimator.predict(X)

    def _load_python_estimator(self, training_task):
        training_task_id = training_task['task_id']
        url = f"models:/{training_task_id}/production"
        return mlflow.pyfunc.load_model(url)
