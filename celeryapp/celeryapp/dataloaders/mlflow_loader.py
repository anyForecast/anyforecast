from typing import Optional

import mlflow


class MlFlowLoader:

    def __init__(
            self,
            tracking_uri: Optional[str] = None,
            registry_uri: Optional[str] = None
    ):
        self._mlflow_client = mlflow.tracking.MlflowClient(
            tracking_uri, registry_uri)

    def load_predictor_from_run_id(self, run_id, artifact='pipeline'):
        url = f'runs:/{run_id}/{artifact}'
        return mlflow.pyfunc.load_model(url)

    def load_predictor(self, model_name, stage):
        url = f'models:/{model_name}/{stage}'
        return mlflow.pyfunc.load_model(url)

    def get_registered_model(self, model_name):
        return self._mlflow_client.get_registered_model(model_name)

    def get_latest_model_version(self, model_name):
        registered_model = self.get_registered_model(model_name)
        return registered_model.latest_versions[-1]

    def get_client(self):
        return self._mlflow_client

    def get_run_id_params(self, run_id):
        run = self._mlflow_client.get_run(run_id)
        return run.to_dictionary()['data']['params']
