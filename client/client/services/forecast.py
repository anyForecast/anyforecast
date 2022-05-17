from client.services.base import BaseService


class Forecast(BaseService):

    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def create_predictor(self, task_name, dataset_group_name, dataset_name,
                         algorithm, forecast_horizon, perform_hpo=False,
                         inference_folder=None):
        api_params = self._create_api_params(
            task_name=task_name,
            dataset_group_name=dataset_group_name,
            dataset_name=dataset_name,
            algorithm=algorithm,
            forecast_horizon=forecast_horizon,
            perform_hpo=perform_hpo,
            inference_folder=inference_folder
        )
        return self._make_api_call(api_params)


    def create_forecast(self):
        pass
